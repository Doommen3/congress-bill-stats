import os
import time
import json
import hashlib
import urllib.parse
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.exceptions import Timeout, RequestException, ConnectionError as ReqConnErr
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# =======================
# Config
# =======================
API_ROOT  = os.environ.get("CONGRESS_API_ROOT", "https://api.congress.gov/v3")
API_KEY   = os.environ.get("CONGRESS_API_KEY", "")
CACHE_DIR = os.environ.get("CACHE_DIR", "./cache")
DEFAULT_CONGRESS = int(os.environ.get("DEFAULT_CONGRESS", "119"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))  # parallel page fetchers

os.makedirs(CACHE_DIR, exist_ok=True)

# Cache directory for raw API responses
HTTP_CACHE_DIR = os.path.join(CACHE_DIR, "http")
os.makedirs(HTTP_CACHE_DIR, exist_ok=True)

# Action codes that indicate a bill became law (public or private).
# Source: Congress.gov action codes (public law 36000–39999; private law 41000–44999)
ENACTED_CODE_RANGES = (
    (36000, 39999),  # public law
    (41000, 44999),  # private law
)

app = FastAPI(title="Congress Bill Stats", version="1.0.0")

# CORS for local dev. Tighten in production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the static frontend
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "..", "frontend", "static")),
    name="static"
)

# -------------------------------
# Small helpers
# -------------------------------
def cache_path(congress: int) -> str:
    return os.path.join(CACHE_DIR, f"stats_{congress}.json")

def load_cache(congress: int) -> Optional[Dict[str, Any]]:
    fp = cache_path(congress)
    if os.path.exists(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def save_cache(congress: int, data: Dict[str, Any]) -> None:
    tmp = cache_path(congress) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, cache_path(congress))

def is_enacted(action_code: Optional[int]) -> bool:
    """Return True if the action code indicates the bill became law."""
    if action_code is None:
        return False
    try:
        code = int(action_code)
    except (TypeError, ValueError):
        return False

    for start, end in ENACTED_CODE_RANGES:
        if start <= code <= end:
            return True
    return False

# -------------------------------
# HTTP client for Congress.gov
# -------------------------------
def api_get(path: str, params: Optional[Dict[str, Any]] = None, use_cache: bool = True) -> Dict[str, Any]:
    """GET helper for Congress.gov API with retries, timeouts, and simple file caching."""
    if not API_KEY:
        raise HTTPException(status_code=500, detail="Missing CONGRESS_API_KEY env var.")

    url = f"{API_ROOT.rstrip('/')}/{path.lstrip('/')}"
    headers = {"X-Api-Key": API_KEY, "Accept": "application/json"}
    params = {**(params or {}), "format": "json"}  # ask for JSON; key is in header

    cache_file = None
    if use_cache:
        key = f"{url}?{urllib.parse.urlencode(sorted(params.items()))}"
        name = hashlib.sha256(key.encode("utf-8")).hexdigest()
        cache_file = os.path.join(HTTP_CACHE_DIR, f"{name}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    print(f"[http-cache] hit {url}", flush=True)
                    return json.load(f)
            except Exception:
                pass

    for attempt in range(3):
        try:
            t0 = time.time()
            # Log without exposing secrets
            log_params = dict(params)
            print(f"[http] GET {url} params={log_params}", flush=True)

            resp = requests.get(url, params=params, headers=headers, timeout=(15, 45))  # (connect, read)
            dt = time.time() - t0
            print(f"[http] <- {resp.status_code} in {dt:.2f}s", flush=True)

            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    raise HTTPException(status_code=502, detail="Invalid JSON from Congress API")

                if cache_file:
                    tmp = cache_file + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(data, f)
                    os.replace(tmp, cache_file)
                return data

            if resp.status_code >= 500 and attempt < 2:
                time.sleep(1.2 * (attempt + 1))
                continue

            raise HTTPException(status_code=resp.status_code,
                                detail=f"Congress API error: {resp.text[:300]}")

        except (Timeout, ReqConnErr) as e:
            if attempt < 2:
                time.sleep(1.2 * (attempt + 1))
                continue
            raise HTTPException(status_code=504, detail=f"Upstream timeout: {type(e).__name__}") from e

        except RequestException as e:
            if attempt < 2:
                time.sleep(1.2 * (attempt + 1))
                continue
            raise HTTPException(status_code=502, detail=f"Upstream error: {type(e).__name__}") from e

    raise HTTPException(status_code=502, detail="Congress API unavailable after retries.")

# -------------------------------
# Data fetching (fast + resilient)
# -------------------------------
def _normalize_bill_item(x: Dict[str, Any]) -> Dict[str, Any]:
    """If a list item is shaped like {'bill': {...}}, return the inner object."""
    if isinstance(x, dict) and isinstance(x.get("bill"), dict):
        return x["bill"]
    return x

def _extract_bills(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Handle both shapes and flatten items to plain bill dicts."""
    data = j.get("data")
    if isinstance(data, dict) and isinstance(data.get("bills"), list):
        return [_normalize_bill_item(it) for it in data["bills"]]
    if isinstance(j.get("bills"), list):
        return [_normalize_bill_item(it) for it in j["bills"]]
    if isinstance(data, list):  # rare fallback
        return [_normalize_bill_item(it) for it in data]
    return []

def fetch_all_bills_for_congress(congress: int, use_cache: bool = True) -> List[Dict[str, Any]]:
    """Fetch all bills for a Congress: probe first page, then fetch remaining pages in parallel."""
    limit = 250

    # First page
    first = api_get(f"/bill/{congress}", params={"limit": limit, "offset": 0}, use_cache=use_cache)
    bills: List[Dict[str, Any]] = _extract_bills(first)
    pagination = first.get("pagination") or {}
    total = int(pagination.get("count") or 0)
    total_pages = (total + limit - 1) // limit if total else 1
    print(f"[bills] total={total} limit={limit} pages={total_pages} first_page_items={len(bills)}", flush=True)

    if total <= limit:
        return bills

    # Remaining offsets
    offsets = list(range(limit, total, limit))

    # Fetch remaining pages in parallel (page-level)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    max_workers = int(os.environ.get("MAX_WORKERS", "8"))

    def fetch_page(off: int) -> List[Dict[str, Any]]:
        r = api_get(
            f"/bill/{congress}", params={"limit": limit, "offset": off}, use_cache=use_cache
        )
        return _extract_bills(r)

    fetched = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(fetch_page, off) for off in offsets]
        for fut in as_completed(futures):
            page_bills = fut.result()
            bills.extend(page_bills)
            fetched += 1
            print(f"[bills] pages_done={1 + fetched}/{total_pages} items={len(bills)}/{total}", flush=True)

    return bills


# -------------------------------
# Sponsor extraction helpers
# -------------------------------
def extract_primary_sponsor(b: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Return {bioguideId, fullName, party, state, chamber} for the primary sponsor,
    handling `sponsor` (object) AND `sponsors` (list/dict) at the *list* level.
    """
    # If a nested shape slipped through, normalize again
    if isinstance(b.get("bill"), dict):
        b = b["bill"]

    # Shape 1: single object sometimes present at list level
    sponsor = b.get("sponsor") or {}
    if isinstance(sponsor, dict) and sponsor.get("bioguideId"):
        return {
            "bioguideId": sponsor.get("bioguideId"),
            "fullName": sponsor.get("fullName") or sponsor.get("name"),
            "party": sponsor.get("party"),
            "state": sponsor.get("state"),
            "chamber": sponsor.get("chamber") or b.get("originChamber"),
        }

    # Shape 2: list OR dict-with-item at list level (rare)
    sponsors = b.get("sponsors") or {}
    # Could be a dict with "item": [ ... ] or already a list
    items = sponsors.get("item") if isinstance(sponsors, dict) else sponsors
    if isinstance(items, list) and items:
        s0 = items[0] or {}
        bioguide = s0.get("bioguideId") or s0.get("bioguideID") or s0.get("bioguide")
        if bioguide:
            return {
                "bioguideId": bioguide,
                "fullName": s0.get("fullName") or s0.get("name"),
                "party": s0.get("party"),
                "state": s0.get("state"),
                "chamber": s0.get("chamber") or b.get("originChamber"),
            }
    return None

def _bill_identity(congress: int, b: Dict[str, Any]) -> Optional[str]:
    """
    Build the item URL path: /bill/{congress}/{type}/{number}
    Prefer the provided 'url' if present.
    """
    # Prefer API-provided URL
    url = b.get("url")
    if isinstance(url, str) and url.strip():
        # Convert absolute URL to path for api_get
        try:
            # path like /v3/bill/119/hr/3076 -> keep the trailing part after /v3
            idx = url.index("/v3/")
            return url[idx + len("/v3") :]
        except ValueError:
            pass  # fall through to construct

    typ = (b.get("type") or "").lower()  # e.g., 'hr', 's', 'hjres'
    num = b.get("number")
    if typ and num:
        return f"/bill/{congress}/{typ}/{num}"
    return None

def fetch_primary_sponsor_from_item(
    congress: int, b: Dict[str, Any], use_cache: bool = True
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    Fetch the *item* endpoint and extract the first sponsor, returning the full
    bill object along with sponsor info.
    Item shape (JSON): { "data": { "bill": { "sponsors": { "item": [ ... ] }, "originChamber": ... } } }
    """
    path = _bill_identity(congress, b)
    if not path:
        return b, None
    j = api_get(path, use_cache=use_cache)
    data = j.get("data") or {}
    bill = data.get("bill") or j.get("bill") or {}
    sponsors = bill.get("sponsors") or {}
    items = sponsors.get("item") if isinstance(sponsors, dict) else sponsors
    if isinstance(items, list) and items:
        s0 = items[0] or {}
        bioguide = s0.get("bioguideId") or s0.get("bioguideID") or s0.get("bioguide")
        if bioguide:
            sponsor = {
                "bioguideId": bioguide,
                "fullName": s0.get("fullName") or s0.get("name"),
                "party": s0.get("party"),
                "state": s0.get("state"),
                "chamber": s0.get("chamber") or bill.get("originChamber"),
            }
            return bill, sponsor
    return bill, None


# -------------------------------
# Member lookup (unchanged, but resilient to shape)
# -------------------------------
def fetch_member_snapshot(bioguide_id: str, use_cache: bool = True) -> Dict[str, Any]:
    """Get member details to attach chamber/state/party."""
    resp = api_get(f"/member/{bioguide_id}", use_cache=use_cache)
    data = resp.get("data") or {}
    member = data.get("member") or resp.get("member") or {}
    out = {
        "bioguideId": bioguide_id,
        "firstName": member.get("firstName"),
        "lastName": member.get("lastName"),
        "fullName": member.get("name") or member.get("fullName"),
        "party": member.get("party"),
        "state": member.get("state"),
        "chamber": None,
    }
    roles = member.get("roles") or []
    if isinstance(roles, list) and roles:
        latest = roles[0]
        out["chamber"] = latest.get("chamber")
        out["party"] = out["party"] or latest.get("party")
        out["state"] = out["state"] or latest.get("state")
    return out


# -------------------------------
# Aggregation (with item-level fallback)
# -------------------------------
def build_stats(congress: int, use_cache: bool = True) -> Dict[str, Any]:
    """Aggregate counts by sponsor from bill list; fetch item details when needed."""
    raw_bills = fetch_all_bills_for_congress(congress, use_cache=use_cache)

    # Try list-level sponsor/action code; fall back to item-level if either missing
    list_level: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []
    need_items: List[Dict[str, Any]] = []

    for b in raw_bills:
        s = extract_primary_sponsor(b)
        latest = b.get("latestAction") or {}
        action_code = latest.get("actionCode")
        if s and action_code is not None:
            list_level.append((b, s))
        else:
            need_items.append(b)

    print(
        f"[agg] bills={len(raw_bills)} list_level_sponsors={len(list_level)} need_items={len(need_items)}",
        flush=True,
    )

    # Fetch item-level details in parallel when needed (tune workers to respect rate limits)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    detail_workers = int(os.environ.get("DETAIL_WORKERS", "8"))
    item_results: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []

    if need_items:
        with ThreadPoolExecutor(max_workers=detail_workers) as pool:
            futs = {
                pool.submit(fetch_primary_sponsor_from_item, congress, b, use_cache): b
                for b in need_items
            }
            done = 0
            for fut in as_completed(futs):
                orig = futs[fut]
                try:
                    bill_detail, s = fut.result()
                except Exception:
                    bill_detail, s = orig, None
                item_results.append((bill_detail, s))
                done += 1
                if done % 50 == 0 or done == len(need_items):
                    print(
                        f"[items] fetched {done}/{len(need_items)} item details",
                        flush=True,
                    )

    # Combine
    pairs = list_level + item_results

    # Tally
    by_sponsor: Dict[str, Dict[str, Any]] = {}
    missing_sponsor = 0

    for b, sponsor_info in pairs:
        if not sponsor_info:
            missing_sponsor += 1
            continue

        bioguide = sponsor_info["bioguideId"]
        latest = b.get("latestAction") or {}
        action_code = latest.get("actionCode")

        rec = by_sponsor.get(bioguide)
        if rec is None:
            rec = {
                "bioguideId": bioguide,
                "sponsorName": sponsor_info.get("fullName"),
                "party": sponsor_info.get("party"),
                "state": sponsor_info.get("state"),
                "chamber": sponsor_info.get("chamber"),
                "sponsored_total": 0,
                "enacted_total": 0,
            }
            by_sponsor[bioguide] = rec

        rec["sponsored_total"] += 1
        if is_enacted(action_code):
            rec["enacted_total"] += 1

    # Enrich any missing meta from the member endpoint (<= 535 lookups worst case)
    for bioguide, rec in by_sponsor.items():
        needs = any(rec.get(k) in (None, "", "Unknown") for k in ("party", "state", "chamber", "sponsorName"))
        if needs:
            try:
                m = fetch_member_snapshot(bioguide, use_cache=use_cache)
                rec["party"] = rec["party"] or m.get("party")
                rec["state"] = rec["state"] or m.get("state")
                rec["chamber"] = rec["chamber"] or m.get("chamber")
                rec["sponsorName"] = rec["sponsorName"] or m.get("fullName")
            except HTTPException:
                pass

    rows = list(by_sponsor.values())
    rows.sort(key=lambda r: (-int(r["sponsored_total"]), r.get("sponsorName") or ""))

    print(
        f"[agg] final sponsors={len(rows)} missing_after_item={missing_sponsor}",
        flush=True
    )

    return {
        "congress": congress,
        "generated_at": int(time.time()),
        "rows": rows,
        "note": (
            "“Became Law” counts bills where latestAction.actionCode indicates a public or private law. "
            "Sponsors are taken from list level when present, otherwise from the bill item’s sponsors.item[0]."
        ),
    }



# -------------------------------
# Routes
# -------------------------------
@app.get("/api/stats")
def api_stats(
    congress: int = Query(DEFAULT_CONGRESS, ge=81, le=999),
    refresh: bool = Query(False, description="Force rebuild and refresh cache."),
):
    print(f"[api_stats] start congress={congress} refresh={refresh}", flush=True)

    cached = None if refresh else load_cache(congress)
    if cached:
        return JSONResponse(cached)

    stats = build_stats(congress, use_cache=not refresh)
    save_cache(congress, stats)
    return JSONResponse(stats)

@app.get("/", response_class=HTMLResponse)
def index():
    """Serve the frontend index.html"""
    html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "static", "index.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Congress Bill Stats</h1><p>Frontend not found.</p>")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())
