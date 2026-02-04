import os
import time
import json
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.exceptions import Timeout, RequestException, ConnectionError as ReqConnErr
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# Import database module
try:
    from . import database as db
except ImportError:
    import database as db

# Import Illinois stats module
try:
    from . import illinois_stats as il_stats
except ImportError:
    import illinois_stats as il_stats

# =======================
# Config
# =======================
API_ROOT  = os.environ.get("CONGRESS_API_ROOT", "https://api.congress.gov/v3")
API_KEY   = os.environ.get("CONGRESS_API_KEY", "")
CACHE_DIR = os.environ.get("CACHE_DIR", "./cache")
DEFAULT_CONGRESS = int(os.environ.get("DEFAULT_CONGRESS", "119"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))  # parallel page fetchers
DETAIL_WORKERS = int(os.environ.get("DETAIL_WORKERS", "8"))  # parallel item fetchers
DEFAULT_IL_SESSION = int(os.environ.get("DEFAULT_IL_SESSION", "104"))  # Illinois GA session

os.makedirs(CACHE_DIR, exist_ok=True)

# Action codes that indicate a bill became law (public or private).
# Source: Congress.gov action codes (public law ~36000–40000; private law ~41000–45000)
# Note: These are kept as fallback but we now primarily use the /law endpoint
ENACTED_CODES = {
    36000, 37000, 38000, 39000, 40000,   # public law
    41000, 42000, 43000, 44000, 45000    # private law
}

# Track background refresh status
_refresh_status: Dict[int, Dict[str, Any]] = {}

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
    """Load cached stats from file or database."""
    # Try file cache first (faster)
    fp = cache_path(congress)
    if os.path.exists(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    # Try database cache
    try:
        cached = db.load_stats_cache(congress)
        if cached:
            return cached
    except Exception as e:
        print(f"[cache] Database cache lookup failed: {e}", flush=True)

    return None

def save_cache(congress: int, data: Dict[str, Any]) -> None:
    """Save stats to both file and database cache."""
    # Save to file
    tmp = cache_path(congress) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, cache_path(congress))

    # Save to database (if not already saved by build_stats)
    try:
        db.save_stats_cache(congress, data)
    except Exception as e:
        print(f"[cache] Database cache save failed: {e}", flush=True)

def is_enacted(action_code: Optional[int]) -> bool:
    """Check if action code indicates enacted status (fallback method)."""
    if action_code is None:
        return False
    try:
        return int(action_code) in ENACTED_CODES
    except Exception:
        return False


def normalize_bill_key(congress: int, bill_type: str, bill_number: int) -> str:
    """Create a consistent key for bill lookup: 'congress-type-number' (e.g., '119-hr-1234')."""
    return f"{congress}-{bill_type.lower()}-{bill_number}"

# -------------------------------
# HTTP client for Congress.gov
# -------------------------------
def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """GET helper for Congress.gov API with retries, timeouts, and safe logging."""
    if not API_KEY:
        raise HTTPException(status_code=500, detail="Missing CONGRESS_API_KEY env var.")

    url = f"{API_ROOT.rstrip('/')}/{path.lstrip('/')}"
    headers = {"X-Api-Key": API_KEY, "Accept": "application/json"}
    params = {**(params or {}), "format": "json"}  # ask for JSON; key is in header

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
                    return resp.json()
                except ValueError:
                    raise HTTPException(status_code=502, detail="Invalid JSON from Congress API")

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

def fetch_all_bills_for_congress(congress: int) -> List[Dict[str, Any]]:
    """Fetch all bills for a Congress: probe first page, then fetch remaining pages in parallel."""
    limit = 250

    # First page
    first = api_get(f"/bill/{congress}", params={"limit": limit, "offset": 0})
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
        r = api_get(f"/bill/{congress}", params={"limit": limit, "offset": off})
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

def fetch_primary_sponsor_from_item(congress: int, b: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Fetch the *item* endpoint and extract the first sponsor.
    Item shape (JSON): { "data": { "bill": { "sponsors": { "item": [ ... ] }, "originChamber": ... } } }
    """
    path = _bill_identity(congress, b)
    if not path:
        return None
    j = api_get(path)
    data = j.get("data") or {}
    bill = data.get("bill") or j.get("bill") or {}
    sponsors = bill.get("sponsors") or {}
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
                "chamber": s0.get("chamber") or bill.get("originChamber"),
            }
    return None


# -------------------------------
# Member lookup (unchanged, but resilient to shape)
# -------------------------------
def fetch_member_snapshot(bioguide_id: str) -> Dict[str, Any]:
    """Get member details to attach chamber/state/party."""
    resp = api_get(f"/member/{bioguide_id}")
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
# Law fetching (primary source for enacted bills)
# -------------------------------
def _extract_laws(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract law items from API response, handling various shapes."""
    # Try common response shapes
    if isinstance(j.get("laws"), list):
        return j["laws"]
    data = j.get("data")
    if isinstance(data, dict) and isinstance(data.get("laws"), list):
        return data["laws"]
    if isinstance(data, list):
        return data
    return []


def fetch_all_laws_for_congress(congress: int) -> List[Dict[str, Any]]:
    """
    Fetch all enacted laws for a Congress using the /law endpoint.
    Returns list of law objects with bill references.
    """
    limit = 250
    all_laws: List[Dict[str, Any]] = []

    # Fetch public laws
    print(f"[laws] Fetching public laws for Congress {congress}...", flush=True)
    try:
        first_pub = api_get(f"/law/{congress}/pub", params={"limit": limit, "offset": 0})
        pub_laws = _extract_laws(first_pub)
        pagination = first_pub.get("pagination") or {}
        total_pub = int(pagination.get("count") or len(pub_laws))
        print(f"[laws] Public laws: {total_pub} total, first page: {len(pub_laws)}", flush=True)

        for law in pub_laws:
            law["_law_type"] = "public"
        all_laws.extend(pub_laws)

        # Fetch remaining pages if needed
        if total_pub > limit:
            offsets = list(range(limit, total_pub, limit))
            for off in offsets:
                resp = api_get(f"/law/{congress}/pub", params={"limit": limit, "offset": off})
                page_laws = _extract_laws(resp)
                for law in page_laws:
                    law["_law_type"] = "public"
                all_laws.extend(page_laws)
                print(f"[laws] Public laws fetched: {len(all_laws)}/{total_pub}", flush=True)
    except HTTPException as e:
        print(f"[laws] Error fetching public laws: {e.detail}", flush=True)

    # Fetch private laws
    print(f"[laws] Fetching private laws for Congress {congress}...", flush=True)
    try:
        first_priv = api_get(f"/law/{congress}/priv", params={"limit": limit, "offset": 0})
        priv_laws = _extract_laws(first_priv)
        pagination = first_priv.get("pagination") or {}
        total_priv = int(pagination.get("count") or len(priv_laws))
        print(f"[laws] Private laws: {total_priv} total, first page: {len(priv_laws)}", flush=True)

        for law in priv_laws:
            law["_law_type"] = "private"
        all_laws.extend(priv_laws)

        # Fetch remaining pages if needed
        if total_priv > limit:
            offsets = list(range(limit, total_priv, limit))
            for off in offsets:
                resp = api_get(f"/law/{congress}/priv", params={"limit": limit, "offset": off})
                page_laws = _extract_laws(resp)
                for law in page_laws:
                    law["_law_type"] = "private"
                all_laws.extend(page_laws)
                print(f"[laws] Private laws fetched: {len(all_laws) - total_pub}/{total_priv}", flush=True)
    except HTTPException as e:
        print(f"[laws] Error fetching private laws: {e.detail}", flush=True)

    print(f"[laws] Total laws fetched: {len(all_laws)}", flush=True)
    return all_laws


def build_law_lookup(congress: int, laws: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build a lookup dict from bill key to law info.
    Key format: 'congress-type-number' (e.g., '119-hr-1234')
    Value: {'law_type': 'public'|'private', 'law_number': '119-1'}
    """
    lookup: Dict[str, Dict[str, Any]] = {}

    for law in laws:
        # Extract bill reference from law
        # Law response typically has: type, number, and bill reference
        law_type = law.get("_law_type", "public")
        law_number = law.get("number")

        # The law endpoint returns bill info - try various shapes
        bill = law.get("bill") or {}
        if not bill:
            # Sometimes the law IS the bill reference
            bill = law

        bill_type = (bill.get("type") or "").lower()
        bill_number = bill.get("number")
        bill_congress = bill.get("congress") or congress

        if bill_type and bill_number:
            key = normalize_bill_key(bill_congress, bill_type, bill_number)
            lookup[key] = {
                "law_type": law_type,
                "law_number": law_number,
            }

    print(f"[laws] Built lookup with {len(lookup)} bill-to-law mappings", flush=True)
    return lookup


# -------------------------------
# Aggregation (with item-level fallback)
# -------------------------------
def build_stats(congress: int) -> Dict[str, Any]:
    """
    Aggregate counts by sponsor from bill list; use /law endpoint for enacted status.
    Returns sponsor statistics with public/private law breakdown.
    """
    # Step 1: Fetch all laws first (this is the authoritative source for enacted bills)
    print(f"[build_stats] Starting stats build for Congress {congress}", flush=True)
    laws = fetch_all_laws_for_congress(congress)
    law_lookup = build_law_lookup(congress, laws)

    # Step 2: Fetch all bills
    raw_bills = fetch_all_bills_for_congress(congress)

    # Step 3: Extract sponsors - try list-level first, fallback to item-level
    list_level: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []
    need_items: List[Dict[str, Any]] = []

    for b in raw_bills:
        s = extract_primary_sponsor(b)
        if s:
            list_level.append((b, s))
        else:
            need_items.append(b)

    print(f"[agg] bills={len(raw_bills)} list_level_sponsors={len(list_level)} need_items={len(need_items)}", flush=True)

    # Fetch item-level sponsors in parallel for bills missing sponsor data
    item_results: List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]] = []

    if need_items:
        with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
            futs = {pool.submit(fetch_primary_sponsor_from_item, congress, b): b for b in need_items}
            done = 0
            for fut in as_completed(futs):
                b = futs[fut]
                try:
                    s = fut.result()
                except Exception:
                    s = None
                item_results.append((b, s))
                done += 1
                if done % 50 == 0 or done == len(need_items):
                    print(f"[items] fetched {done}/{len(need_items)} item sponsors", flush=True)

    # Combine all bill-sponsor pairs
    pairs = list_level + item_results

    # Step 4: Tally by sponsor using the law lookup
    by_sponsor: Dict[str, Dict[str, Any]] = {}
    missing_sponsor = 0
    laws_matched = 0

    for b, sponsor_info in pairs:
        if not sponsor_info:
            missing_sponsor += 1
            continue

        bioguide = sponsor_info["bioguideId"]

        # Build bill key for law lookup
        bill_type = (b.get("type") or "").lower()
        bill_number = b.get("number")
        bill_key = normalize_bill_key(congress, bill_type, bill_number) if bill_type and bill_number else None

        # Check if this bill became law using the law lookup
        law_info = law_lookup.get(bill_key) if bill_key else None

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
                "public_law_count": 0,
                "private_law_count": 0,
            }
            by_sponsor[bioguide] = rec

        rec["sponsored_total"] += 1

        if law_info:
            laws_matched += 1
            rec["enacted_total"] += 1
            if law_info["law_type"] == "public":
                rec["public_law_count"] += 1
            else:
                rec["private_law_count"] += 1

    # Step 5: Enrich any missing metadata from the member endpoint
    needs_enrichment = [
        (bioguide, rec) for bioguide, rec in by_sponsor.items()
        if any(rec.get(k) in (None, "", "Unknown") for k in ("party", "state", "chamber", "sponsorName"))
    ]

    if needs_enrichment:
        print(f"[agg] Enriching {len(needs_enrichment)} sponsors with missing metadata...", flush=True)
        for bioguide, rec in needs_enrichment:
            try:
                m = fetch_member_snapshot(bioguide)
                rec["party"] = rec["party"] or m.get("party")
                rec["state"] = rec["state"] or m.get("state")
                rec["chamber"] = rec["chamber"] or m.get("chamber")
                rec["sponsorName"] = rec["sponsorName"] or m.get("fullName")
            except HTTPException:
                pass

    rows = list(by_sponsor.values())
    rows.sort(key=lambda r: (-int(r["sponsored_total"]), r.get("sponsorName") or ""))

    # Calculate summary stats
    total_public = sum(r["public_law_count"] for r in rows)
    total_private = sum(r["private_law_count"] for r in rows)

    print(
        f"[agg] final sponsors={len(rows)} missing_sponsor={missing_sponsor} "
        f"laws_matched={laws_matched} (public={total_public}, private={total_private})",
        flush=True
    )

    stats = {
        "congress": congress,
        "generated_at": int(time.time()),
        "rows": rows,
        "summary": {
            "total_legislators": len(rows),
            "total_bills": len(raw_bills),
            "total_laws": len(laws),
            "public_laws": total_public,
            "private_laws": total_private,
        },
        "note": (
            "Law counts are determined using the Congress.gov /law endpoint, which provides "
            "authoritative data on enacted legislation. 'Public Laws' and 'Private Laws' show "
            "the breakdown by law type."
        ),
    }

    # Save to database for persistence
    try:
        # Save legislators
        db.save_legislators_batch(rows)

        # Prepare bills with sponsor info for database
        bills_with_sponsors = []
        for b, sponsor_info in pairs:
            if sponsor_info:
                b["_sponsor_info"] = sponsor_info
                bills_with_sponsors.append(b)
        db.save_bills_batch(congress, bills_with_sponsors)

        # Save laws with sponsor info
        for law in laws:
            bill = law.get("bill") or law
            bill_type = (bill.get("type") or "").lower()
            bill_number = bill.get("number")
            if bill_type and bill_number:
                bill_key = normalize_bill_key(congress, bill_type, bill_number)
                # Find sponsor from our pairs
                for b, sponsor_info in pairs:
                    b_type = (b.get("type") or "").lower()
                    b_num = b.get("number")
                    if b_type == bill_type and b_num == bill_number and sponsor_info:
                        law["_sponsor_bioguide_id"] = sponsor_info["bioguideId"]
                        break
        db.save_laws_batch(congress, laws)

        # Save stats cache
        db.save_stats_cache(congress, stats)
        print(f"[db] Persisted all data for Congress {congress}", flush=True)
    except Exception as e:
        print(f"[db] Warning: Failed to persist to database: {e}", flush=True)

    return stats



# -------------------------------
# Background refresh logic
# -------------------------------
def _do_background_refresh(congress: int):
    """Run stats refresh in background, updating status."""
    global _refresh_status
    _refresh_status[congress] = {"status": "running", "started_at": int(time.time())}
    try:
        stats = build_stats(congress)
        save_cache(congress, stats)
        _refresh_status[congress] = {
            "status": "completed",
            "completed_at": int(time.time()),
            "summary": stats.get("summary", {}),
        }
    except Exception as e:
        _refresh_status[congress] = {
            "status": "error",
            "error": str(e),
            "completed_at": int(time.time()),
        }


# -------------------------------
# Routes
# -------------------------------
@app.get("/health")
def health_check():
    """Health check endpoint for keeping the service warm."""
    return {"status": "ok", "timestamp": int(time.time())}


@app.get("/api/stats")
def api_stats(
    congress: int = Query(DEFAULT_CONGRESS, ge=81, le=999),
    refresh: bool = Query(False, description="Force rebuild and refresh cache."),
    background: bool = Query(False, description="Run refresh in background, return cached data immediately."),
    background_tasks: BackgroundTasks = None,
):
    """
    Get legislator statistics for a Congress.

    - If cached data exists and refresh=False, returns cached data immediately.
    - If refresh=True and background=True, returns cached data and refreshes in background.
    - If refresh=True and background=False, rebuilds stats synchronously.
    """
    print(f"[api_stats] start congress={congress} refresh={refresh} background={background}", flush=True)

    cached = load_cache(congress)

    # If background refresh requested and we have cached data
    if refresh and background and cached and background_tasks:
        # Check if already refreshing
        status = _refresh_status.get(congress, {})
        if status.get("status") != "running":
            background_tasks.add_task(_do_background_refresh, congress)
            print(f"[api_stats] Started background refresh for Congress {congress}", flush=True)

        # Return cached data with refresh status
        cached["_refresh_status"] = "pending"
        return JSONResponse(cached)

    # Return cached if not forcing refresh
    if not refresh and cached:
        return JSONResponse(cached)

    # Synchronous refresh
    stats = build_stats(congress)
    save_cache(congress, stats)
    return JSONResponse(stats)


@app.get("/api/refresh-status")
def refresh_status(congress: int = Query(DEFAULT_CONGRESS, ge=81, le=999)):
    """Check the status of a background refresh."""
    status = _refresh_status.get(congress, {"status": "none"})
    return JSONResponse(status)


# -------------------------------
# Illinois Routes
# -------------------------------
@app.get("/api/il-stats")
def api_il_stats(
    session: int = Query(DEFAULT_IL_SESSION, ge=98, le=999, description="Illinois GA session number"),
    refresh: bool = Query(False, description="Force rebuild and refresh cache."),
    background: bool = Query(False, description="Run refresh in background, return cached data immediately."),
    background_tasks: BackgroundTasks = None,
):
    """
    Get Illinois legislator statistics for a General Assembly session.

    - If cached data exists and refresh=False, returns cached data immediately.
    - If refresh=True and background=True, returns cached data and refreshes in background.
    - If refresh=True and background=False, rebuilds stats synchronously.
    """
    print(f"[api_il_stats] start session={session} refresh={refresh} background={background}", flush=True)

    cached = il_stats.load_il_cache(session)

    # If background refresh requested and we have cached data
    if refresh and background and cached and background_tasks:
        # Check if already refreshing
        status = il_stats.get_il_refresh_status(session)
        if status.get("status") != "running":
            background_tasks.add_task(il_stats.do_il_background_refresh, session)
            print(f"[api_il_stats] Started background refresh for IL session {session}", flush=True)

        # Return cached data with refresh status
        cached["_refresh_status"] = "pending"
        return JSONResponse(cached)

    # Return cached if not forcing refresh
    if not refresh and cached:
        return JSONResponse(cached)

    # Synchronous refresh
    stats = il_stats.build_il_stats(session)
    il_stats.save_il_cache(session, stats)
    return JSONResponse(stats)


@app.get("/api/il-refresh-status")
def il_refresh_status(session: int = Query(DEFAULT_IL_SESSION, ge=98, le=999)):
    """Check the status of an Illinois background refresh."""
    status = il_stats.get_il_refresh_status(session)
    return JSONResponse(status)


@app.get("/api/il-sessions")
def il_sessions():
    """Return list of available Illinois GA sessions."""
    return JSONResponse({"sessions": il_stats.get_available_sessions()})


@app.get("/", response_class=HTMLResponse)
def index():
    """Serve the frontend index.html"""
    html_path = os.path.join(os.path.dirname(__file__), "..", "frontend", "static", "index.html")
    if not os.path.exists(html_path):
        return HTMLResponse("<h1>Congress Bill Stats</h1><p>Frontend not found.</p>")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting server on http://localhost:{port}", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=port)
