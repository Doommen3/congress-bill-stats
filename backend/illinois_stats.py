"""
Illinois General Assembly statistics module.
Fetches and parses ILGA FTP XML data for bill sponsorship tracking.
"""
import os
import re
import time
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser

import requests
from requests.exceptions import Timeout, RequestException, ConnectionError as ReqConnErr

# Import database module
try:
    from . import illinois_database as il_db
except ImportError:
    import illinois_database as il_db

# =======================
# Config
# =======================
IL_FTP_ROOT = os.environ.get("IL_FTP_ROOT", "https://ilga.gov/ftp")
IL_CACHE_DIR = os.environ.get("IL_CACHE_DIR", "./cache/illinois")
IL_MAX_WORKERS = int(os.environ.get("IL_MAX_WORKERS", "4"))  # Lower for FTP politeness
IL_REQUEST_DELAY = float(os.environ.get("IL_REQUEST_DELAY", "0.1"))  # Delay between requests

os.makedirs(IL_CACHE_DIR, exist_ok=True)

# Session years mapping
SESSION_YEARS = {
    104: "2025-2026",
    103: "2023-2024",
    102: "2021-2022",
    101: "2019-2020",
    100: "2017-2018",
}

# Public Act detection regex - matches "Public Act . . . . . . . . . 103-0324" format
PUBLIC_ACT_PATTERN = re.compile(r'Public\s+Act\s*[.\s]*(\d{3}-\d{4})', re.IGNORECASE)

# Sponsor action patterns (parsed from action text)
PRIMARY_FILED_PATTERN = re.compile(r'\b(Prefiled|Filed)\b.*\bby\b\s+(.+)$', re.IGNORECASE)
CHIEF_CO_ADD_PATTERN = re.compile(r'\bAdded\s+Chief\s+Co-?Sponsors?\b', re.IGNORECASE)
CO_ADD_PATTERN = re.compile(r'\bAdded\s+Co-?Sponsors?\b', re.IGNORECASE)
CHIEF_CO_REMOVE_PATTERN = re.compile(r'\bRemoved\s+Chief\s+Co-?Sponsors?\b', re.IGNORECASE)
CO_REMOVE_PATTERN = re.compile(r'\bRemoved\s+Co-?Sponsors?\b', re.IGNORECASE)

# Name normalization patterns
TITLE_PATTERN = re.compile(r'^(Rep\.|Sen\.|Representative|Senator)\s+', re.IGNORECASE)
SUFFIX_PATTERN = re.compile(r',?\s+(Jr\.?|Sr\.?|II|III|IV|V)$', re.IGNORECASE)

# Track background refresh status for Illinois
_il_refresh_status: Dict[int, Dict[str, Any]] = {}


# =======================
# HTML Directory Parser
# =======================
class DirectoryListingParser(HTMLParser):
    """Parse HTML directory listing to extract file links."""

    def __init__(self):
        super().__init__()
        self.files = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href' and value and not value.startswith('?'):
                    # Handle both relative and absolute paths
                    if value.startswith('/'):
                        # Extract just the filename from absolute paths
                        filename = value.rsplit('/', 1)[-1]
                        if filename and filename.endswith('.xml'):
                            self.files.append(filename)
                    else:
                        self.files.append(value)


# =======================
# HTTP helpers
# =======================
def il_http_get(url: str, timeout: Tuple[int, int] = (15, 45)) -> requests.Response:
    """
    GET helper for ILGA FTP with retries and timeouts.
    Returns the raw response object.
    """
    for attempt in range(3):
        try:
            t0 = time.time()
            print(f"[il_http] GET {url}", flush=True)

            resp = requests.get(url, timeout=timeout)
            dt = time.time() - t0
            print(f"[il_http] <- {resp.status_code} in {dt:.2f}s", flush=True)

            if resp.status_code == 200:
                return resp

            if resp.status_code >= 500 and attempt < 2:
                time.sleep(1.2 * (attempt + 1))
                continue

            resp.raise_for_status()

        except (Timeout, ReqConnErr) as e:
            if attempt < 2:
                time.sleep(1.2 * (attempt + 1))
                continue
            raise RuntimeError(f"ILGA FTP timeout: {type(e).__name__}") from e

        except RequestException as e:
            if attempt < 2:
                time.sleep(1.2 * (attempt + 1))
                continue
            raise RuntimeError(f"ILGA FTP error: {type(e).__name__}") from e

    raise RuntimeError("ILGA FTP unavailable after retries.")


def il_fetch_xml(url: str) -> str:
    """Fetch XML content from URL, stripping BOM if present."""
    resp = il_http_get(url)
    # Try UTF-8 first, fall back to Latin-1 for files with accented characters
    try:
        content = resp.content.decode('utf-8-sig')
    except UnicodeDecodeError:
        # Some ILGA files contain Latin-1 encoded characters (e.g., accented names)
        content = resp.content.decode('latin-1')
    return content


def il_fetch_directory_listing(url: str) -> List[str]:
    """Fetch and parse directory listing to get file names."""
    resp = il_http_get(url)
    parser = DirectoryListingParser()
    parser.feed(resp.text)
    return parser.files


# =======================
# Name normalization and matching
# =======================
def normalize_name(raw_name: str) -> str:
    """
    Normalize name for matching:
    1. Remove title prefix (Rep., Sen., Representative, Senator)
    2. Remove suffix (Jr., Sr., II, III, IV)
    3. Lowercase and strip whitespace
    """
    if not raw_name:
        return ""

    # Step 1: Remove title
    name = TITLE_PATTERN.sub('', raw_name.strip())

    # Step 2: Remove suffix
    name = SUFFIX_PATTERN.sub('', name)

    # Step 3: Normalize whitespace and case
    name = ' '.join(name.split()).lower().strip()

    return name


def normalize_name_for_lookup(raw_name: str) -> str:
    """
    Create a simplified lookup key from a name.
    Removes middle names/initials for broader matching.
    """
    normalized = normalize_name(raw_name)
    parts = normalized.split()
    if len(parts) >= 2:
        # Return "firstname lastname" without middle parts
        return f"{parts[0]} {parts[-1]}"
    return normalized


def _parse_action_date(date_str: str) -> Optional[datetime]:
    """Parse IL action date in M/D/YYYY format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except ValueError:
        return None


def _normalize_action_text(text: str) -> str:
    """Normalize action text for matching."""
    return ' '.join((text or "").split())


def _strip_title_prefix(name: str) -> str:
    """Remove title prefixes like Rep./Sen. from a name."""
    return TITLE_PATTERN.sub('', (name or "").strip()).strip()


def _strip_action_suffixes(name: str) -> str:
    """Remove trailing parentheticals and stray punctuation from action names."""
    if not name:
        return ""
    cleaned = name.split('(')[0].strip()
    cleaned = cleaned.rstrip(' ,;')
    return cleaned


def _split_name_list(raw: str) -> List[str]:
    """Split a list of names from action text, handling suffix commas."""
    if not raw:
        return []

    text = raw.strip()
    if text.startswith('(') and text.endswith(')'):
        text = text[1:-1].strip()

    # Remove leading title like "Rep." / "Reps."
    text = re.sub(r'^(Reps?\.|Rep\.|Sens?\.|Sen\.|Representatives?|Senators?)\s+', '', text, flags=re.IGNORECASE)

    # Protect suffix commas like "Coffey, Jr."
    text = re.sub(r',\s*(Jr\.?|Sr\.?|II|III|IV|V)\b', r' \1', text)

    # Normalize separators
    text = re.sub(r'\s+and\s+', ', ', text, flags=re.IGNORECASE)

    parts = [p.strip() for p in text.split(',') if p.strip()]
    names: List[str] = []
    for part in parts:
        part = re.sub(r'^(Reps?\.|Rep\.|Sens?\.|Sen\.|Representatives?|Senators?)\s+', '', part, flags=re.IGNORECASE)
        part = _strip_action_suffixes(part)
        if part:
            names.append(part)
    return names


def _extract_names_from_action(text: str, pattern: re.Pattern) -> List[str]:
    """Extract a list of names after a matching action prefix."""
    if not text:
        return []
    match = pattern.search(text)
    if not match:
        return []
    tail = text[match.end():].strip()
    tail = tail.lstrip(": -")
    return _split_name_list(tail)


def _extract_primary_sponsor_from_actions(actions: List[Dict[str, Any]]) -> Optional[str]:
    """Find the first filed/prefiled action and extract the sponsor name."""
    candidates = []
    for idx, action in enumerate(actions):
        text = _normalize_action_text(action.get("text", ""))
        if not text:
            continue
        match = PRIMARY_FILED_PATTERN.search(text)
        if not match:
            continue
        name_text = _strip_action_suffixes(match.group(2) or "")
        if not re.search(r'(Rep\.|Sen\.|Representative|Senator)', name_text, re.IGNORECASE):
            continue
        cleaned = _strip_title_prefix(name_text)
        if cleaned:
            candidates.append((_parse_action_date(action.get("date", "")), idx, cleaned))

    if not candidates:
        return None

    # Sort by earliest date, then document order
    candidates.sort(key=lambda item: (item[0] is None, item[0] or datetime.max, item[1]))
    return candidates[0][2]


def _parse_actions(root: ET.Element) -> List[Dict[str, Any]]:
    """Parse actions from structured or flat ILGA XML formats."""
    actions: List[Dict[str, Any]] = []

    # Structured action elements (<Actions><Action>...)
    for action_path in ['.//Actions/Action', './/Action', './/actions/action']:
        for action in root.findall(action_path):
            action_text = _get_text(action, 'Description') or _get_text(action, 'Action') or (action.text or "")
            action_date = _get_text(action, 'Date') or _get_text(action, 'ActionDate') or ""
            action_chamber = _get_text(action, 'Chamber') or _get_text(action, 'chamber') or ""
            if action_text and action_text.strip():
                actions.append({
                    "text": action_text.strip(),
                    "date": action_date.strip(),
                    "chamber": action_chamber.strip(),
                })
        if actions:
            return actions

    # Flat action list (<actions><statusdate>...</statusdate><action>...</action>...)
    actions_elem = root.find('.//actions') or root.find('.//Actions')
    if actions_elem is not None:
        current: Dict[str, Any] = {}
        for child in list(actions_elem):
            tag = (child.tag or "").lower()
            text = (child.text or "").strip()
            if not text:
                continue
            if tag in ("statusdate", "date", "actiondate"):
                current["date"] = text
            elif tag == "chamber":
                current["chamber"] = text
            elif tag in ("action", "description"):
                current["text"] = text
                actions.append(current)
                current = {}

    return actions


def _apply_sponsor_action(names: List[str], name: str, add: bool) -> None:
    """Add or remove a sponsor name with normalization-based de-duplication."""
    if not name:
        return
    key = normalize_name(name)
    if not key:
        return
    if add:
        if any(normalize_name(existing) == key for existing in names):
            return
        names.append(name)
    else:
        for idx, existing in enumerate(list(names)):
            if normalize_name(existing) == key:
                del names[idx]
                break


def _extract_sponsor_changes_from_actions(actions: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    """Extract chief co-sponsors and co-sponsors from action text."""
    chief_co: List[str] = []
    co: List[str] = []

    for action in actions:
        text = _normalize_action_text(action.get("text", ""))
        if not text:
            continue

        if CHIEF_CO_ADD_PATTERN.search(text):
            names = _extract_names_from_action(text, CHIEF_CO_ADD_PATTERN)
            for name in names:
                _apply_sponsor_action(chief_co, name, True)
                _apply_sponsor_action(co, name, False)
            continue

        if CHIEF_CO_REMOVE_PATTERN.search(text):
            names = _extract_names_from_action(text, CHIEF_CO_REMOVE_PATTERN)
            for name in names:
                _apply_sponsor_action(chief_co, name, False)
            continue

        if CO_ADD_PATTERN.search(text):
            names = _extract_names_from_action(text, CO_ADD_PATTERN)
            for name in names:
                if any(normalize_name(existing) == normalize_name(name) for existing in chief_co):
                    continue
                _apply_sponsor_action(co, name, True)
            continue

        if CO_REMOVE_PATTERN.search(text):
            names = _extract_names_from_action(text, CO_REMOVE_PATTERN)
            for name in names:
                _apply_sponsor_action(co, name, False)
            continue

    return chief_co, co


def _infer_chamber_from_name(raw_name: str) -> Optional[str]:
    """Infer chamber from title in raw name."""
    if not raw_name:
        return None
    if re.search(r'\b(Sen\.|Senator)\b', raw_name, re.IGNORECASE):
        return "senate"
    if re.search(r'\b(Rep\.|Representative)\b', raw_name, re.IGNORECASE):
        return "house"
    return None


def _coerce_json_list(value: Any) -> List[str]:
    """Coerce a stored JSON list or Python list into a list of strings."""
    if not value:
        return []
    if isinstance(value, list):
        return [v for v in value if v]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [v for v in parsed if v]
        except Exception:
            return []
    return []


class ILNameMatcher:
    """
    Handles sponsor name matching to member records.
    Uses multiple lookup strategies for fuzzy matching.
    """

    def __init__(self, members: List[Dict[str, Any]]):
        self.members = members
        self.lookup_exact = {}  # Exact normalized name -> member
        self.lookup_simple = {}  # First + last name -> member
        self.lookup_last = {}  # Last name -> list of members
        self.unmatched: List[Dict[str, Any]] = []

        self._build_lookups()

    def _build_lookups(self):
        """Build multiple lookup dictionaries for matching."""
        for m in self.members:
            name = m.get("name", "")
            first = m.get("first_name", "")
            last = m.get("last_name", "")

            # Exact normalized name
            exact_key = normalize_name(name)
            if exact_key:
                self.lookup_exact[exact_key] = m

            # First + last name (no middle)
            if first and last:
                simple_key = f"{first.lower()} {last.lower()}"
                if simple_key not in self.lookup_simple:
                    self.lookup_simple[simple_key] = m

            # Last name only (for disambiguation)
            if last:
                last_lower = last.lower()
                if last_lower not in self.lookup_last:
                    self.lookup_last[last_lower] = []
                self.lookup_last[last_lower].append(m)

    def match(self, sponsor_name: str, chamber: str = None) -> Optional[Dict[str, Any]]:
        """
        Match sponsor name to member record.
        Tries multiple strategies: exact -> simple -> last name with chamber filter.
        """
        if not sponsor_name:
            return None

        # Strategy 1: Exact normalized match
        exact_key = normalize_name(sponsor_name)
        if exact_key in self.lookup_exact:
            return self.lookup_exact[exact_key]

        # Strategy 2: First + last name match (ignores middle name)
        simple_key = normalize_name_for_lookup(sponsor_name)
        if simple_key in self.lookup_simple:
            return self.lookup_simple[simple_key]

        # Strategy 3: Last name match with chamber filter
        parts = exact_key.split()
        if parts:
            last_name = parts[-1]
            candidates = self.lookup_last.get(last_name, [])

            # Filter by chamber if provided
            if chamber and candidates:
                chamber_filtered = [m for m in candidates if m.get("chamber") == chamber]
                if len(chamber_filtered) == 1:
                    return chamber_filtered[0]

            # If only one candidate with that last name, use it
            if len(candidates) == 1:
                return candidates[0]

        # No match found
        self.unmatched.append({
            "name": sponsor_name,
            "chamber": chamber,
            "normalized": exact_key,
        })
        return None


# =======================
# XML Parsing
# =======================
def parse_members_xml(xml_content: str, chamber: str, ga_session: int) -> List[Dict[str, Any]]:
    """
    Parse member XML file and return list of member dicts.
    Expected structure:
    <Members>
      <Member>
        <Name>Full Name</Name>
        <FirstName>First</FirstName>
        <LastName>Last</LastName>
        <Party>D</Party>
        <District>1</District>
        ...
      </Member>
    </Members>
    """
    members = []

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"[il_xml] Failed to parse members XML: {e}", flush=True)
        return members

    for member_elem in root.findall('.//Member'):
        name = _get_text(member_elem, 'Name') or _get_text(member_elem, 'MemberName') or ""
        first_name = _get_text(member_elem, 'FirstName') or ""
        last_name = _get_text(member_elem, 'LastName') or ""
        party = _get_text(member_elem, 'Party') or ""
        district_str = _get_text(member_elem, 'District') or "0"
        title = _get_text(member_elem, 'Title') or ""

        # Parse district number
        try:
            district = int(district_str)
        except ValueError:
            district = 0

        # Generate member ID
        member_id = f"{ga_session}-{chamber}-{district}"

        members.append({
            "member_id": member_id,
            "ga_session": ga_session,
            "chamber": chamber,
            "district": district,
            "name": name,
            "first_name": first_name,
            "last_name": last_name,
            "party": party,
            "title": title,
        })

    return members


def parse_bill_xml(xml_content: str, filename: str, ga_session: int) -> Optional[Dict[str, Any]]:
    """
    Parse bill status XML file and return bill dict.
    Expected structure varies, but typically:
    <BillStatus>
      <Synopsis>...</Synopsis>
      <PrimarySponsor>
        <Name>Rep. John Smith</Name>
      </PrimarySponsor>
      <Actions>
        <Action>
          <Date>01/15/2025</Date>
          <Description>Public Act . . . 104-0001</Description>
        </Action>
      </Actions>
    </BillStatus>
    """
    # Extract bill type and number from filename
    # Format: 10400HB0001.xml or 10400SB0001.xml
    match = re.match(r'(\d{3})00(HB|SB|HR|SR|HJR|SJR|HJRCA|SJRCA)(\d+)\.xml', filename, re.IGNORECASE)
    if not match:
        return None

    session_from_file = int(match.group(1))
    bill_type = match.group(2).lower()
    bill_number = int(match.group(3))

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"[il_xml] Failed to parse bill XML {filename}: {e}", flush=True)
        return None

    # Extract sponsor from sponsor block (fallback only)
    sponsor_name = None
    sponsor_paths = [
        './/PrimarySponsor/Name',
        './/PrimarySponsor',
        './/ChiefSponsor/Name',
        './/ChiefSponsor',
        './/Sponsor/Name',
        './/sponsor/sponsors',
        './/Sponsors/Sponsor',
    ]

    for path in sponsor_paths:
        elem = root.find(path)
        if elem is not None:
            text = elem.text
            if text and text.strip():
                # Handle comma-separated sponsors (take first)
                sponsor_name = text.strip().split(',')[0].strip()
                break

    # Extract title/synopsis
    title = _get_text(root, './/ShortTitle') or _get_text(root, './/Title') or ""
    synopsis = _get_text(root, './/Synopsis') or _get_text(root, './/Description') or ""

    # Extract actions (flat or structured) and detect Public Act
    public_act_number = None
    latest_action_text = None
    latest_action_date = None

    actions = _parse_actions(root)
    if actions:
        for action in actions:
            action_text = action.get("text") or ""
            action_date = action.get("date") or ""

            # Check for Public Act
            pa_match = PUBLIC_ACT_PATTERN.search(action_text)
            if pa_match:
                public_act_number = pa_match.group(1)

            # Track latest action
            if action_text:
                latest_action_text = action_text
                latest_action_date = action_date

    # Also check lastaction element
    last_action = root.find('.//lastaction')
    if last_action is not None:
        action_text = _get_text(last_action, 'action') or ""
        if action_text:
            pa_match = PUBLIC_ACT_PATTERN.search(action_text)
            if pa_match:
                public_act_number = pa_match.group(1)
            latest_action_text = action_text
            latest_action_date = _get_text(last_action, 'statusdate') or latest_action_date

    # Extract sponsor roles from actions
    primary_sponsor_name = _extract_primary_sponsor_from_actions(actions)
    chief_co_sponsors, co_sponsors = _extract_sponsor_changes_from_actions(actions)

    if not primary_sponsor_name:
        primary_sponsor_name = sponsor_name

    bill_id = f"{ga_session}-{bill_type}-{bill_number}"

    return {
        "bill_id": bill_id,
        "ga_session": ga_session,
        "bill_type": bill_type,
        "bill_number": bill_number,
        "sponsor_name_raw": primary_sponsor_name or sponsor_name,
        "primary_sponsor_name": primary_sponsor_name,
        "chief_co_sponsors": chief_co_sponsors,
        "co_sponsors": co_sponsors,
        "title": title[:500] if title else None,  # Truncate long titles
        "synopsis": synopsis[:1000] if synopsis else None,  # Truncate long synopsis
        "latest_action_text": latest_action_text[:500] if latest_action_text else None,
        "latest_action_date": latest_action_date,
        "public_act_number": public_act_number,
    }


def _get_text(element, path: str) -> Optional[str]:
    """Safely get text from XML element at path."""
    if element is None:
        return None
    found = element.find(path) if path.startswith('.') else element.find(path)
    if found is not None and found.text:
        return found.text.strip()
    return None


# =======================
# Data Fetching
# =======================
class ILDataFetcher:
    """Handles fetching XML data from ILGA FTP."""

    def __init__(self, ga_session: int = 104):
        self.ga_session = ga_session
        self.session_str = str(ga_session)
        self.members_url = f"{IL_FTP_ROOT}/Members"
        self.bills_url = f"{IL_FTP_ROOT}/legislation/{ga_session}/BillStatus/XML"

    def fetch_members(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Fetch both House and Senate member XMLs."""
        house_members = []
        senate_members = []

        # Fetch House members
        house_url = f"{self.members_url}/{self.ga_session}HouseMembers.xml"
        try:
            house_xml = il_fetch_xml(house_url)
            house_members = parse_members_xml(house_xml, "house", self.ga_session)
            print(f"[il_fetch] Fetched {len(house_members)} House members", flush=True)
        except Exception as e:
            print(f"[il_fetch] Error fetching House members: {e}", flush=True)

        time.sleep(IL_REQUEST_DELAY)

        # Fetch Senate members
        senate_url = f"{self.members_url}/{self.ga_session}SenateMembers.xml"
        try:
            senate_xml = il_fetch_xml(senate_url)
            senate_members = parse_members_xml(senate_xml, "senate", self.ga_session)
            print(f"[il_fetch] Fetched {len(senate_members)} Senate members", flush=True)
        except Exception as e:
            print(f"[il_fetch] Error fetching Senate members: {e}", flush=True)

        return house_members, senate_members

    def fetch_bill_list(self) -> List[str]:
        """Get list of bill XML filenames from directory listing."""
        try:
            files = il_fetch_directory_listing(self.bills_url)
            # Filter to HB and SB files only (exclude amendments, resolutions, etc. for now)
            bill_files = [f for f in files if re.match(r'\d{3}00(HB|SB)\d+\.xml', f, re.IGNORECASE)]
            print(f"[il_fetch] Found {len(bill_files)} bill XML files", flush=True)
            return bill_files
        except Exception as e:
            print(f"[il_fetch] Error fetching bill list: {e}", flush=True)
            return []

    def fetch_bill(self, filename: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single bill XML."""
        url = f"{self.bills_url}/{filename}"
        try:
            xml_content = il_fetch_xml(url)
            time.sleep(IL_REQUEST_DELAY)
            return parse_bill_xml(xml_content, filename, self.ga_session)
        except Exception as e:
            print(f"[il_fetch] Error fetching bill {filename}: {e}", flush=True)
            return None


# =======================
# Cache helpers
# =======================
def il_cache_path(ga_session: int) -> str:
    """Get cache file path for Illinois session."""
    return os.path.join(IL_CACHE_DIR, f"il_stats_{ga_session}.json")

def _remote_cache_url(base_url: str, filename: str) -> Optional[str]:
    if not base_url:
        return None
    base = base_url.strip().rstrip("/")
    if not base:
        return None
    return f"{base}/{filename}"


def _fetch_remote_cache(base_url: str, filename: str) -> Optional[Dict[str, Any]]:
    """Fetch cached JSON from a remote URL (e.g., GitHub raw)."""
    url = _remote_cache_url(base_url, filename)
    if not url:
        return None
    try:
        resp = requests.get(url, timeout=(5, 20))
        if resp.status_code != 200:
            return None
        data = resp.json()
        if isinstance(data, dict):
            return data
    except (ValueError, RequestException, Timeout, ReqConnErr):
        return None
    return None


def _il_remote_base_url() -> str:
    return (
        os.environ.get("REMOTE_IL_CACHE_BASE_URL")
        or os.environ.get("REMOTE_CACHE_BASE_URL", "")
    ).strip()


def load_il_cache(ga_session: int) -> Optional[Dict[str, Any]]:
    """Load cached Illinois stats from file or database."""
    # Try remote cache first (e.g., GitHub raw or object storage)
    remote = _fetch_remote_cache(_il_remote_base_url(), f"il_stats_{ga_session}.json")
    if remote:
        return remote

    # Try file cache first
    fp = il_cache_path(ga_session)
    if os.path.exists(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    # Try database cache
    try:
        cached = il_db.load_il_stats_cache(ga_session)
        if cached:
            return cached
    except Exception as e:
        print(f"[il_cache] Database cache lookup failed: {e}", flush=True)

    return None


def save_il_cache(ga_session: int, data: Dict[str, Any]) -> None:
    """Save Illinois stats to both file and database cache."""
    # Save to file (atomic write)
    tmp = il_cache_path(ga_session) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, il_cache_path(ga_session))

    # Save to database
    try:
        il_db.save_il_stats_cache(ga_session, data)
    except Exception as e:
        print(f"[il_cache] Database cache save failed: {e}", flush=True)


# =======================
# Helper: Rebuild stats from database
# =======================
def _rebuild_stats_from_db(ga_session: int, all_members: List[Dict[str, Any]], matcher: ILNameMatcher) -> Dict[str, Any]:
    """Rebuild stats from existing database records without fetching."""
    print(f"[il_build] Rebuilding stats from database for session {ga_session}", flush=True)

    # Load all bills from database
    db_bills = il_db.get_all_bills_for_session(ga_session)
    print(f"[il_build] Loaded {len(db_bills)} bills from database", flush=True)

    # Aggregate stats
    by_member: Dict[str, Dict[str, Any]] = {}
    laws: List[Dict[str, Any]] = []
    unmatched_count = 0

    for bill in db_bills:
        sponsor_id = bill.get("sponsor_member_id")
        primary_name = bill.get("primary_sponsor_name") or bill.get("sponsor_name_raw")

        if not sponsor_id and primary_name:
            # Try to match sponsor
            bill_type = bill.get("bill_type", "").lower()
            chamber = "house" if bill_type in ("hb", "hr", "hjr", "hjrca") else "senate"
            member = matcher.match(primary_name, chamber)
            if member:
                sponsor_id = member["member_id"]

        if not sponsor_id:
            unmatched_count += 1
            continue

        # Get member info
        member_info = next((m for m in all_members if m["member_id"] == sponsor_id), None)
        if not member_info:
            unmatched_count += 1
            continue

        # Initialize or update member record
        if sponsor_id not in by_member:
            by_member[sponsor_id] = {
                "memberId": sponsor_id,
                "sponsorName": member_info["name"],
                "party": member_info["party"],
                "chamber": member_info["chamber"],
                "district": member_info["district"],
                "sponsored_total": 0,
                "primary_sponsor_total": 0,
                "chief_co_sponsor_total": 0,
                "co_sponsor_total": 0,
                "enacted_total": 0,
                "public_act_numbers": [],
            }

        by_member[sponsor_id]["sponsored_total"] += 1
        by_member[sponsor_id]["primary_sponsor_total"] += 1

        # Count chief co-sponsors and co-sponsors
        bill_type = bill.get("bill_type", "").lower()
        fallback_chamber = "house" if bill_type in ("hb", "hr", "hjr", "hjrca") else "senate"

        chief_co = _coerce_json_list(bill.get("chief_co_sponsors"))
        for name in chief_co:
            chamber_hint = _infer_chamber_from_name(name) or fallback_chamber
            member = matcher.match(name, chamber_hint)
            if not member:
                unmatched_count += 1
                continue
            member_id = member["member_id"]
            if member_id not in by_member:
                by_member[member_id] = {
                    "memberId": member_id,
                    "sponsorName": member["name"],
                    "party": member["party"],
                    "chamber": member["chamber"],
                    "district": member["district"],
                    "sponsored_total": 0,
                    "primary_sponsor_total": 0,
                    "chief_co_sponsor_total": 0,
                    "co_sponsor_total": 0,
                    "enacted_total": 0,
                    "public_act_numbers": [],
                }
            by_member[member_id]["chief_co_sponsor_total"] += 1

        co_sponsors = _coerce_json_list(bill.get("co_sponsors"))
        for name in co_sponsors:
            chamber_hint = _infer_chamber_from_name(name) or fallback_chamber
            member = matcher.match(name, chamber_hint)
            if not member:
                unmatched_count += 1
                continue
            member_id = member["member_id"]
            if member_id not in by_member:
                by_member[member_id] = {
                    "memberId": member_id,
                    "sponsorName": member["name"],
                    "party": member["party"],
                    "chamber": member["chamber"],
                    "district": member["district"],
                    "sponsored_total": 0,
                    "primary_sponsor_total": 0,
                    "chief_co_sponsor_total": 0,
                    "co_sponsor_total": 0,
                    "enacted_total": 0,
                    "public_act_numbers": [],
                }
            by_member[member_id]["co_sponsor_total"] += 1

        # Check if enacted
        if bill.get("public_act_number"):
            by_member[sponsor_id]["enacted_total"] += 1
            by_member[sponsor_id]["public_act_numbers"].append(bill["public_act_number"])
            laws.append({
                "public_act_number": bill["public_act_number"],
                "bill_id": bill["bill_id"],
                "sponsor_member_id": sponsor_id,
            })

    # Build response
    rows = sorted(by_member.values(), key=lambda r: (-r["sponsored_total"], r["sponsorName"] or ""))
    years = SESSION_YEARS.get(ga_session, f"{ga_session}")

    stats = {
        "ga_session": ga_session,
        "years": years,
        "generated_at": int(time.time()),
        "rows": rows,
        "summary": {
            "total_legislators": len(rows),
            "total_bills": len(db_bills),
            "total_laws": len(laws),
        },
        "unmatched_sponsors": unmatched_count,
        "note": f"Data from Illinois General Assembly FTP XML files for the {ga_session}th GA ({years}).",
    }

    # Update cache
    save_il_cache(ga_session, stats)

    print(
        f"[il_build] Rebuilt from DB: {len(rows)} legislators, {len(db_bills)} bills, {len(laws)} laws",
        flush=True
    )

    return stats


# =======================
# Main aggregation
# =======================
def build_il_stats(ga_session: int, incremental: bool = True) -> Dict[str, Any]:
    """
    Main entry point: fetch all IL data and build statistics.

    Args:
        ga_session: The GA session number (e.g., 104)
        incremental: If True, only fetch bills not already in the database.
                     If False, fetch all bills (full refresh).
    """
    print(f"[il_build] Starting stats build for IL GA session {ga_session} (incremental={incremental})", flush=True)

    fetcher = ILDataFetcher(ga_session)

    # Step 1: Fetch members
    house_members, senate_members = fetcher.fetch_members()
    all_members = house_members + senate_members

    if not all_members:
        raise RuntimeError(f"No members found for IL GA session {ga_session}")

    print(f"[il_build] Total members: {len(all_members)}", flush=True)

    # Step 2: Build name matcher
    matcher = ILNameMatcher(all_members)

    # Step 3: Fetch bill list
    bill_files = fetcher.fetch_bill_list()

    if not bill_files:
        raise RuntimeError(f"No bill files found for IL GA session {ga_session}")

    # Step 3b: Filter to only new bills if incremental mode
    # Also check pending bills (without public_act_number) for status updates
    updated_pending_bills = []
    if incremental:
        existing_files = {f.lower() for f in il_db.get_existing_bill_filenames(ga_session)}
        original_count = len(bill_files)
        bill_files = [f for f in bill_files if f.lower() not in existing_files]
        print(f"[il_build] Incremental mode: {original_count} total, {len(existing_files)} existing, {len(bill_files)} new to fetch", flush=True)

        # Step 3c: Check pending bills for status updates (Option 1 & 2)
        pending_bills = il_db.get_pending_bills_for_update(ga_session)
        if pending_bills:
            print(f"[il_build] Checking {len(pending_bills)} pending bills for status updates...", flush=True)

            # Build lookup of pending bills by filename
            pending_by_file: Dict[str, Dict[str, Any]] = {}
            for pb in pending_bills:
                bill_type = pb["bill_type"].upper()
                bill_number = pb["bill_number"]
                filename = f"{ga_session}00{bill_type}{bill_number:04d}.xml"
                pending_by_file[filename.lower()] = pb

            # Re-fetch pending bills to check for updates
            pending_files = list(pending_by_file.keys())
            updates_found = 0

            with ThreadPoolExecutor(max_workers=IL_MAX_WORKERS) as pool:
                futures = {pool.submit(fetcher.fetch_bill, f): f for f in pending_files}
                for fut in as_completed(futures):
                    filename = futures[fut]
                    try:
                        new_bill = fut.result()
                        if not new_bill:
                            continue

                        old_bill = pending_by_file.get(filename.lower())
                        if not old_bill:
                            continue

                        # Option 2: Only update if latest_action_date changed
                        old_date = old_bill.get("latest_action_date") or ""
                        new_date = new_bill.get("latest_action_date") or ""

                        if new_date != old_date:
                            # Bill has been updated - check if it's now enacted
                            bill_id = new_bill.get("bill_id")
                            if bill_id:
                                il_db.update_il_bill(bill_id, {
                                    "public_act_number": new_bill.get("public_act_number"),
                                    "latest_action_date": new_date,
                                    "latest_action_text": new_bill.get("latest_action_text"),
                                })
                                updated_pending_bills.append(new_bill)
                                updates_found += 1
                                if new_bill.get("public_act_number"):
                                    print(f"[il_build] Bill {bill_id} is now Public Act {new_bill['public_act_number']}", flush=True)

                    except Exception as e:
                        print(f"[il_build] Error checking pending bill {filename}: {e}", flush=True)

            print(f"[il_build] Updated {updates_found} pending bills with new status", flush=True)

        if not bill_files and not updated_pending_bills:
            print(f"[il_build] No new bills to fetch, rebuilding stats from database", flush=True)
            # Load existing bills from database and rebuild stats
            return _rebuild_stats_from_db(ga_session, all_members, matcher)

    # Step 4: Fetch and parse bills (with parallel fetching, but rate-limited)
    bills = []
    errors = 0

    print(f"[il_build] Fetching {len(bill_files)} bills...", flush=True)

    with ThreadPoolExecutor(max_workers=IL_MAX_WORKERS) as pool:
        futures = {pool.submit(fetcher.fetch_bill, f): f for f in bill_files}
        done = 0
        for fut in as_completed(futures):
            try:
                bill = fut.result()
                if bill:
                    bills.append(bill)
                else:
                    errors += 1
            except Exception as e:
                print(f"[il_build] Error processing {futures[fut]}: {e}", flush=True)
                errors += 1

            done += 1
            if done % 100 == 0 or done == len(bill_files):
                print(f"[il_build] Bills fetched: {done}/{len(bill_files)} (errors: {errors})", flush=True)

    print(f"[il_build] Successfully parsed {len(bills)} new bills", flush=True)

    # Step 4b: In incremental mode, merge with existing bills from database
    if incremental:
        existing_bills = il_db.get_all_bills_for_session(ga_session)
        print(f"[il_build] Merging with {len(existing_bills)} existing bills from database", flush=True)
        # Existing bills go first, new bills override by bill_id.
        # Updated pending bills also override (they have fresh data from server).
        merged_by_id: Dict[str, Dict[str, Any]] = {}
        for bill in existing_bills:
            bill_id = bill.get("bill_id")
            if not bill_id:
                continue
            merged_by_id[bill_id] = bill
        for bill in bills:
            bill_id = bill.get("bill_id")
            if not bill_id:
                continue
            merged_by_id[bill_id] = bill
        # Include updated pending bills (bills that were re-fetched due to status change)
        for bill in updated_pending_bills:
            bill_id = bill.get("bill_id")
            if not bill_id:
                continue
            merged_by_id[bill_id] = bill
        all_bills = list(merged_by_id.values())
        removed_duplicates = (len(existing_bills) + len(bills) + len(updated_pending_bills)) - len(all_bills)
        if removed_duplicates > 0:
            print(f"[il_build] Deduped {removed_duplicates} overlapping bills during merge", flush=True)
    else:
        all_bills = bills

    # Step 5: Match sponsors and aggregate
    by_member: Dict[str, Dict[str, Any]] = {}
    laws: List[Dict[str, Any]] = []
    unmatched_count = 0

    for bill in all_bills:
        primary_name = bill.get("primary_sponsor_name") or bill.get("sponsor_name_raw")
        if not primary_name:
            unmatched_count += 1
            continue

        # Determine chamber from bill type
        bill_type = bill.get("bill_type", "").lower()
        chamber = "house" if bill_type in ("hb", "hr", "hjr", "hjrca") else "senate"

        # Match primary sponsor to member
        member = matcher.match(primary_name, chamber)
        if not member:
            unmatched_count += 1
            continue

        member_id = member["member_id"]

        # Initialize or update member record
        if member_id not in by_member:
            by_member[member_id] = {
                "memberId": member_id,
                "sponsorName": member["name"],
                "party": member["party"],
                "chamber": member["chamber"],
                "district": member["district"],
                "sponsored_total": 0,
                "primary_sponsor_total": 0,
                "chief_co_sponsor_total": 0,
                "co_sponsor_total": 0,
                "enacted_total": 0,
                "public_act_numbers": [],
            }

        by_member[member_id]["sponsored_total"] += 1
        by_member[member_id]["primary_sponsor_total"] += 1

        # Track sponsor for bill
        bill["sponsor_member_id"] = member_id
        bill["primary_sponsor_name"] = primary_name

        # Count chief co-sponsors and co-sponsors
        chief_co = _coerce_json_list(bill.get("chief_co_sponsors"))
        bill["chief_co_sponsors"] = chief_co
        for name in chief_co:
            chamber_hint = _infer_chamber_from_name(name) or chamber
            sponsor_member = matcher.match(name, chamber_hint)
            if not sponsor_member:
                unmatched_count += 1
                continue
            sponsor_id = sponsor_member["member_id"]
            if sponsor_id not in by_member:
                by_member[sponsor_id] = {
                    "memberId": sponsor_id,
                    "sponsorName": sponsor_member["name"],
                    "party": sponsor_member["party"],
                    "chamber": sponsor_member["chamber"],
                    "district": sponsor_member["district"],
                    "sponsored_total": 0,
                    "primary_sponsor_total": 0,
                    "chief_co_sponsor_total": 0,
                    "co_sponsor_total": 0,
                    "enacted_total": 0,
                    "public_act_numbers": [],
                }
            by_member[sponsor_id]["chief_co_sponsor_total"] += 1

        co_sponsors = _coerce_json_list(bill.get("co_sponsors"))
        bill["co_sponsors"] = co_sponsors
        for name in co_sponsors:
            chamber_hint = _infer_chamber_from_name(name) or chamber
            sponsor_member = matcher.match(name, chamber_hint)
            if not sponsor_member:
                unmatched_count += 1
                continue
            sponsor_id = sponsor_member["member_id"]
            if sponsor_id not in by_member:
                by_member[sponsor_id] = {
                    "memberId": sponsor_id,
                    "sponsorName": sponsor_member["name"],
                    "party": sponsor_member["party"],
                    "chamber": sponsor_member["chamber"],
                    "district": sponsor_member["district"],
                    "sponsored_total": 0,
                    "primary_sponsor_total": 0,
                    "chief_co_sponsor_total": 0,
                    "co_sponsor_total": 0,
                    "enacted_total": 0,
                    "public_act_numbers": [],
                }
            by_member[sponsor_id]["co_sponsor_total"] += 1

        # Check if enacted
        if bill.get("public_act_number"):
            by_member[member_id]["enacted_total"] += 1
            by_member[member_id]["public_act_numbers"].append(bill["public_act_number"])
            laws.append({
                "public_act_number": bill["public_act_number"],
                "bill_id": bill["bill_id"],
                "sponsor_member_id": member_id,
            })

    # Add unmatched from matcher
    unmatched_count += len(matcher.unmatched)

    # Step 6: Build response
    rows = sorted(by_member.values(), key=lambda r: (-r["sponsored_total"], r["sponsorName"] or ""))

    # Get session years
    years = SESSION_YEARS.get(ga_session, f"{ga_session}")

    # Deduplicate bills for accurate count (in case of overlap)
    unique_bill_ids = set(b.get("bill_id") for b in all_bills if b.get("bill_id"))
    total_bills = len(unique_bill_ids)

    stats = {
        "ga_session": ga_session,
        "years": years,
        "generated_at": int(time.time()),
        "rows": rows,
        "summary": {
            "total_legislators": len(rows),
            "total_bills": total_bills,
            "total_laws": len(laws),
        },
        "unmatched_sponsors": unmatched_count,
        "note": f"Data from Illinois General Assembly FTP XML files for the {ga_session}th GA ({years}).",
    }

    new_bills_count = len(bills)
    print(
        f"[il_build] Final: {len(rows)} legislators, {total_bills} total bills ({new_bills_count} new), {len(laws)} laws, "
        f"{unmatched_count} unmatched sponsors",
        flush=True
    )

    # Step 7: Save to database (only save new bills, not re-save existing)
    try:
        il_db.save_il_legislators_batch(ga_session, all_members)
        if bills:  # Only save if there are new bills
            il_db.save_il_bills_batch(ga_session, bills)
        il_db.save_il_laws_batch(ga_session, laws)
        il_db.save_il_stats_cache(ga_session, stats)
        print(f"[il_db] Persisted data for IL session {ga_session}", flush=True)
    except Exception as e:
        print(f"[il_db] Warning: Failed to persist to database: {e}", flush=True)

    return stats


# =======================
# Background refresh
# =======================
def do_il_background_refresh(ga_session: int):
    """Run Illinois stats refresh in background, updating status."""
    global _il_refresh_status
    _il_refresh_status[ga_session] = {"status": "running", "started_at": int(time.time())}
    try:
        stats = build_il_stats(ga_session)
        save_il_cache(ga_session, stats)
        _il_refresh_status[ga_session] = {
            "status": "completed",
            "completed_at": int(time.time()),
            "summary": stats.get("summary", {}),
        }
    except Exception as e:
        _il_refresh_status[ga_session] = {
            "status": "error",
            "error": str(e),
            "completed_at": int(time.time()),
        }


def get_il_refresh_status(ga_session: int) -> Dict[str, Any]:
    """Get the current refresh status for an Illinois session."""
    return _il_refresh_status.get(ga_session, {"status": "none"})


def get_session_years(ga_session: int) -> str:
    """Get the years for a GA session."""
    return SESSION_YEARS.get(ga_session, str(ga_session))


def get_available_sessions() -> List[Dict[str, Any]]:
    """Return list of available IL GA sessions."""
    return [
        {"session": 104, "years": "2025-2026", "current": True},
        {"session": 103, "years": "2023-2024", "current": False},
    ]
