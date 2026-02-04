"""
Helpers for ingesting Congress Bill Status bulk XML files.
"""
import os
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional


def _local_name(tag: str) -> str:
    if not tag:
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _iter_by_name(root: ET.Element, name: str):
    for elem in root.iter():
        if _local_name(elem.tag) == name:
            yield elem


def _find_first(root: ET.Element, name: str) -> Optional[ET.Element]:
    for elem in _iter_by_name(root, name):
        return elem
    return None


def _find_text(root: ET.Element, names: List[str]) -> Optional[str]:
    for name in names:
        elem = _find_first(root, name)
        if elem is not None and elem.text:
            value = elem.text.strip()
            if value:
                return value
    return None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("true", "t", "yes", "y", "1")
    return False


def _extract_bioguide(item: ET.Element) -> Optional[str]:
    for key in ("bioguideId", "bioguideID", "bioguide"):
        if key in item.attrib and item.attrib[key]:
            return item.attrib[key].strip()
    return _find_text(item, ["bioguideId", "bioguideID", "bioguide"])


def _extract_cosponsor(item: ET.Element) -> Optional[Dict[str, Any]]:
    bioguide = _extract_bioguide(item)
    if not bioguide:
        return None

    withdrawn_date = _find_text(item, ["withdrawnDate", "sponsorshipWithdrawnDate", "withdrawalDate"])
    withdrawn_flag = _find_text(item, ["isWithdrawn", "withdrawn"])
    is_original_raw = _find_text(item, ["isOriginalCosponsor", "originalCosponsor", "isOriginal"])

    return {
        "bioguideId": bioguide,
        "fullName": _find_text(item, ["fullName", "name"]),
        "party": _find_text(item, ["party"]),
        "state": _find_text(item, ["state"]),
        "chamber": _find_text(item, ["chamber"]),
        "is_original": _boolish(is_original_raw),
        "withdrawn": bool(withdrawn_date) or _boolish(withdrawn_flag),
    }


def _extract_cosponsors(root: ET.Element) -> List[Dict[str, Any]]:
    cosponsors_parent = _find_first(root, "cosponsors")
    if cosponsors_parent is None:
        return []

    out: List[Dict[str, Any]] = []
    for item in cosponsors_parent:
        if _local_name(item.tag) not in ("item", "cosponsor"):
            continue
        normalized = _extract_cosponsor(item)
        if normalized:
            out.append(normalized)
    return out


def _extract_primary_sponsor(root: ET.Element) -> Optional[Dict[str, Any]]:
    sponsors_parent = _find_first(root, "sponsors")
    if sponsors_parent is None:
        return None

    for item in sponsors_parent:
        if _local_name(item.tag) not in ("item", "sponsor"):
            continue
        bioguide = _extract_bioguide(item)
        if not bioguide:
            continue
        return {
            "bioguideId": bioguide,
            "fullName": _find_text(item, ["fullName", "name"]),
            "party": _find_text(item, ["party"]),
            "state": _find_text(item, ["state"]),
            "chamber": _find_text(item, ["chamber"]),
        }
    return None


def parse_bill_status_xml(xml_text: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single Bill Status XML payload.
    Returns normalized bill payload with sponsor/cosponsors.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None

    congress_raw = _find_text(root, ["congress"])
    bill_type = _find_text(root, ["billType", "type"])
    bill_number_raw = _find_text(root, ["billNumber", "number"])

    if not (congress_raw and bill_type and bill_number_raw):
        return None

    try:
        congress = int(congress_raw)
        bill_number = int(str(bill_number_raw).strip())
    except (TypeError, ValueError):
        return None

    bill_type = bill_type.strip().lower()
    bill_id = f"{congress}-{bill_type}-{bill_number}"

    return {
        "bill_id": bill_id,
        "congress": congress,
        "bill_type": bill_type,
        "bill_number": bill_number,
        "update_date": _find_text(root, ["updateDateIncludingText", "updateDate"]),
        "sponsor": _extract_primary_sponsor(root),
        "cosponsors": _extract_cosponsors(root),
    }


def parse_bill_status_file(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            xml_text = f.read()
    except OSError:
        return None
    return parse_bill_status_xml(xml_text)


def _discover_xml_files(base_dir: str) -> List[str]:
    if not base_dir or not os.path.isdir(base_dir):
        return []
    files: List[str] = []
    for root, _, filenames in os.walk(base_dir):
        for filename in filenames:
            if filename.lower().endswith(".xml"):
                files.append(os.path.join(root, filename))
    files.sort()
    return files


def load_bulk_bill_status(
    congress: int,
    base_dir: str,
    max_workers: int = 4,
) -> Dict[str, Dict[str, Any]]:
    """
    Load Bill Status bulk XML files for a target congress.
    Returns mapping: bill_id -> parsed bill payload.
    """
    paths = _discover_xml_files(base_dir)
    if not paths:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(parse_bill_status_file, path): path for path in paths}
        for fut in as_completed(futs):
            parsed = fut.result()
            if not parsed:
                continue
            if parsed.get("congress") != congress:
                continue
            bill_id = parsed.get("bill_id")
            if not bill_id:
                continue
            out[bill_id] = parsed
    return out

