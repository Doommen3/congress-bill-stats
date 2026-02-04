"""
Live factual checks for the deployed site.

These tests are opt-in because they call live services and can fail when the
deployed cache is stale versus source-of-truth datasets.
"""
import os
import re
from typing import Dict, Set

import pytest
import requests


RUN_LIVE = os.getenv("RUN_LIVE_FACT_CHECKS") == "1"

LIVE_SITE_URL = os.getenv("LIVE_SITE_URL", "https://congress-bill-stats.onrender.com").rstrip("/")
LIVE_CONGRESS = int(os.getenv("LIVE_CONGRESS", "119"))
LIVE_IL_SESSION = int(os.getenv("LIVE_IL_SESSION", "104"))
REQUEST_TIMEOUT = int(os.getenv("LIVE_REQUEST_TIMEOUT", "90"))

CONGRESS_API_ROOT = os.getenv("LIVE_CONGRESS_API_ROOT", "https://api.congress.gov/v3").rstrip("/")
CONGRESS_API_KEY = os.getenv("LIVE_CONGRESS_API_KEY", os.getenv("CONGRESS_API_KEY", "DEMO_KEY"))
ILGA_BILLSTATUS_DIR = os.getenv(
    "LIVE_ILGA_BILLSTATUS_DIR",
    f"https://ilga.gov/ftp/legislation/{LIVE_IL_SESSION}/BillStatus/XML",
).rstrip("/")
ILGA_PUBLIC_ACTS_URL = os.getenv("LIVE_ILGA_PUBLIC_ACTS_URL", "https://www.ilga.gov/legislation/publicacts/").rstrip("/")


pytestmark = [
    pytest.mark.live_factual,
    pytest.mark.skipif(not RUN_LIVE, reason="Set RUN_LIVE_FACT_CHECKS=1 to run live factual checks."),
]


def _get_json(url: str, params: Dict[str, object] | None = None, headers: Dict[str, str] | None = None) -> Dict[str, object]:
    resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _get_text(url: str) -> str:
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def _congress_count(path: str) -> int:
    payload = _get_json(
        f"{CONGRESS_API_ROOT}/{path.lstrip('/')}",
        params={"limit": 1, "offset": 0, "format": "json"},
        headers={"X-Api-Key": CONGRESS_API_KEY, "Accept": "application/json"},
    )
    return int((payload.get("pagination") or {}).get("count") or 0)


def _extract_hb_sb_bill_files(directory_html: str) -> Set[str]:
    hrefs = re.findall(r'href=["\']?([^"\'> ]+)', directory_html, flags=re.IGNORECASE)
    filenames = {h.rsplit("/", 1)[-1] for h in hrefs}
    hb_sb_pattern = re.compile(r"^\d{3}00(HB|SB)\d+\.xml$", re.IGNORECASE)
    return {f for f in filenames if hb_sb_pattern.match(f)}


def _extract_public_act_numbers(public_acts_html: str, ga_session: int) -> Set[str]:
    pattern = re.compile(rf"\b{ga_session}-\d{{4}}\b")
    return set(pattern.findall(public_acts_html))


def test_live_congress_summary_matches_authoritative_sources():
    stats = _get_json(f"{LIVE_SITE_URL}/api/stats", params={"congress": LIVE_CONGRESS})
    summary = stats["summary"]
    rows = stats["rows"]

    # Internal consistency of what's displayed.
    assert summary["total_legislators"] == len(rows)
    assert summary["total_bills"] == sum((r.get("primary_sponsor_total") or r.get("sponsored_total") or 0) for r in rows)
    assert summary["public_laws"] == sum(r.get("public_law_count", 0) for r in rows)
    assert summary["private_laws"] == sum(r.get("private_law_count", 0) for r in rows)
    assert summary["total_laws"] == sum(r.get("enacted_total", 0) for r in rows)

    # External factual checks against Congress.gov.
    source_total_bills = _congress_count(f"/bill/{LIVE_CONGRESS}")
    source_public_laws = _congress_count(f"/law/{LIVE_CONGRESS}/pub")
    source_private_laws = _congress_count(f"/law/{LIVE_CONGRESS}/priv")

    mismatches = []
    if summary["total_bills"] != source_total_bills:
        mismatches.append(f"total_bills site={summary['total_bills']} source={source_total_bills}")
    if summary["public_laws"] != source_public_laws:
        mismatches.append(f"public_laws site={summary['public_laws']} source={source_public_laws}")
    if summary["private_laws"] != source_private_laws:
        mismatches.append(f"private_laws site={summary['private_laws']} source={source_private_laws}")
    if summary["total_laws"] != source_public_laws + source_private_laws:
        mismatches.append(
            f"total_laws site={summary['total_laws']} source={source_public_laws + source_private_laws}"
        )

    assert not mismatches, "; ".join(mismatches)


def test_live_ilga_summary_matches_authoritative_sources():
    stats = _get_json(f"{LIVE_SITE_URL}/api/il-stats", params={"session": LIVE_IL_SESSION})
    summary = stats["summary"]
    rows = stats["rows"]

    # Internal consistency of what's displayed.
    assert summary["total_legislators"] == len(rows)
    assert summary["total_laws"] == sum(r.get("enacted_total", 0) for r in rows)

    # External factual checks against ILGA.
    billstatus_html = _get_text(ILGA_BILLSTATUS_DIR)
    source_total_bills = len(_extract_hb_sb_bill_files(billstatus_html))

    public_acts_html = _get_text(ILGA_PUBLIC_ACTS_URL)
    source_total_public_acts = len(_extract_public_act_numbers(public_acts_html, LIVE_IL_SESSION))

    mismatches = []
    if summary["total_bills"] != source_total_bills:
        mismatches.append(f"total_bills site={summary['total_bills']} source={source_total_bills}")
    if summary["total_laws"] != source_total_public_acts:
        mismatches.append(f"total_laws site={summary['total_laws']} source={source_total_public_acts}")

    assert not mismatches, "; ".join(mismatches)
