"""
Targeted test for Congress /law response shape parsing.
This test does not call the live API. It uses a local JSON fixture.
"""
import json
import os
import pytest
import requests

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import _extract_laws


FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "law_response_sample.json")


def _load_fixture():
    if not os.path.exists(FIXTURE_PATH):
        return None
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return None


def test_extract_laws_from_sample_response():
    """
    Paste a real /law endpoint JSON response into the fixture file.
    The test will fail if _extract_laws cannot find the list of laws.
    """
    payload = _load_fixture()
    if not payload:
        pytest.skip("No fixture JSON found or invalid JSON.")
    if payload.get("_placeholder"):
        pytest.skip("Fixture is a placeholder. Replace with real /law response JSON.")

    laws = _extract_laws(payload)
    if not laws:
        top_keys = list(payload.keys())
        data = payload.get("data")
        data_keys = list(data.keys()) if isinstance(data, dict) else []
        pytest.fail(
            "No laws extracted from fixture. "
            f"Top-level keys={top_keys}, data keys={data_keys}"
        )


def test_extract_laws_from_live_api():
    """
    Live smoke test against the /law endpoint with limit=1.
    Skips if CONGRESS_API_KEY is not set.
    """
    api_key = os.environ.get("CONGRESS_API_KEY")
    if not api_key:
        pytest.skip("CONGRESS_API_KEY not set; skipping live /law test.")

    api_root = os.environ.get("CONGRESS_API_ROOT", "https://api.data.gov/congress/v3")
    congress = os.environ.get("DEFAULT_CONGRESS", "119")
    url = f"{api_root.rstrip('/')}/law/{congress}/pub"
    params = {"limit": 1, "offset": 0, "format": "json"}
    headers = {"X-Api-Key": api_key, "Accept": "application/json"}

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    if resp.status_code != 200:
        pytest.fail(f"Live /law request failed: {resp.status_code} {resp.text[:300]}")

    payload = resp.json()
    laws = _extract_laws(payload)
    if not laws:
        top_keys = list(payload.keys())
        data = payload.get("data")
        data_keys = list(data.keys()) if isinstance(data, dict) else []
        pytest.fail(
            "No laws extracted from live response. "
            f"Top-level keys={top_keys}, data keys={data_keys}"
        )
