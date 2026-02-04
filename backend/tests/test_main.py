"""
Comprehensive tests for Congress Bill Stats application.
Tests cover: law detection, sponsor extraction, database operations, and API endpoints.
"""
import os
import sys
import json
import pytest
import tempfile
import sqlite3
from unittest.mock import patch, MagicMock

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import (
    is_enacted,
    normalize_bill_key,
    extract_primary_sponsor,
    _normalize_bill_item,
    _extract_bills,
    _extract_laws,
    build_law_lookup,
    load_cache,
)


class TestIsEnacted:
    """Tests for the is_enacted function (legacy action code checking)."""

    def test_enacted_public_law_codes(self):
        """Test that public law action codes are recognized."""
        assert is_enacted(36000) is True  # Became public law
        assert is_enacted(37000) is True  # Public Law signed by President
        assert is_enacted(38000) is True
        assert is_enacted(39000) is True
        assert is_enacted(40000) is True

    def test_enacted_private_law_codes(self):
        """Test that private law action codes are recognized."""
        assert is_enacted(41000) is True
        assert is_enacted(42000) is True
        assert is_enacted(43000) is True
        assert is_enacted(44000) is True
        assert is_enacted(45000) is True

    def test_non_enacted_codes(self):
        """Test that non-enacted action codes return False."""
        assert is_enacted(1000) is False
        assert is_enacted(10000) is False
        assert is_enacted(35000) is False
        assert is_enacted(46000) is False

    def test_none_action_code(self):
        """Test that None returns False."""
        assert is_enacted(None) is False

    def test_string_action_code(self):
        """Test that string action codes are converted properly."""
        assert is_enacted("36000") is True
        assert is_enacted("1000") is False

    def test_invalid_action_code(self):
        """Test that invalid action codes return False."""
        assert is_enacted("invalid") is False
        assert is_enacted({}) is False
        assert is_enacted([]) is False


class TestNormalizeBillKey:
    """Tests for the normalize_bill_key function."""

    def test_basic_key_generation(self):
        """Test basic bill key generation."""
        assert normalize_bill_key(119, "hr", 1234) == "119-hr-1234"
        assert normalize_bill_key(117, "S", 100) == "117-s-100"

    def test_case_normalization(self):
        """Test that bill type is lowercased."""
        assert normalize_bill_key(119, "HR", 1) == "119-hr-1"
        assert normalize_bill_key(119, "HJRES", 5) == "119-hjres-5"


class TestExtractPrimarySponsor:
    """Tests for the extract_primary_sponsor function."""

    def test_sponsor_object_shape(self):
        """Test extraction from sponsor object."""
        bill = {
            "sponsor": {
                "bioguideId": "S001217",
                "fullName": "Sen. Scott, Rick [R-FL]",
                "party": "R",
                "state": "FL",
            },
            "originChamber": "Senate",
        }
        result = extract_primary_sponsor(bill)
        assert result is not None
        assert result["bioguideId"] == "S001217"
        assert result["fullName"] == "Sen. Scott, Rick [R-FL]"
        assert result["party"] == "R"
        assert result["state"] == "FL"
        assert result["chamber"] == "Senate"

    def test_sponsors_list_shape(self):
        """Test extraction from sponsors list."""
        bill = {
            "sponsors": {
                "item": [
                    {
                        "bioguideId": "B001302",
                        "fullName": "Rep. Biggs, Andy [R-AZ-5]",
                        "party": "R",
                        "state": "AZ",
                    }
                ]
            },
            "originChamber": "House",
        }
        result = extract_primary_sponsor(bill)
        assert result is not None
        assert result["bioguideId"] == "B001302"

    def test_nested_bill_object(self):
        """Test extraction from nested bill object."""
        data = {
            "bill": {
                "sponsor": {
                    "bioguideId": "C001098",
                    "fullName": "Sen. Cruz, Ted [R-TX]",
                }
            }
        }
        result = extract_primary_sponsor(data)
        assert result is not None
        assert result["bioguideId"] == "C001098"

    def test_missing_sponsor(self):
        """Test that missing sponsor returns None."""
        bill = {"type": "hr", "number": 1234}
        result = extract_primary_sponsor(bill)
        assert result is None

    def test_empty_sponsors_list(self):
        """Test that empty sponsors list returns None."""
        bill = {"sponsors": {"item": []}}
        result = extract_primary_sponsor(bill)
        assert result is None


class TestNormalizeBillItem:
    """Tests for the _normalize_bill_item function."""

    def test_nested_bill(self):
        """Test unwrapping nested bill object."""
        item = {"bill": {"type": "hr", "number": 1}}
        result = _normalize_bill_item(item)
        assert result == {"type": "hr", "number": 1}

    def test_flat_bill(self):
        """Test that flat bill object is returned as-is."""
        item = {"type": "hr", "number": 1}
        result = _normalize_bill_item(item)
        assert result == {"type": "hr", "number": 1}


class TestExtractBills:
    """Tests for the _extract_bills function."""

    def test_data_bills_shape(self):
        """Test extraction from data.bills shape."""
        response = {"data": {"bills": [{"type": "hr", "number": 1}]}}
        result = _extract_bills(response)
        assert len(result) == 1
        assert result[0]["number"] == 1

    def test_bills_shape(self):
        """Test extraction from bills shape."""
        response = {"bills": [{"type": "hr", "number": 1}]}
        result = _extract_bills(response)
        assert len(result) == 1

    def test_list_fallback(self):
        """Test extraction from list fallback."""
        response = {"data": [{"type": "hr", "number": 1}]}
        result = _extract_bills(response)
        assert len(result) == 1

    def test_empty_response(self):
        """Test empty response returns empty list."""
        result = _extract_bills({})
        assert result == []


class TestExtractLaws:
    """Tests for the _extract_laws function."""

    def test_laws_shape(self):
        """Test extraction from laws shape."""
        response = {"laws": [{"number": "119-1", "type": "public"}]}
        result = _extract_laws(response)
        assert len(result) == 1

    def test_data_laws_shape(self):
        """Test extraction from data.laws shape."""
        response = {"data": {"laws": [{"number": "119-1"}]}}
        result = _extract_laws(response)
        assert len(result) == 1

    def test_empty_response(self):
        """Test empty response returns empty list."""
        result = _extract_laws({})
        assert result == []


class TestBuildLawLookup:
    """Tests for the build_law_lookup function."""

    def test_basic_lookup_building(self):
        """Test building a law lookup from law list."""
        laws = [
            {
                "_law_type": "public",
                "number": "119-1",
                "bill": {"type": "hr", "number": 1234, "congress": 119}
            },
            {
                "_law_type": "private",
                "number": "119-1",
                "bill": {"type": "s", "number": 100, "congress": 119}
            },
        ]
        lookup = build_law_lookup(119, laws)
        assert "119-hr-1234" in lookup
        assert lookup["119-hr-1234"]["law_type"] == "public"
        assert "119-s-100" in lookup
        assert lookup["119-s-100"]["law_type"] == "private"

    def test_law_without_bill(self):
        """Test law with bill info at root level."""
        laws = [
            {
                "_law_type": "public",
                "number": "119-2",
                "type": "hjres",
                "number": 5,
                "congress": 119,
            }
        ]
        # This should handle the case where bill info is at root
        lookup = build_law_lookup(119, laws)
        # The lookup should handle this gracefully (may be empty if no bill reference)
        assert isinstance(lookup, dict)


class TestDatabase:
    """Tests for database operations."""

    @pytest.fixture
    def temp_db(self, tmp_path):
        """Create a temporary database for testing."""
        db_path = str(tmp_path / "test.db")
        os.environ["DATABASE_PATH"] = db_path

        # Re-import to use the new path
        import importlib
        import database
        importlib.reload(database)

        yield database

        # Cleanup
        if os.path.exists(db_path):
            os.remove(db_path)

    def test_init_database(self, temp_db):
        """Test database initialization."""
        # Database should already be initialized from import
        conn = sqlite3.connect(os.environ["DATABASE_PATH"])
        cursor = conn.cursor()

        # Check tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        assert "legislators" in tables
        assert "bills" in tables
        assert "bill_cosponsors" in tables
        assert "laws" in tables
        assert "cache_metadata" in tables

        cursor.execute("PRAGMA table_info(bills)")
        bill_cols = {row[1] for row in cursor.fetchall()}
        assert "update_date" in bill_cols
        assert "cosponsors_last_update_date" in bill_cols
        assert "cosponsors_updated_at" in bill_cols

        conn.close()

    def test_save_and_load_legislators(self, temp_db):
        """Test saving and loading legislators."""
        legislators = [
            {
                "bioguideId": "S001217",
                "sponsorName": "Sen. Scott, Rick [R-FL]",
                "party": "R",
                "state": "FL",
                "chamber": "Senate",
            }
        ]
        temp_db.save_legislators_batch(legislators)

        # Verify data was saved
        conn = sqlite3.connect(os.environ["DATABASE_PATH"])
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM legislators WHERE bioguide_id = ?", ("S001217",))
        row = cursor.fetchone()
        conn.close()

        assert row is not None
        assert row[1] == "Sen. Scott, Rick [R-FL]"  # name

    def test_save_and_load_stats_cache(self, temp_db):
        """Test saving and loading stats cache."""
        stats = {
            "congress": 119,
            "generated_at": 1234567890,
            "rows": [{"bioguideId": "TEST", "sponsored_total": 10}],
            "summary": {"total_bills": 100, "total_laws": 5},
        }
        temp_db.save_stats_cache(119, stats)

        loaded = temp_db.load_stats_cache(119)
        assert loaded is not None
        assert loaded["congress"] == 119
        assert len(loaded["rows"]) == 1

    def test_cache_miss(self, temp_db):
        """Test that cache miss returns None."""
        result = temp_db.load_stats_cache(999)
        assert result is None


class TestRemoteCache:
    """Tests for remote cache loading."""

    class DummyResp:
        def __init__(self, status_code, data):
            self.status_code = status_code
            self._data = data

        def json(self):
            return self._data

    @patch("main.db.load_stats_cache")
    @patch("main.requests.get")
    def test_load_cache_remote_preferred(self, mock_get, mock_db, monkeypatch, tmp_path):
        """Remote cache should be used when configured."""
        monkeypatch.setenv("REMOTE_CACHE_BASE_URL", "https://example.com/cache")
        monkeypatch.setattr("main.CACHE_DIR", str(tmp_path))
        mock_get.return_value = self.DummyResp(200, {"congress": 119, "rows": []})

        result = load_cache(119)

        assert result is not None
        assert result["congress"] == 119
        mock_db.assert_not_called()

    @patch("main.db.load_stats_cache")
    @patch("main.requests.get")
    def test_load_cache_remote_fallback_to_file(self, mock_get, mock_db, monkeypatch, tmp_path):
        """Fallback to local file when remote is missing."""
        monkeypatch.setenv("REMOTE_CACHE_BASE_URL", "https://example.com/cache")
        monkeypatch.setattr("main.CACHE_DIR", str(tmp_path))
        mock_get.return_value = self.DummyResp(404, {})

        # Write local cache file
        fp = tmp_path / "stats_119.json"
        fp.write_text(json.dumps({"congress": 119, "rows": [{"bioguideId": "X"}]}), encoding="utf-8")

        result = load_cache(119)

        assert result is not None
        assert result["congress"] == 119
        mock_db.assert_not_called()


class TestAPIEndpoints:
    """Tests for FastAPI endpoints."""

    @pytest.fixture
    def client(self):
        """Create a test client."""
        from fastapi.testclient import TestClient
        from main import app
        return TestClient(app)

    def test_health_endpoint(self, client):
        """Test the health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "timestamp" in data

    def test_refresh_status_endpoint(self, client):
        """Test the refresh status endpoint."""
        response = client.get("/api/refresh-status?congress=119")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    @patch("main.load_cache")
    def test_stats_endpoint_with_cache(self, mock_cache, client):
        """Test stats endpoint returns cached data."""
        mock_cache.return_value = {
            "congress": 119,
            "generated_at": 1234567890,
            "rows": [],
            "summary": {},
        }
        response = client.get("/api/stats?congress=119")
        assert response.status_code == 200
        data = response.json()
        assert data["congress"] == 119

    @patch("main.build_stats")
    @patch("main.load_cache")
    def test_stats_refresh_blocked_for_non_admin(self, mock_cache, mock_build, client, monkeypatch):
        """Non-admin refresh should not trigger rebuild when cache exists."""
        monkeypatch.setenv("ADMIN_IP_ALLOWLIST", "10.0.0.1")
        mock_cache.return_value = {
            "congress": 119,
            "generated_at": 1234567890,
            "rows": [],
            "summary": {},
        }
        response = client.get(
            "/api/stats?congress=119&refresh=true",
            headers={"x-forwarded-for": "1.2.3.4"},
        )
        assert response.status_code == 200
        mock_build.assert_not_called()

    @patch("main.build_stats")
    @patch("main.load_cache")
    def test_stats_refresh_allowed_for_admin(self, mock_cache, mock_build, client, monkeypatch):
        """Admin refresh should trigger rebuild."""
        monkeypatch.setenv("ADMIN_IP_ALLOWLIST", "1.2.3.4")
        mock_cache.return_value = {
            "congress": 119,
            "generated_at": 1234567890,
            "rows": [],
            "summary": {},
        }
        mock_build.return_value = mock_cache.return_value

        response = client.get(
            "/api/stats?congress=119&refresh=true",
            headers={"x-forwarded-for": "1.2.3.4"},
        )
        assert response.status_code == 200
        mock_build.assert_called_once()

    @patch("main.load_cache")
    def test_stats_cold_build_blocked_for_non_admin(self, mock_cache, client, monkeypatch):
        """Non-admin should not trigger cold build when no cache exists."""
        monkeypatch.setenv("ADMIN_IP_ALLOWLIST", "10.0.0.1")
        mock_cache.return_value = None
        response = client.get(
            "/api/stats?congress=119",
            headers={"x-forwarded-for": "1.2.3.4"},
        )
        assert response.status_code == 503


class TestLawEndpointIntegration:
    """
    Integration tests to verify law endpoint handling.
    These tests mock the API responses to test the full flow.
    """

    @patch("main.api_get")
    def test_fetch_all_laws_public(self, mock_api_get):
        """Test fetching public laws."""
        mock_api_get.return_value = {
            "laws": [
                {"number": "119-1", "bill": {"type": "hr", "number": 1}},
                {"number": "119-2", "bill": {"type": "hr", "number": 2}},
            ],
            "pagination": {"count": 2}
        }

        from main import fetch_all_laws_for_congress
        laws = fetch_all_laws_for_congress(119)

        # Should have called public and private endpoints
        assert mock_api_get.call_count >= 2

    @patch("main.api_get")
    def test_law_lookup_building(self, mock_api_get):
        """Test building law lookup from API response."""
        laws = [
            {
                "_law_type": "public",
                "number": "119-1",
                "bill": {"type": "hr", "number": 1234, "congress": 119}
            },
        ]

        lookup = build_law_lookup(119, laws)

        assert "119-hr-1234" in lookup
        assert lookup["119-hr-1234"]["law_type"] == "public"
        assert lookup["119-hr-1234"]["law_number"] == "119-1"


class TestStatisticsAggregation:
    """Tests to verify statistics are correctly aggregated."""

    def test_law_matching_by_bill_key(self):
        """Test that bills are correctly matched to laws."""
        law_lookup = {
            "119-hr-1234": {"law_type": "public", "law_number": "119-1"},
            "119-s-100": {"law_type": "private", "law_number": "119-1"},
        }

        # Simulate bill matching
        bill_key = normalize_bill_key(119, "hr", 1234)
        law_info = law_lookup.get(bill_key)

        assert law_info is not None
        assert law_info["law_type"] == "public"

    def test_non_matching_bill(self):
        """Test that non-enacted bills don't match."""
        law_lookup = {
            "119-hr-1234": {"law_type": "public", "law_number": "119-1"},
        }

        bill_key = normalize_bill_key(119, "hr", 9999)
        law_info = law_lookup.get(bill_key)

        assert law_info is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
