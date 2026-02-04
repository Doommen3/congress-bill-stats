"""
Comprehensive tests for Illinois General Assembly statistics module.
Tests cover: name normalization, name matching, XML parsing, database operations, and API endpoints.
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

from illinois_stats import (
    normalize_name,
    normalize_name_for_lookup,
    ILNameMatcher,
    parse_members_xml,
    parse_bill_xml,
    build_il_stats,
    PUBLIC_ACT_PATTERN,
    get_session_years,
    get_available_sessions,
    load_il_cache,
)


class TestNameNormalization:
    """Tests for name normalization functions."""

    def test_remove_rep_title(self):
        """Test removing Rep. title."""
        assert normalize_name("Rep. John Smith") == "john smith"

    def test_remove_sen_title(self):
        """Test removing Sen. title."""
        assert normalize_name("Sen. Jane Doe") == "jane doe"

    def test_remove_representative_title(self):
        """Test removing full Representative title."""
        assert normalize_name("Representative John Smith") == "john smith"

    def test_remove_senator_title(self):
        """Test removing full Senator title."""
        assert normalize_name("Senator Jane Doe") == "jane doe"

    def test_remove_suffix_jr(self):
        """Test removing Jr. suffix."""
        assert normalize_name("Rep. John Smith, Jr.") == "john smith"
        assert normalize_name("Rep. John Smith Jr.") == "john smith"

    def test_remove_suffix_ii(self):
        """Test removing II suffix."""
        assert normalize_name("Rep. Curtis J. Tarver, II") == "curtis j. tarver"

    def test_remove_suffix_iii(self):
        """Test removing III suffix."""
        assert normalize_name("Sen. Robert Smith III") == "robert smith"

    def test_handle_middle_name(self):
        """Test preserving middle name/initial."""
        assert normalize_name("Rep. John A. Smith") == "john a. smith"
        assert normalize_name("Rep. John Andrew Smith") == "john andrew smith"

    def test_normalize_whitespace(self):
        """Test normalizing extra whitespace."""
        assert normalize_name("Rep.  John   Smith") == "john smith"

    def test_empty_string(self):
        """Test empty string returns empty."""
        assert normalize_name("") == ""
        assert normalize_name(None) == ""

    def test_case_insensitive_title(self):
        """Test title removal is case insensitive."""
        assert normalize_name("REP. JOHN SMITH") == "john smith"
        assert normalize_name("rep. john smith") == "john smith"


class TestNameNormalizationForLookup:
    """Tests for simplified lookup key generation."""

    def test_removes_middle_name(self):
        """Test that middle name is removed for lookup."""
        assert normalize_name_for_lookup("Rep. John Andrew Smith") == "john smith"
        assert normalize_name_for_lookup("Rep. John A. Smith") == "john smith"

    def test_two_part_name(self):
        """Test two-part name stays the same."""
        assert normalize_name_for_lookup("Rep. John Smith") == "john smith"

    def test_single_name(self):
        """Test single name is returned."""
        assert normalize_name_for_lookup("Madonna") == "madonna"


class TestPublicActPattern:
    """Tests for Public Act detection regex."""

    def test_standard_format(self):
        """Test standard Public Act format with dots."""
        action = "Public Act . . . . . . . . . 103-0324"
        match = PUBLIC_ACT_PATTERN.search(action)
        assert match is not None
        assert match.group(1) == "103-0324"

    def test_compact_format(self):
        """Test compact Public Act format."""
        action = "Public Act 104-0001"
        match = PUBLIC_ACT_PATTERN.search(action)
        assert match is not None
        assert match.group(1) == "104-0001"

    def test_mixed_dots_spaces(self):
        """Test mixed dots and spaces."""
        action = "Public Act.. . . 103-0500"
        match = PUBLIC_ACT_PATTERN.search(action)
        assert match is not None
        assert match.group(1) == "103-0500"

    def test_no_public_act(self):
        """Test non-enacted action."""
        action = "Referred to Rules Committee"
        match = PUBLIC_ACT_PATTERN.search(action)
        assert match is None

    def test_session_sine_die(self):
        """Test session end doesn't match."""
        action = "Session Sine Die"
        match = PUBLIC_ACT_PATTERN.search(action)
        assert match is None

    def test_case_insensitive(self):
        """Test case insensitive matching."""
        action = "PUBLIC ACT 103-0001"
        match = PUBLIC_ACT_PATTERN.search(action)
        assert match is not None


class TestILNameMatcher:
    """Tests for sponsor-to-member matching."""

    @pytest.fixture
    def sample_members(self):
        """Create sample member list for testing."""
        return [
            {
                "member_id": "104-house-1",
                "name": "John A. Smith",
                "first_name": "John",
                "last_name": "Smith",
                "party": "D",
                "chamber": "house",
                "district": 1,
            },
            {
                "member_id": "104-senate-5",
                "name": "Jane B. Doe",
                "first_name": "Jane",
                "last_name": "Doe",
                "party": "R",
                "chamber": "senate",
                "district": 5,
            },
            {
                "member_id": "104-house-10",
                "name": "Robert Johnson",
                "first_name": "Robert",
                "last_name": "Johnson",
                "party": "D",
                "chamber": "house",
                "district": 10,
            },
            {
                "member_id": "104-senate-15",
                "name": "Michael Johnson",
                "first_name": "Michael",
                "last_name": "Johnson",
                "party": "R",
                "chamber": "senate",
                "district": 15,
            },
        ]

    def test_exact_match(self, sample_members):
        """Test exact name match."""
        matcher = ILNameMatcher(sample_members)
        result = matcher.match("Rep. John A. Smith", "house")
        assert result is not None
        assert result["member_id"] == "104-house-1"

    def test_match_without_middle_name(self, sample_members):
        """Test match when sponsor has no middle name."""
        matcher = ILNameMatcher(sample_members)
        result = matcher.match("Rep. John Smith", "house")
        assert result is not None
        assert result["member_id"] == "104-house-1"

    def test_match_senate_member(self, sample_members):
        """Test matching senate member."""
        matcher = ILNameMatcher(sample_members)
        result = matcher.match("Sen. Jane B. Doe", "senate")
        assert result is not None
        assert result["member_id"] == "104-senate-5"

    def test_no_match_returns_none(self, sample_members):
        """Test non-existent member returns None."""
        matcher = ILNameMatcher(sample_members)
        result = matcher.match("Rep. Unknown Person", "house")
        assert result is None

    def test_unmatched_tracking(self, sample_members):
        """Test that unmatched names are tracked."""
        matcher = ILNameMatcher(sample_members)
        matcher.match("Rep. Unknown Person", "house")
        assert len(matcher.unmatched) == 1
        assert matcher.unmatched[0]["name"] == "Rep. Unknown Person"

    def test_chamber_filter_disambiguation(self, sample_members):
        """Test chamber filter helps disambiguate same last name."""
        matcher = ILNameMatcher(sample_members)

        # Should match house Johnson when chamber is house
        result = matcher.match("Rep. Robert Johnson", "house")
        assert result is not None
        assert result["member_id"] == "104-house-10"

        # Should match senate Johnson when chamber is senate
        result = matcher.match("Sen. Michael Johnson", "senate")
        assert result is not None
        assert result["member_id"] == "104-senate-15"

    def test_empty_sponsor_name(self, sample_members):
        """Test empty sponsor name returns None."""
        matcher = ILNameMatcher(sample_members)
        result = matcher.match("", "house")
        assert result is None
        result = matcher.match(None, "house")
        assert result is None


class TestMembersXMLParsing:
    """Tests for parsing member XML files."""

    SAMPLE_MEMBER_XML = """<?xml version="1.0" encoding="utf-8"?>
    <Members>
      <Member>
        <Name>John A. Smith</Name>
        <FirstName>John</FirstName>
        <MiddleName>A.</MiddleName>
        <LastName>Smith</LastName>
        <Suffix></Suffix>
        <Party>D</Party>
        <District>1</District>
        <Title>Representative</Title>
      </Member>
      <Member>
        <Name>Jane Doe</Name>
        <FirstName>Jane</FirstName>
        <LastName>Doe</LastName>
        <Party>R</Party>
        <District>2</District>
        <Title>Representative</Title>
      </Member>
    </Members>
    """

    def test_parse_multiple_members(self):
        """Test parsing multiple members."""
        members = parse_members_xml(self.SAMPLE_MEMBER_XML, "house", 104)
        assert len(members) == 2

    def test_parse_member_fields(self):
        """Test all member fields are extracted."""
        members = parse_members_xml(self.SAMPLE_MEMBER_XML, "house", 104)
        member = members[0]

        assert member["name"] == "John A. Smith"
        assert member["first_name"] == "John"
        assert member["last_name"] == "Smith"
        assert member["party"] == "D"
        assert member["district"] == 1
        assert member["chamber"] == "house"
        assert member["ga_session"] == 104

    def test_member_id_generation(self):
        """Test member ID is correctly generated."""
        members = parse_members_xml(self.SAMPLE_MEMBER_XML, "house", 104)
        assert members[0]["member_id"] == "104-house-1"
        assert members[1]["member_id"] == "104-house-2"

    def test_invalid_xml(self):
        """Test invalid XML returns empty list."""
        members = parse_members_xml("not valid xml", "house", 104)
        assert members == []

    def test_empty_xml(self):
        """Test empty Members element."""
        members = parse_members_xml("<Members></Members>", "house", 104)
        assert members == []


class TestBillXMLParsing:
    """Tests for parsing bill status XML files."""

    SAMPLE_ENACTED_BILL_XML = """<?xml version="1.0" encoding="utf-8"?>
    <BillStatus>
      <ShortTitle>Test Bill</ShortTitle>
      <Synopsis>A test bill description</Synopsis>
      <PrimarySponsor>
        <Name>Rep. John A. Smith</Name>
      </PrimarySponsor>
      <Actions>
        <Action>
          <Date>01/15/2025</Date>
          <Description>Prefiled with Clerk by Rep. John A. Smith</Description>
        </Action>
        <Action>
          <Date>06/15/2025</Date>
          <Description>Public Act . . . . . . . . . 104-0001</Description>
        </Action>
      </Actions>
    </BillStatus>
    """

    SAMPLE_NON_ENACTED_BILL_XML = """<?xml version="1.0" encoding="utf-8"?>
    <BillStatus>
      <ShortTitle>Another Bill</ShortTitle>
      <PrimarySponsor>
        <Name>Rep. Jane Doe</Name>
      </PrimarySponsor>
      <Actions>
        <Action>
          <Date>01/15/2025</Date>
          <Description>Session Sine Die</Description>
        </Action>
      </Actions>
    </BillStatus>
    """

    def test_parse_enacted_bill(self):
        """Test parsing an enacted bill."""
        bill = parse_bill_xml(self.SAMPLE_ENACTED_BILL_XML, "10400HB0001.xml", 104)

        assert bill is not None
        assert bill["bill_type"] == "hb"
        assert bill["bill_number"] == 1
        assert bill["ga_session"] == 104
        assert bill["sponsor_name_raw"] == "John A. Smith"
        assert bill["primary_sponsor_name"] == "John A. Smith"
        assert bill["public_act_number"] == "104-0001"

    def test_parse_non_enacted_bill(self):
        """Test parsing a non-enacted bill."""
        bill = parse_bill_xml(self.SAMPLE_NON_ENACTED_BILL_XML, "10400HB0002.xml", 104)

        assert bill is not None
        assert bill["bill_number"] == 2
        assert bill["public_act_number"] is None

    def test_parse_senate_bill(self):
        """Test parsing Senate bill filename."""
        bill = parse_bill_xml(self.SAMPLE_NON_ENACTED_BILL_XML, "10400SB0100.xml", 104)

        assert bill is not None
        assert bill["bill_type"] == "sb"
        assert bill["bill_number"] == 100

    def test_invalid_filename(self):
        """Test invalid filename returns None."""
        bill = parse_bill_xml(self.SAMPLE_ENACTED_BILL_XML, "invalid.xml", 104)
        assert bill is None

    def test_invalid_xml(self):
        """Test invalid XML returns None."""
        bill = parse_bill_xml("not valid xml", "10400HB0001.xml", 104)
        assert bill is None

    def test_bill_id_generation(self):
        """Test bill ID is correctly generated."""
        bill = parse_bill_xml(self.SAMPLE_ENACTED_BILL_XML, "10400HB0001.xml", 104)
        assert bill["bill_id"] == "104-hb-1"


class TestActionSponsorParsing:
    """Tests for parsing sponsor roles from action lists."""

    SAMPLE_ACTION_SPONSOR_XML = """<?xml version="1.0" encoding="utf-8"?>
    <BillStatus>
      <shortdesc>GENERAL ANESTHESIA COVERAGE</shortdesc>
      <actions>
        <statusdate>1/3/2025</statusdate>
        <chamber>House</chamber>
        <action>Prefiled with Clerk by Rep. William E Hauter</action>
        <statusdate>1/14/2025</statusdate>
        <chamber>House</chamber>
        <action>Added Chief Co-Sponsor Rep. Diane Blair-Sherlock</action>
        <statusdate>1/22/2025</statusdate>
        <chamber>House</chamber>
        <action>Added Chief Co-Sponsor Rep. Tracy Katz Muhl</action>
        <statusdate>1/28/2025</statusdate>
        <chamber>House</chamber>
        <action>Added Co-Sponsor Rep. Harry Benton</action>
        <statusdate>1/30/2025</statusdate>
        <chamber>House</chamber>
        <action>Removed Co-Sponsor Rep. Harry Benton</action>
        <statusdate>2/1/2025</statusdate>
        <chamber>House</chamber>
        <action>Added Co-Sponsors Reps. Amy Briel and Rick Ryan</action>
        <statusdate>2/5/2025</statusdate>
        <chamber>House</chamber>
        <action>Added Co-Sponsor Rep. Michael J. Coffey, Jr.</action>
      </actions>
    </BillStatus>
    """

    def test_parse_action_roles(self):
        bill = parse_bill_xml(self.SAMPLE_ACTION_SPONSOR_XML, "10400HB1141.xml", 104)
        assert bill["primary_sponsor_name"] == "William E Hauter"
        assert bill["chief_co_sponsors"] == ["Diane Blair-Sherlock", "Tracy Katz Muhl"]
        assert "Harry Benton" not in bill["co_sponsors"]
        assert "Amy Briel" in bill["co_sponsors"]
        assert "Rick Ryan" in bill["co_sponsors"]
        assert "Michael J. Coffey Jr." in bill["co_sponsors"]

    SAMPLE_SPONSOR_CHANGED_XML = """<?xml version="1.0" encoding="utf-8"?>
    <BillStatus>
      <shortdesc>TEST BILL WITH SPONSOR CHANGE</shortdesc>
      <actions>
        <statusdate>12/17/2024</statusdate>
        <chamber>House</chamber>
        <action>Prefiled with Clerk by Rep. Emanuel Chris Welch</action>
        <statusdate>1/8/2025</statusdate>
        <chamber>House</chamber>
        <action>First Reading</action>
        <statusdate>3/18/2025</statusdate>
        <chamber>House</chamber>
        <action>Chief Sponsor Changed to Rep. Amy Briel</action>
        <statusdate>8/15/2025</statusdate>
        <chamber>House</chamber>
        <action>Public Act . . . . . . . . . 104-0165</action>
      </actions>
    </BillStatus>
    """

    def test_sponsor_change_overrides_original_filer(self):
        """Test that Chief Sponsor Changed action updates the primary sponsor."""
        bill = parse_bill_xml(self.SAMPLE_SPONSOR_CHANGED_XML, "10400HB0871.xml", 104)
        # Should be Amy Briel (the changed sponsor), not Emanuel Chris Welch (original filer)
        assert bill["primary_sponsor_name"] == "Amy Briel"
        assert bill["public_act_number"] == "104-0165"


class TestSessionHelpers:
    """Tests for session utility functions."""

    def test_get_session_years(self):
        """Test session year mapping."""
        assert get_session_years(104) == "2025-2026"
        assert get_session_years(103) == "2023-2024"
        assert get_session_years(102) == "2021-2022"

    def test_unknown_session_years(self):
        """Test unknown session returns session number."""
        assert get_session_years(50) == "50"

    def test_available_sessions(self):
        """Test available sessions list."""
        sessions = get_available_sessions()
        assert len(sessions) >= 2
        assert sessions[0]["session"] == 104
        assert sessions[0]["current"] is True


class TestILDatabase:
    """Tests for Illinois database operations."""

    @pytest.fixture
    def temp_db(self, tmp_path):
        """Create a temporary database for testing."""
        db_path = str(tmp_path / "test.db")
        os.environ["DATABASE_PATH"] = db_path

        # Re-import to use the new path
        import importlib
        import illinois_database
        importlib.reload(illinois_database)

        yield illinois_database

        # Cleanup
        if os.path.exists(db_path):
            os.remove(db_path)

    def test_init_il_database(self, temp_db):
        """Test Illinois database initialization."""
        conn = sqlite3.connect(os.environ["DATABASE_PATH"])
        cursor = conn.cursor()

        # Check tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        assert "il_legislators" in tables
        assert "il_bills" in tables
        assert "il_laws" in tables
        assert "il_cache_metadata" in tables

        conn.close()

    def test_save_and_load_il_legislators(self, temp_db):
        """Test saving and loading IL legislators."""
        legislators = [
            {
                "member_id": "104-house-1",
                "ga_session": 104,
                "chamber": "house",
                "district": 1,
                "name": "John Smith",
                "first_name": "John",
                "last_name": "Smith",
                "party": "D",
                "title": "Representative",
            }
        ]
        temp_db.save_il_legislators_batch(104, legislators)

        # Verify data was saved
        conn = sqlite3.connect(os.environ["DATABASE_PATH"])
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM il_legislators WHERE member_id = ?", ("104-house-1",))
        row = cursor.fetchone()
        conn.close()

        assert row is not None

    def test_save_and_load_il_bills(self, temp_db):
        """Test saving and loading IL bills."""
        bills = [
            {
                "bill_type": "hb",
                "bill_number": 1,
                "sponsor_member_id": "104-house-1",
                "sponsor_name_raw": "Rep. John Smith",
                "title": "Test Bill",
                "public_act_number": "104-0001",
            }
        ]
        temp_db.save_il_bills_batch(104, bills)

        # Verify data was saved
        conn = sqlite3.connect(os.environ["DATABASE_PATH"])
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM il_bills WHERE bill_id = ?", ("104-hb-1",))
        row = cursor.fetchone()
        conn.close()

        assert row is not None

    def test_save_and_load_il_stats_cache(self, temp_db):
        """Test saving and loading IL stats cache."""
        stats = {
            "ga_session": 104,
            "generated_at": 1234567890,
            "rows": [{"memberId": "104-house-1", "sponsored_total": 10}],
            "summary": {"total_bills": 100, "total_laws": 5},
        }
        temp_db.save_il_stats_cache(104, stats)

        loaded = temp_db.load_il_stats_cache(104)
        assert loaded is not None
        assert loaded["ga_session"] == 104
        assert len(loaded["rows"]) == 1

    def test_il_cache_miss(self, temp_db):
        """Test that cache miss returns None."""
        result = temp_db.load_il_stats_cache(999)
        assert result is None

    def test_clear_il_session_data(self, temp_db):
        """Test clearing session data."""
        # First save some data
        legislators = [{"member_id": "104-house-1", "ga_session": 104, "chamber": "house",
                       "district": 1, "name": "Test", "party": "D"}]
        temp_db.save_il_legislators_batch(104, legislators)

        # Clear it
        temp_db.clear_il_session_data(104)

        # Verify it's gone
        conn = sqlite3.connect(os.environ["DATABASE_PATH"])
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM il_legislators WHERE ga_session = ?", (104,))
        count = cursor.fetchone()[0]
        conn.close()

        assert count == 0


class TestILRemoteCache:
    """Tests for remote cache loading."""

    class DummyResp:
        def __init__(self, status_code, data):
            self.status_code = status_code
            self._data = data

        def json(self):
            return self._data

    @patch("illinois_stats.il_db.load_il_stats_cache")
    @patch("illinois_stats.requests.get")
    def test_load_il_cache_remote_preferred(self, mock_get, mock_db, monkeypatch, tmp_path):
        """Remote cache should be used when configured."""
        monkeypatch.setenv("REMOTE_CACHE_BASE_URL", "https://example.com/cache")
        monkeypatch.setattr("illinois_stats.IL_CACHE_DIR", str(tmp_path))
        mock_get.return_value = self.DummyResp(200, {"ga_session": 104, "rows": []})

        result = load_il_cache(104)

        assert result is not None
        assert result["ga_session"] == 104
        mock_db.assert_not_called()

    @patch("illinois_stats.il_db.load_il_stats_cache")
    @patch("illinois_stats.requests.get")
    def test_load_il_cache_remote_fallback_to_file(self, mock_get, mock_db, monkeypatch, tmp_path):
        """Fallback to local file when remote is missing."""
        monkeypatch.setenv("REMOTE_CACHE_BASE_URL", "https://example.com/cache")
        monkeypatch.setattr("illinois_stats.IL_CACHE_DIR", str(tmp_path))
        mock_get.return_value = self.DummyResp(404, {})

        # Write local cache file
        fp = tmp_path / "il_stats_104.json"
        fp.write_text(json.dumps({"ga_session": 104, "rows": [{"memberId": "X"}]}), encoding="utf-8")

        result = load_il_cache(104)

        assert result is not None
        assert result["ga_session"] == 104
        mock_db.assert_not_called()


class TestILAPIEndpoints:
    """Tests for Illinois API endpoints."""

    @pytest.fixture
    def client(self):
        """Create a test client."""
        from fastapi.testclient import TestClient
        from main import app
        return TestClient(app)

    def test_il_sessions_endpoint(self, client):
        """Test the IL sessions endpoint."""
        response = client.get("/api/il-sessions")
        assert response.status_code == 200
        data = response.json()
        assert "sessions" in data
        assert len(data["sessions"]) >= 2

    def test_il_refresh_status_endpoint(self, client):
        """Test the IL refresh status endpoint."""
        response = client.get("/api/il-refresh-status?session=104")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data

    @patch("illinois_stats.load_il_cache")
    def test_il_stats_endpoint_with_cache(self, mock_cache, client):
        """Test IL stats endpoint returns cached data."""
        mock_cache.return_value = {
            "ga_session": 104,
            "years": "2025-2026",
            "generated_at": 1234567890,
            "rows": [],
            "summary": {},
        }
        response = client.get("/api/il-stats?session=104")
        assert response.status_code == 200
        data = response.json()
        assert data["ga_session"] == 104

    @patch("illinois_stats.load_il_cache")
    @patch("illinois_stats.build_il_stats")
    @patch("illinois_stats.save_il_cache")
    def test_il_stats_endpoint_refresh(self, mock_save, mock_build, mock_cache, client, monkeypatch):
        """Test IL stats endpoint with refresh."""
        monkeypatch.setenv("ADMIN_IP_ALLOWLIST", "1.2.3.4")
        mock_cache.return_value = None
        mock_build.return_value = {
            "ga_session": 104,
            "years": "2025-2026",
            "generated_at": 1234567890,
            "rows": [],
            "summary": {"total_legislators": 0, "total_bills": 0, "total_laws": 0},
        }

        response = client.get(
            "/api/il-stats?session=104&refresh=true",
            headers={"x-forwarded-for": "1.2.3.4"},
        )
        assert response.status_code == 200
        mock_build.assert_called_once()

    @patch("illinois_database.get_il_network_data")
    def test_il_network_endpoint_defaults_to_network_view(self, mock_get_network, client):
        """Network endpoint should default to the force-directed graph payload."""
        mock_get_network.return_value = {
            "ga_session": 104,
            "nodes": [],
            "links": [],
            "min_connections": 3,
            "view": "network",
        }

        response = client.get("/api/il-network?session=104")
        assert response.status_code == 200

        mock_get_network.assert_called_once_with(104, min_connections=3, view="network")
        payload = response.json()
        assert payload["view"] == "network"

    @patch("illinois_database.get_il_network_data")
    def test_il_network_endpoint_edge_bundling_view(self, mock_get_network, client):
        """Network endpoint should forward edge-bundling view selection."""
        mock_get_network.return_value = {
            "ga_session": 104,
            "nodes": [],
            "links": [],
            "min_connections": 2,
            "view": "edge_bundling",
            "hierarchy": {"name": "Illinois GA", "children": []},
        }

        response = client.get("/api/il-network?session=104&min_connections=2&view=edge_bundling")
        assert response.status_code == 200

        mock_get_network.assert_called_once_with(104, min_connections=2, view="edge_bundling")
        payload = response.json()
        assert payload["view"] == "edge_bundling"
        assert "hierarchy" in payload

    def test_il_network_endpoint_rejects_invalid_view(self, client):
        """Invalid view should fail validation at the API layer."""
        response = client.get("/api/il-network?session=104&view=invalid")
        assert response.status_code == 422


class TestILIncrementalMerge:
    @patch("illinois_stats.il_db.update_il_bill")
    @patch("illinois_stats.il_db.get_pending_bills_for_update")
    @patch("illinois_stats.il_db.save_il_legislators_batch")
    @patch("illinois_stats.il_db.save_il_bills_batch")
    @patch("illinois_stats.il_db.save_il_laws_batch")
    @patch("illinois_stats.il_db.save_il_stats_cache")
    @patch("illinois_stats.il_db.get_all_bills_for_session")
    @patch("illinois_stats.il_db.get_existing_bill_filenames")
    @patch("illinois_stats.ILDataFetcher.fetch_bill")
    @patch("illinois_stats.ILDataFetcher.fetch_bill_list")
    @patch("illinois_stats.ILDataFetcher.fetch_members")
    def test_incremental_merge_dedupes_overlapping_bills(
        self,
        mock_fetch_members,
        mock_fetch_bill_list,
        mock_fetch_bill,
        mock_existing_files,
        mock_existing_bills,
        mock_save_cache,
        mock_save_laws,
        mock_save_bills,
        mock_save_legs,
        mock_get_pending,
        mock_update_bill,
    ):
        members = [
            {
                "member_id": "104-house-7",
                "name": 'Emanuel "Chris" Welch',
                "first_name": "Emanuel",
                "last_name": "Welch",
                "party": "D",
                "district": 7,
                "title": "Rep.",
                "chamber": "house",
            }
        ]
        mock_fetch_members.return_value = (members, [])
        mock_fetch_bill_list.return_value = ["10400HB0001.xml"]
        mock_existing_files.return_value = set()
        mock_get_pending.return_value = []  # No pending bills to check

        existing_bill = {
            "bill_id": "104-hb-1",
            "bill_type": "hb",
            "bill_number": 1,
            "primary_sponsor_name": 'Emanuel "Chris" Welch',
            "sponsor_name_raw": "Rep. Emanuel Chris Welch",
            "chief_co_sponsors": "[]",
            "co_sponsors": "[]",
            "public_act_number": None,
        }
        new_bill = dict(existing_bill)
        new_bill["latest_action_text"] = "Updated action"

        mock_existing_bills.return_value = [existing_bill]
        mock_fetch_bill.return_value = new_bill

        stats = build_il_stats(104, incremental=True)
        rows = {row["memberId"]: row for row in stats["rows"]}
        assert rows["104-house-7"]["primary_sponsor_total"] == 1
        assert stats["summary"]["total_bills"] == 1

    @patch("illinois_stats._rebuild_stats_from_db")
    @patch("illinois_stats.il_db.get_pending_bills_for_update")
    @patch("illinois_stats.ILDataFetcher.fetch_bill")
    @patch("illinois_stats.il_db.get_existing_bill_filenames")
    @patch("illinois_stats.ILDataFetcher.fetch_bill_list")
    @patch("illinois_stats.ILDataFetcher.fetch_members")
    def test_incremental_filename_filter_is_case_insensitive(
        self,
        mock_fetch_members,
        mock_fetch_bill_list,
        mock_existing_files,
        mock_fetch_bill,
        mock_get_pending,
        mock_rebuild,
    ):
        members = [
            {
                "member_id": "104-house-7",
                "name": 'Emanuel "Chris" Welch',
                "first_name": "Emanuel",
                "last_name": "Welch",
                "party": "D",
                "district": 7,
                "title": "Rep.",
                "chamber": "house",
            }
        ]
        mock_fetch_members.return_value = (members, [])
        mock_fetch_bill_list.return_value = ["10400HB0001.xml"]
        mock_existing_files.return_value = {"10400HB0001.XML"}
        mock_get_pending.return_value = []  # No pending bills to check
        mock_rebuild.return_value = {"ga_session": 104, "rows": [], "summary": {"total_bills": 1, "total_laws": 0}}

        stats = build_il_stats(104, incremental=True)
        assert stats["ga_session"] == 104
        mock_fetch_bill.assert_not_called()
        mock_rebuild.assert_called_once()


class TestBillStatusUpdateLogic:
    """Tests for re-fetching bills that may have become public acts."""

    @pytest.fixture
    def temp_db(self, tmp_path):
        """Create a temporary database for testing."""
        db_path = str(tmp_path / "test.db")
        os.environ["DATABASE_PATH"] = db_path

        import importlib
        import illinois_database
        importlib.reload(illinois_database)

        yield illinois_database

        if os.path.exists(db_path):
            os.remove(db_path)

    def test_get_pending_bills_for_update(self, temp_db):
        """Test that get_pending_bills_for_update returns bills without public_act_number."""
        # Save some bills - some with public_act_number, some without
        bills = [
            {
                "bill_id": "104-hb-1",
                "bill_type": "hb",
                "bill_number": 1,
                "sponsor_name_raw": "Rep. John Smith",
                "public_act_number": "104-0001",  # Already enacted
                "latest_action_date": "06/15/2025",
            },
            {
                "bill_id": "104-hb-2",
                "bill_type": "hb",
                "bill_number": 2,
                "sponsor_name_raw": "Rep. Jane Doe",
                "public_act_number": None,  # Not enacted - should be returned
                "latest_action_date": "03/10/2025",
            },
            {
                "bill_id": "104-hb-3",
                "bill_type": "hb",
                "bill_number": 3,
                "sponsor_name_raw": "Rep. Bob Wilson",
                "public_act_number": None,  # Not enacted - should be returned
                "latest_action_date": "04/20/2025",
            },
        ]
        temp_db.save_il_bills_batch(104, bills)

        # Get pending bills
        pending = temp_db.get_pending_bills_for_update(104)

        assert len(pending) == 2
        bill_ids = {b["bill_id"] for b in pending}
        assert "104-hb-2" in bill_ids
        assert "104-hb-3" in bill_ids
        assert "104-hb-1" not in bill_ids  # Already has public_act_number

    def test_get_pending_bills_includes_action_date(self, temp_db):
        """Test that pending bills include latest_action_date for comparison."""
        bills = [
            {
                "bill_id": "104-hb-1",
                "bill_type": "hb",
                "bill_number": 1,
                "sponsor_name_raw": "Rep. John Smith",
                "public_act_number": None,
                "latest_action_date": "03/10/2025",
            },
        ]
        temp_db.save_il_bills_batch(104, bills)

        pending = temp_db.get_pending_bills_for_update(104)

        assert len(pending) == 1
        assert pending[0]["latest_action_date"] == "03/10/2025"
        assert pending[0]["bill_type"] == "hb"
        assert pending[0]["bill_number"] == 1

    def test_update_il_bill_sets_public_act(self, temp_db):
        """Test that update_il_bill correctly updates a bill's public_act_number."""
        # Save initial bill without public_act
        bills = [
            {
                "bill_id": "104-hb-1",
                "bill_type": "hb",
                "bill_number": 1,
                "sponsor_name_raw": "Rep. John Smith",
                "public_act_number": None,
                "latest_action_date": "03/10/2025",
            },
        ]
        temp_db.save_il_bills_batch(104, bills)

        # Update the bill with new data
        temp_db.update_il_bill("104-hb-1", {
            "public_act_number": "104-0050",
            "latest_action_date": "06/15/2025",
            "latest_action_text": "Public Act . . . 104-0050",
        })

        # Verify update
        all_bills = temp_db.get_all_bills_for_session(104)
        assert len(all_bills) == 1
        assert all_bills[0]["public_act_number"] == "104-0050"
        assert all_bills[0]["latest_action_date"] == "06/15/2025"

    @patch("illinois_stats.il_db.update_il_bill")
    @patch("illinois_stats.il_db.get_pending_bills_for_update")
    @patch("illinois_stats.il_db.save_il_legislators_batch")
    @patch("illinois_stats.il_db.save_il_bills_batch")
    @patch("illinois_stats.il_db.save_il_laws_batch")
    @patch("illinois_stats.il_db.save_il_stats_cache")
    @patch("illinois_stats.il_db.get_all_bills_for_session")
    @patch("illinois_stats.il_db.get_existing_bill_filenames")
    @patch("illinois_stats.ILDataFetcher.fetch_bill")
    @patch("illinois_stats.ILDataFetcher.fetch_bill_list")
    @patch("illinois_stats.ILDataFetcher.fetch_members")
    def test_incremental_refetches_pending_bills_with_changed_date(
        self,
        mock_fetch_members,
        mock_fetch_bill_list,
        mock_fetch_bill,
        mock_existing_files,
        mock_existing_bills,
        mock_save_cache,
        mock_save_laws,
        mock_save_bills,
        mock_save_legs,
        mock_get_pending,
        mock_update_bill,
    ):
        """Test that incremental mode re-fetches pending bills when action date changed."""
        members = [
            {
                "member_id": "104-house-1",
                "name": "John Smith",
                "first_name": "John",
                "last_name": "Smith",
                "party": "D",
                "district": 1,
                "chamber": "house",
            }
        ]
        mock_fetch_members.return_value = (members, [])
        mock_fetch_bill_list.return_value = ["10400HB0001.xml"]
        mock_existing_files.return_value = {"10400hb0001.xml"}  # Bill already exists

        # Existing bill without public_act_number
        existing_bill = {
            "bill_id": "104-hb-1",
            "bill_type": "hb",
            "bill_number": 1,
            "primary_sponsor_name": "John Smith",
            "sponsor_name_raw": "Rep. John Smith",
            "chief_co_sponsors": "[]",
            "co_sponsors": "[]",
            "public_act_number": None,
            "latest_action_date": "03/10/2025",  # Old date
        }
        mock_existing_bills.return_value = [existing_bill]
        mock_get_pending.return_value = [existing_bill]

        # Updated bill from server (now enacted, with new date)
        updated_bill = {
            "bill_id": "104-hb-1",
            "bill_type": "hb",
            "bill_number": 1,
            "primary_sponsor_name": "John Smith",
            "sponsor_name_raw": "Rep. John Smith",
            "chief_co_sponsors": [],
            "co_sponsors": [],
            "public_act_number": "104-0001",
            "latest_action_date": "06/15/2025",  # New date - triggers refetch
        }
        mock_fetch_bill.return_value = updated_bill

        stats = build_il_stats(104, incremental=True)

        # Should have re-fetched the bill because date changed
        # Note: filename is lowercased during processing
        mock_fetch_bill.assert_called_once_with("10400hb0001.xml")
        # Should have updated the bill in DB
        mock_update_bill.assert_called_once()

        # Stats should reflect the enacted bill
        rows = {row["memberId"]: row for row in stats["rows"]}
        assert rows["104-house-1"]["enacted_total"] == 1

    @patch("illinois_stats.il_db.update_il_bill")
    @patch("illinois_stats.il_db.get_pending_bills_for_update")
    @patch("illinois_stats.il_db.save_il_legislators_batch")
    @patch("illinois_stats.il_db.save_il_bills_batch")
    @patch("illinois_stats.il_db.save_il_laws_batch")
    @patch("illinois_stats.il_db.save_il_stats_cache")
    @patch("illinois_stats.il_db.get_all_bills_for_session")
    @patch("illinois_stats.il_db.get_existing_bill_filenames")
    @patch("illinois_stats.ILDataFetcher.fetch_bill")
    @patch("illinois_stats.ILDataFetcher.fetch_bill_list")
    @patch("illinois_stats.ILDataFetcher.fetch_members")
    def test_incremental_skips_pending_bills_with_same_date(
        self,
        mock_fetch_members,
        mock_fetch_bill_list,
        mock_fetch_bill,
        mock_existing_files,
        mock_existing_bills,
        mock_save_cache,
        mock_save_laws,
        mock_save_bills,
        mock_save_legs,
        mock_get_pending,
        mock_update_bill,
    ):
        """Test that incremental mode skips pending bills when action date unchanged."""
        members = [
            {
                "member_id": "104-house-1",
                "name": "John Smith",
                "first_name": "John",
                "last_name": "Smith",
                "party": "D",
                "district": 1,
                "chamber": "house",
            }
        ]
        mock_fetch_members.return_value = (members, [])
        mock_fetch_bill_list.return_value = ["10400HB0001.xml"]
        mock_existing_files.return_value = {"10400hb0001.xml"}

        # Existing bill without public_act_number
        existing_bill = {
            "bill_id": "104-hb-1",
            "bill_type": "hb",
            "bill_number": 1,
            "primary_sponsor_name": "John Smith",
            "sponsor_name_raw": "Rep. John Smith",
            "chief_co_sponsors": "[]",
            "co_sponsors": "[]",
            "public_act_number": None,
            "latest_action_date": "03/10/2025",
        }
        mock_existing_bills.return_value = [existing_bill]
        mock_get_pending.return_value = [existing_bill]

        # Server returns same date - no update needed
        same_bill = dict(existing_bill)
        same_bill["chief_co_sponsors"] = []
        same_bill["co_sponsors"] = []
        mock_fetch_bill.return_value = same_bill

        stats = build_il_stats(104, incremental=True)

        # Should have fetched to check the date
        mock_fetch_bill.assert_called_once()
        # But should NOT have called update since date is the same
        mock_update_bill.assert_not_called()


class TestIntegration:
    """Integration tests with mocked HTTP."""

    @patch("illinois_stats.il_fetch_xml")
    @patch("illinois_stats.il_fetch_directory_listing")
    def test_full_flow_mocked(self, mock_dir, mock_xml):
        """Test complete data flow with mocked ILGA responses."""
        # Mock member XML
        member_xml = """<?xml version="1.0"?>
        <Members>
          <Member>
            <Name>John Smith</Name>
            <FirstName>John</FirstName>
            <LastName>Smith</LastName>
            <Party>D</Party>
            <District>1</District>
          </Member>
        </Members>
        """

        # Mock bill XML
        bill_xml = """<?xml version="1.0"?>
        <BillStatus>
          <PrimarySponsor><Name>Rep. John Smith</Name></PrimarySponsor>
          <Actions>
            <Action><Description>Public Act 104-0001</Description></Action>
          </Actions>
        </BillStatus>
        """

        mock_dir.return_value = ["10400HB0001.xml"]
        mock_xml.side_effect = [member_xml, member_xml, bill_xml]

        from illinois_stats import ILDataFetcher, ILNameMatcher

        # Test member fetching
        fetcher = ILDataFetcher(104)
        house, senate = fetcher.fetch_members()

        assert len(house) == 1
        assert house[0]["name"] == "John Smith"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
