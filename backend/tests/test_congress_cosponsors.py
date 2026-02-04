"""
Tests for Congress cosponsor extraction and aggregation.
"""
import os
import sys
from unittest.mock import patch

import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import _extract_cosponsors, _normalize_cosponsor_item, build_stats


class TestExtractCosponsors:
    def test_extract_cosponsors_list(self):
        resp = {"cosponsors": [{"bioguideId": "A1"}]}
        items = _extract_cosponsors(resp)
        assert len(items) == 1

    def test_extract_cosponsors_data_list(self):
        resp = {"data": {"cosponsors": [{"bioguideId": "A1"}]}}
        items = _extract_cosponsors(resp)
        assert len(items) == 1

    def test_extract_cosponsors_item_shape(self):
        resp = {"cosponsors": {"item": [{"bioguideId": "A1"}]}}
        items = _extract_cosponsors(resp)
        assert len(items) == 1

    def test_normalize_cosponsor_flags(self):
        item = {
            "bioguideId": "A1",
            "fullName": "Rep. Alpha",
            "party": "D",
            "state": "NY",
            "isOriginalCosponsor": True,
            "withdrawnDate": "",
        }
        norm = _normalize_cosponsor_item(item)
        assert norm["bioguideId"] == "A1"
        assert norm["is_original"] is True
        assert norm["withdrawn"] is False


class TestCosponsorAggregation:
    @patch("main.fetch_all_laws_for_congress")
    @patch("main.fetch_cosponsors_for_bill")
    @patch("main.db.save_legislators_batch")
    @patch("main.db.save_bills_batch")
    @patch("main.db.save_laws_batch")
    @patch("main.db.save_stats_cache")
    @patch("main.db.save_bill_cosponsors_batch")
    @patch("main.db.mark_bill_cosponsors_refreshed")
    @patch("main.db.clear_bill_cosponsors_for_congress")
    def test_build_stats_includes_cosponsors(
        self,
        mock_clear_cosponsors,
        mock_mark_cosponsors,
        mock_save_cosponsors,
        mock_save_stats,
        mock_save_laws,
        mock_save_bills,
        mock_save_legs,
        mock_fetch_cosponsors,
        mock_fetch_laws,
    ):
        mock_fetch_laws.return_value = []

        bills = [
            {
                "type": "hr",
                "number": 1,
                "originChamber": "House",
                "sponsor": {
                    "bioguideId": "A1",
                    "fullName": "Rep. Alpha",
                    "party": "D",
                    "state": "NY",
                    "chamber": "House",
                },
            },
            {
                "type": "hr",
                "number": 2,
                "originChamber": "House",
                "sponsor": {
                    "bioguideId": "B2",
                    "fullName": "Rep. Beta",
                    "party": "R",
                    "state": "TX",
                    "chamber": "House",
                },
            },
        ]

        cosponsor_map = {
            "119-hr-1": [
                {
                    "bioguideId": "C3",
                    "fullName": "Rep. Gamma",
                    "party": "D",
                    "state": "CA",
                    "chamber": "House",
                    "is_original": True,
                    "withdrawn": False,
                },
                {
                    "bioguideId": "D4",
                    "fullName": "Rep. Delta",
                    "party": "R",
                    "state": "FL",
                    "chamber": "House",
                    "is_original": False,
                    "withdrawn": True,
                },
            ],
            "119-hr-2": [
                {
                    "bioguideId": "C3",
                    "fullName": "Rep. Gamma",
                    "party": "D",
                    "state": "CA",
                    "chamber": "House",
                    "is_original": False,
                    "withdrawn": False,
                }
            ],
        }

        def cosponsor_side_effect(congress, bill, api_key=None):
            key = f"{congress}-{bill.get('type')}-{bill.get('number')}"
            return cosponsor_map.get(key, [])

        mock_fetch_cosponsors.side_effect = cosponsor_side_effect

        with patch("main.fetch_all_bills_for_congress", return_value=bills):
            stats = build_stats(119)

        rows = {r["bioguideId"]: r for r in stats["rows"]}

        assert rows["A1"]["primary_sponsor_total"] == 1
        assert rows["A1"]["cosponsor_total"] == 0
        assert rows["B2"]["primary_sponsor_total"] == 1

        assert rows["C3"]["cosponsor_total"] == 2
        assert rows["C3"]["original_cosponsor_total"] == 1

        assert "D4" not in rows or rows["D4"]["cosponsor_total"] == 0

    @patch("main.db.get_stats_from_db")
    @patch("main.db.save_legislators_batch")
    @patch("main.db.save_bills_batch")
    @patch("main.db.save_laws_batch")
    @patch("main.db.save_stats_cache")
    @patch("main.fetch_cosponsors_for_bill")
    @patch("main.fetch_all_laws_for_congress")
    def test_build_stats_skip_cosponsors(
        self,
        mock_fetch_laws,
        mock_fetch_cosponsors,
        mock_save_stats,
        mock_save_laws,
        mock_save_bills,
        mock_save_legs,
        mock_get_stats,
    ):
        mock_fetch_laws.return_value = []
        mock_get_stats.return_value = {"congress": 119, "rows": [], "summary": {}}
        mock_fetch_cosponsors.return_value = []

        bills = [
            {
                "type": "hr",
                "number": 1,
                "originChamber": "House",
                "sponsor": {
                    "bioguideId": "A1",
                    "fullName": "Rep. Alpha",
                    "party": "D",
                    "state": "NY",
                    "chamber": "House",
                },
            }
        ]

        with patch("main.fetch_all_bills_for_congress", return_value=bills):
            stats = build_stats(119, cosponsor_mode="skip")

        assert stats["congress"] == 119
        mock_fetch_cosponsors.assert_not_called()

    @patch("main.db.get_stats_from_db")
    @patch("main.db.get_bill_cosponsor_refresh_map")
    @patch("main.db.mark_bill_cosponsors_refreshed")
    @patch("main.db.delete_bill_cosponsors_for_bills")
    @patch("main.db.save_legislators_batch")
    @patch("main.db.save_bills_batch")
    @patch("main.db.save_laws_batch")
    @patch("main.db.save_stats_cache")
    @patch("main.fetch_cosponsors_for_bill")
    @patch("main.fetch_all_laws_for_congress")
    def test_build_stats_incremental_cosponsors(
        self,
        mock_fetch_laws,
        mock_fetch_cosponsors,
        mock_save_stats,
        mock_save_laws,
        mock_save_bills,
        mock_save_legs,
        mock_delete_cosponsors,
        mock_mark_cosponsors,
        mock_refresh_map,
        mock_get_stats,
    ):
        mock_fetch_laws.return_value = []
        mock_get_stats.return_value = {"congress": 119, "rows": [], "summary": {}}
        mock_fetch_cosponsors.return_value = []
        mock_refresh_map.return_value = {
            "119-hr-1": {"cosponsors_last_update_date": "2024-01-01", "cosponsors_updated_at": 123},
            "119-hr-2": {"cosponsors_last_update_date": "2024-01-01", "cosponsors_updated_at": 123},
        }

        bills = [
            {
                "type": "hr",
                "number": 1,
                "updateDate": "2024-01-01",
                "originChamber": "House",
                "sponsor": {
                    "bioguideId": "A1",
                    "fullName": "Rep. Alpha",
                    "party": "D",
                    "state": "NY",
                    "chamber": "House",
                },
            },
            {
                "type": "hr",
                "number": 2,
                "updateDate": "2024-02-01",
                "originChamber": "House",
                "sponsor": {
                    "bioguideId": "B2",
                    "fullName": "Rep. Beta",
                    "party": "R",
                    "state": "TX",
                    "chamber": "House",
                },
            },
        ]

        with patch("main.fetch_all_bills_for_congress", return_value=bills):
            stats = build_stats(119, cosponsor_mode="incremental")

        assert stats["congress"] == 119
        assert mock_fetch_cosponsors.call_count == 1

    @patch("main._load_bulk_bill_status_map")
    @patch("main.fetch_all_laws_for_congress")
    @patch("main.fetch_cosponsors_for_bill")
    @patch("main.db.save_legislators_batch")
    @patch("main.db.save_bills_batch")
    @patch("main.db.save_laws_batch")
    @patch("main.db.save_stats_cache")
    @patch("main.db.save_bill_cosponsors_batch")
    @patch("main.db.mark_bill_cosponsors_refreshed")
    @patch("main.db.clear_bill_cosponsors_for_congress")
    def test_build_stats_bulk_source_skips_api_cosponsor_calls(
        self,
        mock_clear_cosponsors,
        mock_mark_cosponsors,
        mock_save_cosponsors,
        mock_save_stats,
        mock_save_laws,
        mock_save_bills,
        mock_save_legs,
        mock_fetch_cosponsors,
        mock_fetch_laws,
        mock_load_bulk,
    ):
        mock_fetch_laws.return_value = []
        mock_load_bulk.return_value = {
            "119-hr-1": {
                "sponsor": {"bioguideId": "A1", "fullName": "Rep. Alpha", "party": "D", "state": "NY", "chamber": "House"},
                "cosponsors": [
                    {
                        "bioguideId": "C3",
                        "fullName": "Rep. Gamma",
                        "party": "D",
                        "state": "CA",
                        "chamber": "House",
                        "is_original": True,
                        "withdrawn": False,
                    }
                ],
            }
        }

        bills = [
            {
                "type": "hr",
                "number": 1,
                "originChamber": "House",
                "sponsor": {
                    "bioguideId": "A1",
                    "fullName": "Rep. Alpha",
                    "party": "D",
                    "state": "NY",
                    "chamber": "House",
                },
            }
        ]

        with patch("main.fetch_all_bills_for_congress", return_value=bills):
            stats = build_stats(119, cosponsor_source="bulk")

        rows = {r["bioguideId"]: r for r in stats["rows"]}
        assert rows["C3"]["cosponsor_total"] == 1
        mock_fetch_cosponsors.assert_not_called()

    @patch("main._load_bulk_bill_status_map")
    @patch("main.fetch_all_laws_for_congress")
    @patch("main.fetch_cosponsors_for_bill")
    @patch("main.db.save_legislators_batch")
    @patch("main.db.save_bills_batch")
    @patch("main.db.save_laws_batch")
    @patch("main.db.save_stats_cache")
    @patch("main.db.save_bill_cosponsors_batch")
    @patch("main.db.mark_bill_cosponsors_refreshed")
    @patch("main.db.clear_bill_cosponsors_for_congress")
    def test_build_stats_auto_source_falls_back_to_api(
        self,
        mock_clear_cosponsors,
        mock_mark_cosponsors,
        mock_save_cosponsors,
        mock_save_stats,
        mock_save_laws,
        mock_save_bills,
        mock_save_legs,
        mock_fetch_cosponsors,
        mock_fetch_laws,
        mock_load_bulk,
    ):
        mock_fetch_laws.return_value = []
        mock_load_bulk.return_value = {
            "119-hr-1": {
                "cosponsors": [
                    {
                        "bioguideId": "C3",
                        "fullName": "Rep. Gamma",
                        "party": "D",
                        "state": "CA",
                        "chamber": "House",
                        "is_original": True,
                        "withdrawn": False,
                    }
                ],
                "sponsor": {"bioguideId": "A1"},
            }
        }

        def api_cosponsor_side_effect(congress, bill, api_key=None):
            if bill.get("number") == 2:
                return [
                    {
                        "bioguideId": "C3",
                        "fullName": "Rep. Gamma",
                        "party": "D",
                        "state": "CA",
                        "chamber": "House",
                        "is_original": False,
                        "withdrawn": False,
                    }
                ]
            return []

        mock_fetch_cosponsors.side_effect = api_cosponsor_side_effect

        bills = [
            {
                "type": "hr",
                "number": 1,
                "originChamber": "House",
                "sponsor": {
                    "bioguideId": "A1",
                    "fullName": "Rep. Alpha",
                    "party": "D",
                    "state": "NY",
                    "chamber": "House",
                },
            },
            {
                "type": "hr",
                "number": 2,
                "originChamber": "House",
                "sponsor": {
                    "bioguideId": "B2",
                    "fullName": "Rep. Beta",
                    "party": "R",
                    "state": "TX",
                    "chamber": "House",
                },
            },
        ]

        with patch("main.fetch_all_bills_for_congress", return_value=bills):
            stats = build_stats(119, cosponsor_source="auto")

        rows = {r["bioguideId"]: r for r in stats["rows"]}
        assert rows["C3"]["cosponsor_total"] == 2
        assert mock_fetch_cosponsors.call_count == 1
