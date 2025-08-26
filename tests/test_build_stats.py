import sys
import pathlib

# Add backend module path
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1] / "backend"))

import main


def test_build_stats_counts_enacted(monkeypatch):
    # Sample bill missing actionCode at list level
    raw_bills = [
        {
            "type": "hr",
            "number": 1,
            "sponsor": {
                "bioguideId": "A000001",
                "fullName": "Alice",
                "party": "D",
                "state": "NY",
            },
            "latestAction": {},
        }
    ]

    def fake_fetch_all(congress, use_cache=True):
        return raw_bills

    def fake_api_get(path, params=None, use_cache=True):
        return {
            "data": {
                "bill": {
                    "sponsors": {
                        "item": [
                            {
                                "bioguideId": "A000001",
                                "fullName": "Alice",
                                "party": "D",
                                "state": "NY",
                                "chamber": "House",
                            }
                        ]
                    },
                    "originChamber": "House",
                    "latestAction": {"actionCode": 36000},
                }
            }
        }

    monkeypatch.setattr(main, "fetch_all_bills_for_congress", fake_fetch_all)
    monkeypatch.setattr(main, "api_get", fake_api_get)

    stats = main.build_stats(118, use_cache=False)
    assert stats["rows"][0]["enacted_total"] == 1
    assert stats["rows"][0]["sponsored_total"] == 1
