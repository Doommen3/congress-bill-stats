"""
Tests for Illinois bipartisan score normalization.
"""
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from illinois_stats import (  # noqa: E402
    BIPARTISAN_PRIOR_WEIGHT,
    ILNameMatcher,
    _calculate_advanced_metrics,
)


def _member(member_id, name, party, chamber="house", district=1):
    first, last = name.split()
    return {
        "member_id": member_id,
        "name": name,
        "first_name": first,
        "last_name": last,
        "party": party,
        "chamber": chamber,
        "district": district,
    }


def _record(member):
    return {
        "memberId": member["member_id"],
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


def test_bipartisan_score_is_smoothed_by_group_baseline():
    members = [
        _member("104-house-1", "Alice Blue", "D", district=1),
        _member("104-house-2", "Bob Red", "R", district=2),
        _member("104-house-3", "Carol Red", "R", district=3),
        _member("104-house-4", "Dana Blue", "D", district=4),
    ]
    matcher = ILNameMatcher(members)
    by_member = {m["member_id"]: _record(m) for m in members}

    bills = [
        {"sponsor_member_id": "104-house-1", "bill_type": "hb", "co_sponsors": ["Rep. Bob Red"]},
        {"sponsor_member_id": "104-house-1", "bill_type": "hb", "co_sponsors": ["Rep. Bob Red"]},
        {"sponsor_member_id": "104-house-3", "bill_type": "hb", "co_sponsors": ["Rep. Bob Red"]},
    ]
    bills.extend(
        {"sponsor_member_id": "104-house-3", "bill_type": "hb", "co_sponsors": ["Rep. Carol Red"]}
        for _ in range(8)
    )

    _calculate_advanced_metrics(by_member, bills, members, matcher)

    bob = by_member["104-house-2"]
    assert bob["bipartisan_cross_party_total"] == 2
    assert bob["bipartisan_total"] == 3
    assert bob["bipartisan_score_raw"] == 66.7

    # House Republicans in this sample have 2 cross-party cosponsorships over 11 total.
    baseline = 2 / 11
    expected_adjusted = round(
        ((2 + (BIPARTISAN_PRIOR_WEIGHT * baseline)) / (3 + BIPARTISAN_PRIOR_WEIGHT)) * 100,
        1,
    )
    assert bob["bipartisan_score"] == expected_adjusted
    assert bob["bipartisan_score"] < bob["bipartisan_score_raw"]


def test_bipartisan_score_fields_default_when_no_cosponsor_matches():
    members = [
        _member("104-house-1", "Alice Blue", "D", district=1),
        _member("104-house-2", "Bob Red", "R", district=2),
    ]
    matcher = ILNameMatcher(members)
    by_member = {m["member_id"]: _record(m) for m in members}

    bills = [
        {"sponsor_member_id": "104-house-1", "bill_type": "hb", "co_sponsors": ["Rep. Unknown Person"]},
    ]

    _calculate_advanced_metrics(by_member, bills, members, matcher)

    assert by_member["104-house-1"]["bipartisan_score"] is None
    assert by_member["104-house-1"]["bipartisan_score_raw"] is None
    assert by_member["104-house-1"]["bipartisan_cross_party_total"] == 0
    assert by_member["104-house-1"]["bipartisan_total"] == 0

