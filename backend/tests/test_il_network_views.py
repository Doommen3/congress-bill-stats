"""
Tests for Illinois network view payloads (force graph + hierarchical edge bundling).
"""
import os
import sys
import importlib
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import illinois_database


def _collect_leaf_ids(node):
    """Collect leaf IDs from a hierarchy payload."""
    children = node.get("children") or []
    if not children:
        return [node["id"]] if node.get("id") else []
    leaf_ids = []
    for child in children:
        leaf_ids.extend(_collect_leaf_ids(child))
    return leaf_ids


def test_build_il_edge_bundling_hierarchy_groups_by_chamber_and_party():
    nodes = [
        {"id": "104-house-1", "name": "Alice Adams", "party": "D", "chamber": "house", "district": 1},
        {"id": "104-house-2", "name": "Bob Brown", "party": "R", "chamber": "house", "district": 2},
        {"id": "104-senate-3", "name": "Carol Clark", "party": "D", "chamber": "senate", "district": 3},
    ]
    links = [
        {"source": "104-house-1", "target": "104-house-2", "value": 4},
        {"source": "104-house-1", "target": "104-senate-3", "value": 2},
    ]

    hierarchy = illinois_database.build_il_edge_bundling_hierarchy(nodes, links)

    assert hierarchy["name"] == "Illinois GA"
    leaf_ids = set(_collect_leaf_ids(hierarchy))
    assert leaf_ids == {"104-house-1", "104-house-2", "104-senate-3"}

    # Ensure link relationships are preserved for edge bundling routing.
    house_node = next(ch for ch in hierarchy["children"] if ch["key"] == "house")
    dem_party = next(p for p in house_node["children"] if p["key"] == "D")
    alice_leaf = next(m for m in dem_party["children"] if m["id"] == "104-house-1")
    assert alice_leaf["connection_ids"] == ["104-house-2", "104-senate-3"]


@pytest.fixture
def temp_network_db(tmp_path):
    """Reload Illinois DB module with an isolated database."""
    old_db_path = os.environ.get("DATABASE_PATH")
    db_path = str(tmp_path / "network_views.db")
    os.environ["DATABASE_PATH"] = db_path

    importlib.reload(illinois_database)
    yield illinois_database

    if old_db_path is None:
        os.environ.pop("DATABASE_PATH", None)
    else:
        os.environ["DATABASE_PATH"] = old_db_path
    importlib.reload(illinois_database)


def test_get_il_network_data_edge_bundling_includes_hierarchy(temp_network_db):
    temp_network_db.save_il_legislators_batch(104, [
        {
            "member_id": "104-house-1",
            "chamber": "house",
            "district": 1,
            "name": "Alice Adams",
            "first_name": "Alice",
            "last_name": "Adams",
            "party": "D",
        },
        {
            "member_id": "104-house-2",
            "chamber": "house",
            "district": 2,
            "name": "Bob Brown",
            "first_name": "Bob",
            "last_name": "Brown",
            "party": "R",
        },
    ])

    temp_network_db.save_il_bills_batch(104, [
        {
            "bill_type": "hb",
            "bill_number": 1,
            "sponsor_member_id": "104-house-1",
            "chief_co_sponsors": ["Bob Brown"],
            "co_sponsors": [],
        },
        {
            "bill_type": "hb",
            "bill_number": 2,
            "sponsor_member_id": "104-house-2",
            "chief_co_sponsors": ["Alice Adams"],
            "co_sponsors": [],
        },
    ])

    payload = temp_network_db.get_il_network_data(104, min_connections=1, view="edge_bundling")

    assert payload["view"] == "edge_bundling"
    assert len(payload["nodes"]) == 2
    assert len(payload["links"]) == 1
    assert "hierarchy" in payload
    leaf_ids = set(_collect_leaf_ids(payload["hierarchy"]))
    assert leaf_ids == {"104-house-1", "104-house-2"}
