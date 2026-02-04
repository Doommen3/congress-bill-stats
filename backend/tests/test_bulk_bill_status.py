"""
Tests for Bill Status bulk XML parsing and loading.
"""
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bulk_bill_status import parse_bill_status_xml, load_bulk_bill_status


def test_parse_bill_status_xml_with_namespace():
    xml_text = """<?xml version="1.0" encoding="UTF-8"?>
    <billStatus xmlns="http://schemas.congress.gov/billstatus">
      <bill>
        <congress>119</congress>
        <billType>hr</billType>
        <billNumber>123</billNumber>
        <updateDate>2025-02-01</updateDate>
        <sponsors>
          <item>
            <bioguideId>A000001</bioguideId>
            <fullName>Rep. Alpha</fullName>
            <party>D</party>
            <state>NY</state>
            <chamber>House</chamber>
          </item>
        </sponsors>
        <cosponsors>
          <item>
            <bioguideId>B000002</bioguideId>
            <isOriginalCosponsor>true</isOriginalCosponsor>
          </item>
          <item>
            <bioguideId>C000003</bioguideId>
            <sponsorshipWithdrawnDate>2025-03-01</sponsorshipWithdrawnDate>
          </item>
        </cosponsors>
      </bill>
    </billStatus>
    """

    parsed = parse_bill_status_xml(xml_text)
    assert parsed is not None
    assert parsed["bill_id"] == "119-hr-123"
    assert parsed["sponsor"]["bioguideId"] == "A000001"
    assert len(parsed["cosponsors"]) == 2
    assert parsed["cosponsors"][0]["is_original"] is True
    assert parsed["cosponsors"][1]["withdrawn"] is True


def test_load_bulk_bill_status_filters_congress(tmp_path):
    xml_119 = """<billStatus><bill><congress>119</congress><billType>hr</billType><billNumber>1</billNumber></bill></billStatus>"""
    xml_118 = """<billStatus><bill><congress>118</congress><billType>s</billType><billNumber>2</billNumber></bill></billStatus>"""
    bad_xml = """<billStatus><bill>"""

    (tmp_path / "a.xml").write_text(xml_119, encoding="utf-8")
    (tmp_path / "b.xml").write_text(xml_118, encoding="utf-8")
    (tmp_path / "bad.xml").write_text(bad_xml, encoding="utf-8")

    records = load_bulk_bill_status(119, str(tmp_path), max_workers=2)
    assert "119-hr-1" in records
    assert "118-s-2" not in records

