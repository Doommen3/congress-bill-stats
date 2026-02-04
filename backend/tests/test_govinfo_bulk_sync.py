"""
Tests for GovInfo BILLSTATUS bulk sync helpers.
"""
import os
import sys
from unittest.mock import patch

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from govinfo_bulk_sync import discover_billstatus_files, sync_billstatus_bulk, RemoteFile


class _Resp:
    def __init__(self, status_code=200, payload=None, content=b""):
        self.status_code = status_code
        self._payload = payload
        self.content = content

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, mapping):
        self.mapping = mapping
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append(url)
        item = self.mapping.get(url)
        if item is None:
            return _Resp(status_code=404, payload={})
        if isinstance(item, bytes):
            return _Resp(status_code=200, payload=None, content=item)
        return _Resp(status_code=200, payload=item, content=b"")


def test_discover_billstatus_files_walks_json_tree():
    root = "https://www.govinfo.gov/bulkdata/json/BILLSTATUS"
    session = _FakeSession(
        {
            f"{root}/119": {
                "files": [
                    {"folder": True, "link": "https://www.govinfo.gov/bulkdata/BILLSTATUS/119/hr"},
                ]
            },
            "https://www.govinfo.gov/bulkdata/json/BILLSTATUS/119/hr": {
                "files": [
                    {
                        "folder": False,
                        "link": "https://www.govinfo.gov/bulkdata/BILLSTATUS/119/hr/BILLSTATUS-119hr1.xml",
                        "lastModified": "2026-01-01T00:00:00Z",
                    }
                ]
            },
        }
    )

    files = discover_billstatus_files(119, root_json_url=root, session=session)

    assert len(files) == 1
    assert files[0].relative_path.endswith("119/hr/BILLSTATUS-119hr1.xml")
    assert files[0].modified == "2026-01-01T00:00:00Z"


@patch("govinfo_bulk_sync.discover_billstatus_files")
@patch("govinfo_bulk_sync._download_bytes")
def test_sync_billstatus_bulk_uses_manifest_skip(mock_download, mock_discover, tmp_path):
    mock_discover.return_value = [
        RemoteFile(
            url="https://www.govinfo.gov/bulkdata/BILLSTATUS/119/hr/BILLSTATUS-119hr1.xml",
            relative_path="119/hr/BILLSTATUS-119hr1.xml",
            modified="2026-01-01T00:00:00Z",
        )
    ]
    mock_download.return_value = b"<billStatus></billStatus>"

    first = sync_billstatus_bulk(119, dest_dir=str(tmp_path))
    assert first["downloaded"] == 1
    assert first["skipped"] == 0

    second = sync_billstatus_bulk(119, dest_dir=str(tmp_path))
    assert second["downloaded"] == 0
    assert second["skipped"] == 1
    assert mock_download.call_count == 1
