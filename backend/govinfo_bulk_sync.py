"""
Sync helpers for GovInfo Bill Status bulk XML data.
"""
import json
import os
import zipfile
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urljoin

import requests
from requests.exceptions import RequestException


DEFAULT_BULK_JSON_ROOT = "https://www.govinfo.gov/bulkdata/json/BILLSTATUS"


@dataclass
class RemoteFile:
    url: str
    relative_path: str
    modified: Optional[str] = None


def _headers(api_key: Optional[str]) -> Dict[str, str]:
    if not api_key:
        return {"Accept": "application/json"}
    # GovInfo API uses data.gov keys; for bulkdata endpoints keys are usually not required.
    return {"Accept": "application/json", "X-Api-Key": api_key}


def _norm_url(base_url: str, maybe_url: str) -> Optional[str]:
    if not maybe_url:
        return None
    maybe_url = str(maybe_url).strip()
    if not maybe_url:
        return None
    if maybe_url.startswith("http://") or maybe_url.startswith("https://"):
        return maybe_url
    return urljoin(base_url.rstrip("/") + "/", maybe_url)


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "t", "yes", "y", "1"):
            return True
        if low in ("false", "f", "no", "n", "0"):
            return False
    return None


def _is_file_url(url: str) -> bool:
    lower = url.lower()
    return lower.endswith(".xml") or lower.endswith(".zip")


def _to_json_listing_url(url: str) -> str:
    """
    Convert GovInfo bulk directory URLs into JSON index URLs.
    """
    if "/bulkdata/json/" in url:
        return url.rstrip("/")
    if "/bulkdata/" in url:
        return url.replace("/bulkdata/", "/bulkdata/json/").rstrip("/")
    return url.rstrip("/")


def _extract_modified(node: Dict[str, Any]) -> Optional[str]:
    for key in ("lastModified", "modified", "updated", "lastUpdated", "mtime"):
        val = node.get(key)
        if val:
            return str(val)
    return None


def _extract_nodes(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("children", "childNodes", "entries", "items", "files", "directories", "results"):
        val = payload.get(key)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]

    # Fallback: if dict values are entry-like objects, use them.
    values = [v for v in payload.values() if isinstance(v, dict)]
    if values:
        return values
    return []


def _extract_links(node: Dict[str, Any]) -> List[str]:
    links: List[str] = []
    for key in ("href", "url", "link", "path", "downloadUrl"):
        val = node.get(key)
        if isinstance(val, str):
            links.append(val)
    return links


def _billstatus_relative_path(file_url: str) -> str:
    path = urlparse(file_url).path
    marker = "/BILLSTATUS/"
    idx = path.upper().find(marker)
    if idx >= 0:
        return path[idx + len(marker):].lstrip("/")
    return os.path.basename(path)


def discover_billstatus_files(
    congress: int,
    api_key: Optional[str] = None,
    root_json_url: str = DEFAULT_BULK_JSON_ROOT,
    session: Optional[requests.Session] = None,
) -> List[RemoteFile]:
    """
    Discover Bill Status XML/ZIP files for a congress by traversing the GovInfo bulk JSON tree.
    """
    sess = session or requests.Session()
    start_url = f"{root_json_url.rstrip('/')}/{congress}"
    pending = [start_url]
    seen = set()
    files: Dict[str, RemoteFile] = {}

    while pending:
        current = pending.pop()
        current = _to_json_listing_url(current)
        if current in seen:
            continue
        seen.add(current)

        try:
            resp = sess.get(current, headers=_headers(api_key), timeout=(10, 30))
            if resp.status_code != 200:
                continue
            payload = resp.json()
        except (RequestException, ValueError):
            continue

        for node in _extract_nodes(payload):
            explicit_dir = None
            for key in ("isDirectory", "directory", "isDir", "dir", "folder", "isFolder"):
                if key in node:
                    explicit_dir = _as_bool(node.get(key))
                    if explicit_dir is not None:
                        break

            typ = str(node.get("type") or "").lower()
            if typ in ("directory", "dir", "folder"):
                explicit_dir = True
            elif typ in ("file",):
                explicit_dir = False

            links = _extract_links(node)
            if not links:
                continue

            for raw_link in links:
                full_url = _norm_url(current, raw_link)
                if not full_url:
                    continue

                if _is_file_url(full_url):
                    rel = _billstatus_relative_path(full_url)
                    files[full_url] = RemoteFile(
                        url=full_url,
                        relative_path=rel,
                        modified=_extract_modified(node),
                    )
                    continue

                is_dir = explicit_dir if explicit_dir is not None else full_url.endswith("/")
                if is_dir:
                    pending.append(full_url)

    return list(files.values())


def _manifest_path(dest_dir: str) -> str:
    return os.path.join(dest_dir, ".billstatus_manifest.json")


def _load_manifest(dest_dir: str) -> Dict[str, Dict[str, Any]]:
    fp = _manifest_path(dest_dir)
    if not os.path.exists(fp):
        return {}
    try:
        with open(fp, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_manifest(dest_dir: str, manifest: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(dest_dir, exist_ok=True)
    fp = _manifest_path(dest_dir)
    tmp = fp + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp, fp)


def _download_bytes(url: str, api_key: Optional[str], session: requests.Session) -> Optional[bytes]:
    try:
        resp = session.get(url, headers=_headers(api_key), timeout=(10, 60))
        if resp.status_code != 200:
            return None
        return resp.content
    except RequestException:
        return None


def _write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def _write_zip_xmls(dest_dir: str, rel_path: str, zip_bytes: bytes) -> List[str]:
    """
    Extract XML files from a downloaded ZIP and write them under dest_dir.
    Returns relative paths written.
    """
    written: List[str] = []
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xml"):
                continue
            rel = os.path.join(os.path.dirname(rel_path), os.path.basename(name))
            target = os.path.join(dest_dir, rel)
            data = zf.read(name)
            _write_bytes(target, data)
            written.append(rel)
    return written


def sync_billstatus_bulk(
    congress: int,
    dest_dir: str,
    api_key: Optional[str] = None,
    root_json_url: str = DEFAULT_BULK_JSON_ROOT,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    Discover and sync Bill Status XML files to local disk with manifest-based skipping.
    """
    sess = session or requests.Session()
    discovered = discover_billstatus_files(
        congress=congress,
        api_key=api_key,
        root_json_url=root_json_url,
        session=sess,
    )

    manifest = _load_manifest(dest_dir)
    next_manifest = dict(manifest)

    downloaded = 0
    skipped = 0
    failed = 0

    for item in discovered:
        rel = item.relative_path
        old = manifest.get(rel) or {}
        if item.modified and old.get("modified") == item.modified:
            skipped += 1
            continue

        data = _download_bytes(item.url, api_key, sess)
        if data is None:
            failed += 1
            continue

        try:
            if item.url.lower().endswith(".zip"):
                written = _write_zip_xmls(dest_dir, rel, data)
                for rel_xml in written:
                    next_manifest[rel_xml] = {
                        "source_url": item.url,
                        "modified": item.modified,
                    }
            else:
                target = os.path.join(dest_dir, rel)
                _write_bytes(target, data)
                next_manifest[rel] = {
                    "source_url": item.url,
                    "modified": item.modified,
                }
            downloaded += 1
        except Exception:
            failed += 1
            continue

    _save_manifest(dest_dir, next_manifest)

    return {
        "congress": congress,
        "discovered": len(discovered),
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
        "dest_dir": dest_dir,
    }
