"""
Nightly refresh script for Congress and Illinois stats.
Intended to be run by a scheduler (e.g., Render cron job).
"""
import os

from main import build_stats, save_cache, DEFAULT_CONGRESS, DEFAULT_IL_SESSION
from illinois_stats import build_il_stats, save_il_cache
from govinfo_bulk_sync import sync_billstatus_bulk, DEFAULT_BULK_JSON_ROOT


def _parse_int_list(value: str) -> list[int]:
    items = [v.strip() for v in (value or "").split(",") if v.strip()]
    out = []
    for item in items:
        try:
            out.append(int(item))
        except ValueError:
            continue
    return out


def main() -> None:
    congress_list = _parse_int_list(os.environ.get("CRON_CONGRESS", ""))
    if not congress_list:
        congress_list = [DEFAULT_CONGRESS]

    il_list = _parse_int_list(os.environ.get("CRON_IL_SESSIONS", ""))
    if not il_list:
        il_list = [DEFAULT_IL_SESSION]

    api_key = (
        os.environ.get("CRON_CONGRESS_API_KEY")
        or os.environ.get("ADMIN_CONGRESS_API_KEY")
        or os.environ.get("CONGRESS_API_KEY")
    )
    cosponsor_mode = os.environ.get("CRON_COSPONSOR_MODE", "full")
    cosponsor_source = os.environ.get("CRON_COSPONSOR_SOURCE", "auto")
    sync_bulk = os.environ.get("CRON_SYNC_BILLSTATUS_BULK", "0") == "1"
    bulk_dir = os.environ.get("BILL_STATUS_BULK_DIR", "").strip()
    bulk_root = os.environ.get("GOVINFO_BULK_JSON_ROOT", "")

    for congress in congress_list:
        if sync_bulk and bulk_dir:
            sync_summary = sync_billstatus_bulk(
                congress=congress,
                dest_dir=bulk_dir,
                api_key=(
                    os.environ.get("GOVINFO_API_KEY")
                    or os.environ.get("DATA_GOV_API_KEY")
                    or api_key
                ),
                root_json_url=bulk_root or DEFAULT_BULK_JSON_ROOT,
            )
            print(f"[cron] Billstatus sync summary: {sync_summary}", flush=True)
        stats = build_stats(
            congress,
            api_key=api_key,
            cosponsor_mode=cosponsor_mode,
            cosponsor_source=cosponsor_source,
        )
        save_cache(congress, stats)

    for session in il_list:
        stats = build_il_stats(session)
        save_il_cache(session, stats)


if __name__ == "__main__":
    main()
