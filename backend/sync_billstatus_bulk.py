"""
CLI script to sync GovInfo Bill Status bulk XML files.
"""
import os
import argparse

from govinfo_bulk_sync import sync_billstatus_bulk, DEFAULT_BULK_JSON_ROOT


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync GovInfo BILLSTATUS bulk XML files")
    parser.add_argument("--congress", type=int, required=True, help="Congress number (e.g., 119)")
    parser.add_argument(
        "--dest",
        default=os.environ.get("BILL_STATUS_BULK_DIR", "./cache/billstatus"),
        help="Destination directory for XML files",
    )
    parser.add_argument(
        "--root-json-url",
        default=os.environ.get("GOVINFO_BULK_JSON_ROOT", DEFAULT_BULK_JSON_ROOT),
        help="GovInfo bulk JSON root URL",
    )
    parser.add_argument(
        "--api-key",
        default=(
            os.environ.get("GOVINFO_API_KEY")
            or os.environ.get("DATA_GOV_API_KEY")
            or os.environ.get("CONGRESS_API_KEY")
            or ""
        ),
        help="Optional data.gov API key",
    )
    args = parser.parse_args()

    summary = sync_billstatus_bulk(
        congress=args.congress,
        dest_dir=args.dest,
        api_key=args.api_key or None,
        root_json_url=args.root_json_url,
    )
    print(summary, flush=True)


if __name__ == "__main__":
    main()

