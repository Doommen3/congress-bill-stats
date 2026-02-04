"""
Nightly refresh script for Congress and Illinois stats.
Intended to be run by a scheduler (e.g., Render cron job).
"""
import os

from main import build_stats, save_cache, DEFAULT_CONGRESS
from illinois_stats import build_il_stats, save_il_cache, DEFAULT_IL_SESSION


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

    for congress in congress_list:
        stats = build_stats(congress)
        save_cache(congress, stats)

    for session in il_list:
        stats = build_il_stats(session)
        save_il_cache(session, stats)


if __name__ == "__main__":
    main()
