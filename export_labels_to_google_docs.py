"""Export saved Moneymaker labels to a Google Doc from the command line."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import web_app


def main() -> int:
    parser = argparse.ArgumentParser(description="Send saved Moneymaker labels to Google Docs.")
    parser.add_argument(
        "--cache-file",
        default=web_app.DEFAULT_CACHE_FILE,
        help=f"SQLite cache file to export labels from. Default: {web_app.DEFAULT_CACHE_FILE}",
    )
    parser.add_argument("--scan-id", type=int, help="Optional scan id. Omit to export all saved labels.")
    parser.add_argument("--title", help="Optional Google Doc title.")
    args = parser.parse_args()

    try:
        result = web_app._export_labels_to_google_docs(
            {
                "cache_file": args.cache_file,
                "scan_id": args.scan_id,
                "title": args.title,
            }
        )
    except Exception as exc:
        print(f"Export failed: {exc}")
        return 1

    print(f"Created Google Doc: {result['title']}")
    print(f"Selections exported: {result['selection_count']}")
    print(result["url"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
