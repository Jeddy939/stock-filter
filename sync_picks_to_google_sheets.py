"""Sync saved Moneymaker picks to the shared Google Sheet from the command line."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import web_app


def _looks_like_secret(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if "client_secret" in lowered or "apps.googleusercontent.com" in lowered:
        return True
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return False
    return isinstance(data, dict) and ("installed" in data or "web" in data)


def _invalid_sheet_input_message(value: str) -> str:
    text = (value or "").strip()
    lowered = text.lower()
    if not text:
        return ""
    if _looks_like_secret(text):
        return (
            "That looks like a Google OAuth secret, not a Google Sheet link/ID.\n"
            "Leave the secret as the JSON file in this folder. Do not paste it here."
        )
    if "localhost" in lowered or "127.0.0.1" in lowered:
        return (
            "That is the local Moneymaker app URL, not a Google Sheet link.\n"
            "Paste a Google Sheets link that starts with https://docs.google.com/spreadsheets/."
        )
    if text == "1AbCxyz123":
        return (
            "That is the fake example Sheet ID from the instructions, not your real Sheet.\n"
            "Open the shared Sheet and copy the real link from the browser address bar."
        )
    if lowered.startswith("http") and "docs.google.com/spreadsheets/" not in lowered:
        return (
            "That URL is not a Google Sheet link.\n"
            "Paste a Google Sheets link that starts with https://docs.google.com/spreadsheets/."
        )
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync saved Moneymaker picks to Google Sheets.")
    parser.add_argument(
        "--cache-file",
        default=web_app.DEFAULT_CACHE_FILE,
        help=f"SQLite cache file to sync. Default: {web_app.DEFAULT_CACHE_FILE}",
    )
    parser.add_argument("--sheet", help="Google Sheet link or ID to use as the shared picks list.")
    parser.add_argument("--user-name", help="Name to show in the shared picks list.")
    parser.add_argument("--create", action="store_true", help="Create a new shared Google Sheet first.")
    args = parser.parse_args()

    try:
        invalid_message = _invalid_sheet_input_message(args.sheet or "")
        if invalid_message:
            print(invalid_message)
            return 1
        if args.create:
            created = web_app._create_shared_picks_sheet({"user_name": args.user_name})
            print(f"Created shared Google Sheet: {created['sheet_url']}")
        elif args.sheet or args.user_name:
            web_app._save_shared_settings({"sheet_id": args.sheet, "user_name": args.user_name})

        settings = web_app._load_shared_settings()
        if not settings.get("sheet_id"):
            print("No shared Google Sheet is configured.")
            print("Run this again with --create, or pass --sheet with a Google Sheet link/ID.")
            return 1

        result = web_app._sync_shared_picks(args.cache_file)
    except Exception as exc:
        print(f"Sync failed: {exc}")
        return 1

    print("Shared picks synced to Google Sheets.")
    print(f"Local picks before sync: {result.get('local_count', 0)}")
    print(f"Shared picks after sync: {result.get('shared_count', 0)}")
    print(f"https://docs.google.com/spreadsheets/d/{result['spreadsheet_id']}/edit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
