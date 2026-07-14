"""Migrate local SQLite caches into the Cloud SQL database used by SQL Connect.

Firebase SQL Connect does not store SQLite files directly. It exposes the
project's Cloud SQL PostgreSQL tables through a typed GraphQL layer, so the
correct migration path is still SQLite -> PostgreSQL tables.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate MoneyMaker SQLite caches into SQL Connect-backed Cloud SQL")
    parser.add_argument("--asx-cache", default=str(ROOT / "stock_cache.sqlite"))
    parser.add_argument("--us-cache", default=str(ROOT / "stock_cache_us.sqlite"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--bulk-prices", action="store_true")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("MONEYMAKER_DATABASE_URL", ""),
        help="PostgreSQL connection URL for the SQL Connect Cloud SQL database",
    )
    args = parser.parse_args()

    if not args.database_url and not args.dry_run:
        parser.error("Set MONEYMAKER_DATABASE_URL or pass --database-url")

    migrate_script = ROOT / "firebase" / "migrate_sqlite_to_postgres.py"
    if not migrate_script.exists():
        raise FileNotFoundError(migrate_script)

    env = {**os.environ}
    if args.database_url:
        env["MONEYMAKER_DATABASE_URL"] = args.database_url

    exit_code = 0
    for market, cache in (("asx", Path(args.asx_cache)), ("us", Path(args.us_cache))):
        if not cache.exists():
            print(f"Skipping {market.upper()}: {cache} does not exist")
            continue
        command = [
            sys.executable,
            str(migrate_script),
            "--market",
            market,
            "--cache",
            str(cache),
        ]
        if args.dry_run:
            command.append("--dry-run")
        if args.resume:
            command.append("--resume")
        if args.bulk_prices:
            command.append("--bulk-prices")
        exit_code = subprocess.call(command, cwd=ROOT, env=env)
        if exit_code:
            return exit_code
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
