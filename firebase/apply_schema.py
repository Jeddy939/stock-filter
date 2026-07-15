"""Apply the checked-in PostgreSQL schema to SQL Connect."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from firebase.schema import apply_migrations

database_url = os.environ.get("MONEYMAKER_DATABASE_URL", "").strip()
if not database_url:
    raise SystemExit("Set MONEYMAKER_DATABASE_URL before applying the schema.")

with psycopg.connect(database_url) as connection:
    applied = apply_migrations(connection)
print(
    "MoneyMaker PostgreSQL schema is current."
    if not applied
    else f"Applied MoneyMaker migrations: {', '.join(applied)}"
)
