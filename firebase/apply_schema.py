"""Apply the checked-in PostgreSQL schema to SQL Connect."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg


ROOT = Path(__file__).resolve().parents[1]
database_url = os.environ.get("MONEYMAKER_DATABASE_URL", "").strip()
if not database_url:
    raise SystemExit("Set MONEYMAKER_DATABASE_URL before applying the schema.")

schema = (ROOT / "firebase" / "migrations" / "001_schema.sql").read_text(encoding="utf-8")
with psycopg.connect(database_url) as connection:
    connection.execute(schema)
    connection.commit()
print("MoneyMaker PostgreSQL schema applied.")
