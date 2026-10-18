"""Ordered, repeatable PostgreSQL schema migrations."""

from __future__ import annotations

from pathlib import Path

import psycopg


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "firebase" / "migrations"


def apply_migrations(connection: psycopg.Connection) -> list[str]:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    connection.commit()

    applied: list[str] = []
    for path in sorted(MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql")):
        version = path.stem
        row = connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version = %s",
            (version,),
        ).fetchone()
        if row:
            continue
        try:
            connection.execute(path.read_text(encoding="utf-8"))
            connection.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s)",
                (version,),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        applied.append(version)
    return applied
