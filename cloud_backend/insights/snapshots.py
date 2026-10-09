"""Create immutable point-in-time feature snapshots for rating events."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
from typing import Any, Iterable

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .anchor import ANCHOR_POLICY
from .features import FEATURE_DEFINITIONS, FEATURE_VERSION, FeatureValue, calculate_technical_features
from .fundamentals import FUNDAMENTAL_DEFINITIONS, point_in_time_fundamentals


def _json_feature(value: FeatureValue) -> dict[str, Any]:
    return value.as_json()


def ensure_feature_definitions(cursor: psycopg.Cursor[Any]) -> dict[str, int]:
    """Register this feature version without mutating prior definitions."""
    for definition in (*FEATURE_DEFINITIONS, *FUNDAMENTAL_DEFINITIONS):
        cursor.execute(
            """
            INSERT INTO feature_definitions (
                feature_name, feature_version, category, value_type, unit,
                description, formula, required_history_days, source_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (feature_name, feature_version) DO NOTHING
            """,
            (
                definition.name,
                FEATURE_VERSION,
                definition.category,
                definition.value_type,
                definition.unit,
                definition.description,
                definition.formula,
                definition.required_history_days,
                definition.source_name,
            ),
        )
    cursor.execute(
        """
        SELECT id, feature_name
        FROM feature_definitions
        WHERE feature_version = %s AND enabled = TRUE
        """,
        (FEATURE_VERSION,),
    )
    return {str(row["feature_name"]): int(row["id"]) for row in cursor.fetchall()}


def create_snapshot_stub(
    cursor: psycopg.Cursor[Any],
    rating_event_id: int,
    feature_version: int = FEATURE_VERSION,
) -> int | None:
    """Insert a retry-safe queued snapshot using only event-time information."""
    cursor.execute(
        """
        WITH target AS (
            SELECT id, firebase_uid, market, ticker, label, event_at_utc
            FROM rating_events
            WHERE id = %s AND action = 'label' AND label IS NOT NULL
              AND firebase_uid IS NOT NULL AND market IN ('asx', 'us')
        ), origin AS (
            SELECT first_event.id
            FROM target
            JOIN LATERAL (
                SELECT re.id
                FROM rating_events re
                WHERE re.firebase_uid = target.firebase_uid
                  AND re.market = target.market
                  AND re.ticker = target.ticker
                  AND re.action = 'label'
                  AND re.label IS NOT NULL
                  AND (re.event_at_utc, re.id) <= (target.event_at_utc, target.id)
                ORDER BY re.event_at_utc, re.id
                LIMIT 1
            ) first_event ON TRUE
        )
        INSERT INTO pick_feature_snapshots (
            rating_event_id, origin_event_id, firebase_uid, market, ticker,
            appraisal_label, appraisal_at_utc, feature_version, snapshot_status
        )
        SELECT target.id, origin.id, target.firebase_uid, target.market, target.ticker,
               target.label, target.event_at_utc, %s, 'queued'
        FROM target CROSS JOIN origin
        ON CONFLICT (rating_event_id, feature_version) DO NOTHING
        RETURNING id
        """,
        (rating_event_id, feature_version),
    )
    row = cursor.fetchone()
    return int(row["id"]) if row else None


def _store_feature_values(
    cursor: psycopg.Cursor[Any],
    snapshot_id: int,
    definition_ids: dict[str, int],
    values: dict[str, FeatureValue],
) -> None:
    cursor.execute("DELETE FROM pick_feature_values WHERE snapshot_id = %s", (snapshot_id,))
    rows = []
    for name, value in values.items():
        definition_id = definition_ids.get(name)
        if definition_id is None:
            raise RuntimeError(f"Feature definition is missing for {name}")
        numeric_value: float | None = None
        boolean_value: bool | None = None
        categorical_value: str | None = None
        if not value.is_missing:
            if isinstance(value.value, bool):
                boolean_value = value.value
            elif isinstance(value.value, (int, float)):
                numeric_value = float(value.value)
            else:
                categorical_value = str(value.value)
        rows.append((
            snapshot_id,
            definition_id,
            numeric_value,
            boolean_value,
            categorical_value,
            value.is_missing,
            value.missing_reason,
            value.source_as_of_utc,
        ))
    # One batched round trip instead of one insert per feature.
    cursor.executemany(
        """
        INSERT INTO pick_feature_values (
            snapshot_id, feature_definition_id, numeric_value, boolean_value,
            categorical_value, is_missing, missing_reason, source_as_of_utc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def process_snapshot(
    connection: psycopg.Connection[Any],
    snapshot_id: int,
    definition_ids: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Build one snapshot. Complete snapshots are immutable and skipped.

    Pass ``definition_ids`` from ``ensure_feature_definitions`` when building
    many snapshots so the definitions are registered once per run.
    """
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT snapshot.*, event.provider, event.scan_id, event.source_id,
                   event.rank, event.signal_date, event.close_price,
                   event.market_cap, event.avg_volume, event.volume_ratio,
                   event.sector, event.industry, event.result_json
            FROM pick_feature_snapshots snapshot
            JOIN rating_events event ON event.id = snapshot.rating_event_id
            WHERE snapshot.id = %s
            FOR UPDATE OF snapshot
            """,
            (snapshot_id,),
        )
        snapshot = cursor.fetchone()
        if not snapshot:
            return {"snapshot_id": snapshot_id, "status": "missing"}
        if snapshot["snapshot_status"] == "complete":
            return {"snapshot_id": snapshot_id, "status": "complete", "skipped": True}

        cursor.execute(
            """
            UPDATE pick_feature_snapshots
            SET snapshot_status = 'running', error = NULL, completed_at_utc = NULL
            WHERE id = %s
            """,
            (snapshot_id,),
        )
        event_provider = str(snapshot.get("provider") or "").strip()
        cursor.execute(
            """
            SELECT provider, MAX(price_date) AS feature_as_of_date
            FROM price_history
            WHERE market = %s AND ticker = %s
              AND price_date <= appraisal_cutoff_date(%s, %s::timestamptz)
              AND close_price IS NOT NULL
            GROUP BY provider
            ORDER BY CASE
                       WHEN provider = %s THEN 0
                       WHEN provider = 'yfinance' THEN 1
                       ELSE 2
                     END,
                     MAX(price_date) DESC
            LIMIT 1
            """,
            (snapshot["market"], snapshot["ticker"], snapshot["market"], snapshot["appraisal_at_utc"], event_provider),
        )
        cutoff_row = cursor.fetchone()
        cutoff: date | None = cutoff_row["feature_as_of_date"] if cutoff_row else None
        if cutoff is None:
            cursor.execute(
                """
                UPDATE pick_feature_snapshots
                SET snapshot_status = 'failed', error = %s, completed_at_utc = now(),
                    quality_json = %s
                WHERE id = %s
                """,
                ("No price history existed at the appraisal cutoff", Jsonb({"price_history_available": False}), snapshot_id),
            )
            return {"snapshot_id": snapshot_id, "status": "failed", "error": "No point-in-time price history"}
        provider = str(cutoff_row["provider"])

        cursor.execute(
            """
            SELECT price_date AS date, open_price AS open, high_price AS high,
                   low_price AS low, close_price AS close, volume
            FROM price_history
            WHERE market = %s AND provider = %s AND ticker = %s
              AND price_date <= %s
            ORDER BY price_date
            """,
            (snapshot["market"], provider, snapshot["ticker"], cutoff),
        )
        history = list(cursor.fetchall())
        values = calculate_technical_features(history)
        appraisal_close = float(history[-1]["close"])
        fundamental_values, fundamental_quality = point_in_time_fundamentals(
            cursor,
            market=str(snapshot["market"]),
            ticker=str(snapshot["ticker"]),
            appraisal_at_utc=snapshot["appraisal_at_utc"],
            appraisal_close=appraisal_close,
        )
        definitions = definition_ids if definition_ids is not None else ensure_feature_definitions(cursor)
        _store_feature_values(cursor, snapshot_id, definitions, {**values, **fundamental_values})

        missing = [name for name, value in values.items() if value.is_missing]
        context = {
            "price_provider": provider,
            "appraisal_source_provider": event_provider or None,
            "scan_id": snapshot.get("scan_id"),
            "source_id": snapshot.get("source_id"),
            "rank": snapshot.get("rank"),
            "signal_date": str(snapshot.get("signal_date") or "") or None,
            "signal_price": snapshot.get("close_price"),
            "market_cap": snapshot.get("market_cap"),
            "avg_volume": snapshot.get("avg_volume"),
            "volume_ratio": snapshot.get("volume_ratio"),
            "sector": snapshot.get("sector"),
            "industry": snapshot.get("industry"),
            "scan_result": snapshot.get("result_json") or {},
        }
        quality = {
            "feature_version": FEATURE_VERSION,
            "price_basis": "price_history",
            "anchor_policy": ANCHOR_POLICY,
            "provider": provider,
            "history_rows": len(history),
            "feature_count": len(values),
            "available_feature_count": len(values) - len(missing),
            "missing_feature_count": len(missing),
            "missing_features": missing,
            "fundamentals": fundamental_quality,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        fundamental_missing = [name for name, value in fundamental_values.items() if value.is_missing]
        status = "partial" if snapshot["market"] == "us" and fundamental_quality["eligible_fact_rows"] == 0 else "complete"
        cursor.execute(
            """
            UPDATE pick_feature_snapshots
            SET feature_as_of_date = %s,
                snapshot_status = %s,
                technical_json = %s,
                fundamental_json = %s,
                context_json = %s,
                quality_json = %s,
                error = NULL,
                completed_at_utc = now()
            WHERE id = %s
            """,
            (
                cutoff,
                status,
                Jsonb({name: _json_feature(value) for name, value in values.items()}),
                Jsonb({name: _json_feature(value) for name, value in fundamental_values.items()}),
                Jsonb(context),
                Jsonb(quality),
                snapshot_id,
            ),
        )
        return {
            "snapshot_id": snapshot_id,
            "status": status,
            **quality,
            "fundamental_missing_features": fundamental_missing,
            "feature_as_of_date": cutoff.isoformat(),
        }


def queued_snapshot_ids(
    connection: psycopg.Connection[Any],
    *,
    event_ids: Iterable[int] | None = None,
    market: str | None = None,
    owner_uid: str | None = None,
    limit: int = 1000,
    statuses: Iterable[str] = ("queued", "partial", "failed"),
) -> list[int]:
    requested_ids = list(event_ids or [])
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT id
            FROM pick_feature_snapshots
            WHERE feature_version = %s
              AND snapshot_status = ANY(%s::text[])
              AND (%s::text IS NULL OR market = %s)
              AND (%s::text IS NULL OR firebase_uid = %s)
              AND (cardinality(%s::bigint[]) = 0 OR rating_event_id = ANY(%s::bigint[]))
            ORDER BY appraisal_at_utc, id
            LIMIT %s
            """,
            (FEATURE_VERSION, list(statuses), market, market, owner_uid, owner_uid, requested_ids, requested_ids, limit),
        )
        return [int(row["id"]) for row in cursor.fetchall()]


def seed_snapshot_stubs(
    connection: psycopg.Connection[Any],
    *,
    market: str | None = None,
    owner_uid: str | None = None,
    labels: Iterable[str] | None = None,
    limit: int = 100000,
) -> int:
    requested_labels = list(labels or ["winner", "maybe", "bad", "needs_confirmation"])
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT id
            FROM rating_events
            WHERE action = 'label' AND label = ANY(%s::text[])
              AND firebase_uid IS NOT NULL AND market IN ('asx', 'us')
              AND (%s::text IS NULL OR market = %s)
              AND (%s::text IS NULL OR firebase_uid = %s)
            ORDER BY event_at_utc, id
            LIMIT %s
            """,
            (requested_labels, market, market, owner_uid, owner_uid, limit),
        )
        event_ids = [int(row["id"]) for row in cursor.fetchall()]
        created = sum(1 for event_id in event_ids if create_snapshot_stub(cursor, event_id) is not None)
    connection.commit()
    return created
