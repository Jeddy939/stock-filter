"""Chronological split construction and model reliability summaries."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Sequence


def event_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def chronological_partitions(
    dates: Sequence[Any],
    *,
    horizon_days: int,
    fold_count: int = 3,
    holdout_fraction: float = 0.2,
) -> dict[str, Any]:
    """Create expanding walk-forward folds grouped by event date.

    Training observations whose outcome windows overlap a validation boundary
    are purged using the selected outcome horizon.
    """
    normalized = [event_date(value) for value in dates]
    unique_dates = sorted(set(normalized))
    if len(unique_dates) < 5:
        return {"folds": [], "development_indices": [], "holdout_indices": [], "holdout_start": None}

    holdout_group_count = max(1, round(len(unique_dates) * holdout_fraction))
    holdout_start = unique_dates[-holdout_group_count]
    development_dates = [value for value in unique_dates if value < holdout_start]
    # The final model trains only on events whose outcome windows closed
    # before the holdout begins, so no holdout-period prices leak into it.
    holdout_embargo = holdout_start - timedelta(days=horizon_days)
    development_indices = [index for index, value in enumerate(normalized) if value < holdout_embargo]
    holdout_indices = [index for index, value in enumerate(normalized) if value >= holdout_start]
    if len(development_dates) < 4:
        return {"folds": [], "development_indices": development_indices, "holdout_indices": holdout_indices, "holdout_start": holdout_start}

    first_validation_group = max(1, len(development_dates) // 2)
    remaining = development_dates[first_validation_group:]
    block_size = max(1, (len(remaining) + fold_count - 1) // fold_count)
    folds: list[dict[str, Any]] = []
    for start in range(0, len(remaining), block_size):
        validation_dates = remaining[start:start + block_size]
        if not validation_dates:
            continue
        validation_start = validation_dates[0]
        embargo_cutoff = validation_start - timedelta(days=horizon_days)
        train = [index for index, value in enumerate(normalized) if value < embargo_cutoff]
        validation = [index for index, value in enumerate(normalized) if value in validation_dates]
        if train and validation:
            folds.append({
                "train_indices": train,
                "validation_indices": validation,
                "train_end": max(normalized[index] for index in train),
                "validation_start": validation_start,
                "validation_end": validation_dates[-1],
                "embargo_days": horizon_days,
            })
    return {
        "folds": folds[:fold_count],
        "development_indices": development_indices,
        "holdout_indices": holdout_indices,
        "holdout_start": holdout_start,
    }


def calibration_summary(labels: Sequence[int], scores: Sequence[float], bins: int = 5) -> dict[str, Any]:
    if not labels or len(labels) != len(scores):
        return {"bins": [], "brier_score": None, "calibrated": False}
    brier = sum((float(score) - int(label)) ** 2 for label, score in zip(labels, scores)) / len(labels)
    rows: list[dict[str, Any]] = []
    for bin_index in range(bins):
        low = bin_index / bins
        high = (bin_index + 1) / bins
        selected = [
            (label, score)
            for label, score in zip(labels, scores)
            if low <= score < high or (bin_index == bins - 1 and score == 1)
        ]
        if selected:
            rows.append({
                "lower": low,
                "upper": high,
                "count": len(selected),
                "mean_score": sum(score for _, score in selected) / len(selected),
                "observed_rate": sum(label for label, _ in selected) / len(selected),
            })
    # Scores only count as calibrated probabilities when they beat always
    # predicting the observed base rate.
    base_rate = sum(int(label) for label in labels) / len(labels)
    reference_brier = base_rate * (1 - base_rate)
    enough_bins = sum(row["count"] >= 5 for row in rows) >= 3
    return {
        "bins": rows,
        "brier_score": brier,
        "reference_brier_score": reference_brier,
        "brier_skill": 1 - brier / reference_brier if reference_brier else None,
        "calibrated": enough_bins and brier < reference_brier,
    }
