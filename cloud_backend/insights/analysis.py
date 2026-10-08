"""Leakage-resistant descriptive analysis for mature appraisal snapshots."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
import json
import math
from statistics import fmean, median, variance
from typing import Any, Iterable


@dataclass(frozen=True)
class SampleGate:
    level: str
    allow_descriptive: bool
    allow_regularized_models: bool
    allow_boosting: bool
    message: str


def sample_gate(mature_count: int) -> SampleGate:
    if mature_count < 20:
        return SampleGate("coverage", False, False, False, "At least 20 mature winner events are required for feature comparisons.")
    if mature_count < 50:
        return SampleGate("descriptive", True, False, False, "Results are exploratory descriptive comparisons only.")
    if mature_count < 200:
        return SampleGate("regularized", True, True, False, "Regularized and shallow-tree models are allowed.")
    return SampleGate("boosted", True, True, True, "All model tiers are allowed, subject to walk-forward validation.")


def percentile(values: list[float], probability: float) -> float:
    if not values:
        raise ValueError("percentile requires values")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def market_cohort_cutoffs(rows: Iterable[dict[str, Any]]) -> dict[str, tuple[float, float]]:
    """Return (low, high) quartile cutoffs of benchmark excess per market.

    ASX and US appraisals are measured against different benchmarks, so their
    excess returns are ranked separately rather than pooled.
    """
    outcomes_by_market: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        outcomes_by_market[str(row.get("market") or "all")].append(float(row["benchmark_excess_return_percent"]))
    return {
        market: (percentile(values, 0.25), percentile(values, 0.75))
        for market, values in outcomes_by_market.items()
    }


def cohort_of(row: dict[str, Any], cutoffs: dict[str, tuple[float, float]]) -> str:
    low, high = cutoffs[str(row.get("market") or "all")]
    outcome = float(row["benchmark_excess_return_percent"])
    if outcome >= high:
        return "high"
    if outcome <= low:
        return "low"
    return "middle"


def benjamini_hochberg(p_values: dict[str, float]) -> dict[str, float]:
    """Return monotonic Benjamini-Hochberg adjusted p-values."""
    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    count = len(ordered)
    adjusted: dict[str, float] = {}
    running = 1.0
    for rank_index in range(count - 1, -1, -1):
        name, raw = ordered[rank_index]
        rank = rank_index + 1
        running = min(running, raw * count / rank)
        adjusted[name] = min(max(running, 0.0), 1.0)
    return adjusted


def _welch_normal_p_value(left: list[float], right: list[float]) -> float:
    if len(left) < 2 or len(right) < 2:
        return 1.0
    standard_error = math.sqrt(variance(left) / len(left) + variance(right) / len(right))
    if standard_error == 0:
        return 1.0 if fmean(left) == fmean(right) else 0.0
    z_score = abs(fmean(left) - fmean(right)) / standard_error
    return math.erfc(z_score / math.sqrt(2.0))


def _effect_size(high: list[float], low: list[float]) -> float | None:
    if len(high) < 2 or len(low) < 2:
        return None
    pooled_denominator = len(high) + len(low) - 2
    pooled_variance = ((len(high) - 1) * variance(high) + (len(low) - 1) * variance(low)) / pooled_denominator
    if pooled_variance <= 0:
        return 0.0
    return (fmean(high) - fmean(low)) / math.sqrt(pooled_variance)


def _stable_id(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:20]


def analyze_numeric_features(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Compare top/bottom excess-return quartiles without fitting a model."""
    rows = [row for row in records if isinstance(row.get("benchmark_excess_return_percent"), (int, float))]
    gate = sample_gate(len(rows))
    result: dict[str, Any] = {
        "mature_count": len(rows),
        "gate": gate.__dict__,
        "cohorts": {"high": 0, "middle": 0, "low": 0},
        "findings": [],
    }
    if not rows:
        return result
    cutoffs = market_cohort_cutoffs(rows)
    high_rows = [row for row in rows if cohort_of(row, cutoffs) == "high"]
    low_rows = [row for row in rows if cohort_of(row, cutoffs) == "low"]
    result["cohorts"] = {
        "high": len(high_rows),
        "middle": len(rows) - len(high_rows) - len(low_rows),
        "low": len(low_rows),
        "cutoffs_by_market": {market: {"low": low, "high": high} for market, (low, high) in cutoffs.items()},
    }
    if len(cutoffs) == 1:
        (low_cutoff, high_cutoff), = cutoffs.values()
        result["cohorts"].update({"high_cutoff": high_cutoff, "low_cutoff": low_cutoff})
    if not gate.allow_descriptive:
        return result

    feature_names = sorted({name for row in rows for name, value in (row.get("features") or {}).items() if isinstance(value, (int, float)) and not isinstance(value, bool)})
    provisional: list[dict[str, Any]] = []
    raw_p_values: dict[str, float] = {}
    for name in feature_names:
        high_values = [float(row["features"][name]) for row in high_rows if isinstance((row.get("features") or {}).get(name), (int, float))]
        low_values = [float(row["features"][name]) for row in low_rows if isinstance((row.get("features") or {}).get(name), (int, float))]
        available = [float(row["features"][name]) for row in rows if isinstance((row.get("features") or {}).get(name), (int, float))]
        if len(high_values) < 5 or len(low_values) < 5:
            continue
        raw_p = _welch_normal_p_value(high_values, low_values)
        raw_p_values[name] = raw_p
        payload = {
            "feature": name,
            "high_mean": fmean(high_values),
            "high_median": median(high_values),
            "low_mean": fmean(low_values),
            "low_median": median(low_values),
            "effect_size": _effect_size(high_values, low_values),
            "raw_p_value": raw_p,
            "available_count": len(available),
            "missing_count": len(rows) - len(available),
            "high_count": len(high_values),
            "low_count": len(low_values),
        }
        payload["finding_id"] = _stable_id(payload)
        provisional.append(payload)

    adjusted = benjamini_hochberg(raw_p_values)
    for finding in provisional:
        finding["adjusted_p_value"] = adjusted[finding["feature"]]
        finding["status"] = "exploratory"
    result["findings"] = sorted(
        provisional,
        key=lambda finding: (finding["adjusted_p_value"], -abs(finding["effect_size"] or 0), finding["feature"]),
    )
    return result
