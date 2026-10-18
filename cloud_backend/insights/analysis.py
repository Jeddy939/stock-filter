"""Leakage-resistant descriptive analysis for mature appraisal snapshots."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Iterable

import numpy as np
from scipy.stats import rankdata


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


BOOTSTRAP_SAMPLES = 1000
BOOTSTRAP_SEED = 17
MINIMUM_FEATURE_ROWS = 20
CANDIDATE_ADJUSTED_P = 0.10
CHRONOLOGICAL_BLOCKS = 3
MINIMUM_BLOCK_ROWS = 10


def _stable_id(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:20]


def _within_market_ranks(values: np.ndarray, markets: np.ndarray) -> np.ndarray:
    """Percentile ranks (0-1, ties averaged) computed separately per market."""
    ranks = np.empty(len(values), dtype=float)
    for market in np.unique(markets):
        selected = markets == market
        count = int(selected.sum())
        ranks[selected] = (rankdata(values[selected]) - 0.5) / count if count else 0.5
    return ranks


def _pearson(x: np.ndarray, y: np.ndarray) -> float | None:
    if len(x) < 3:
        return None
    x_centered, y_centered = x - x.mean(), y - y.mean()
    denominator = math.sqrt(float((x_centered ** 2).sum() * (y_centered ** 2).sum()))
    return float((x_centered * y_centered).sum() / denominator) if denominator else None


def _clustered_bootstrap(
    x: np.ndarray,
    y: np.ndarray,
    clusters: np.ndarray,
    rng: np.random.Generator,
    samples: int,
) -> np.ndarray:
    """Rank-correlation draws from resampling whole clusters (signal weeks).

    Picks from the same week share market conditions, so resampling weeks
    rather than picks keeps the uncertainty honest.
    """
    cluster_ids, cluster_index = np.unique(clusters, return_inverse=True)
    counts = rng.multinomial(len(cluster_ids), np.full(len(cluster_ids), 1.0 / len(cluster_ids)), size=samples)
    weights = counts[:, cluster_index].astype(float)  # samples x rows
    totals = weights.sum(axis=1, keepdims=True)
    mean_x = (weights * x).sum(axis=1, keepdims=True) / totals
    mean_y = (weights * y).sum(axis=1, keepdims=True) / totals
    dx, dy = x - mean_x, y - mean_y
    covariance = (weights * dx * dy).sum(axis=1)
    spread = np.sqrt((weights * dx * dx).sum(axis=1) * (weights * dy * dy).sum(axis=1))
    with np.errstate(invalid="ignore", divide="ignore"):
        draws = covariance / spread
    return draws[np.isfinite(draws)]


def _quintiles(feature_ranks: np.ndarray, outcomes: np.ndarray) -> list[dict[str, Any]]:
    bins = np.minimum((feature_ranks * 5).astype(int), 4)
    table = []
    for quintile in range(5):
        selected = outcomes[bins == quintile]
        table.append({
            "quintile": quintile + 1,
            "count": int(len(selected)),
            "mean_excess": float(selected.mean()) if len(selected) else None,
            "median_excess": float(np.median(selected)) if len(selected) else None,
        })
    return table


def _block_correlations(x: np.ndarray, y: np.ndarray, order: np.ndarray) -> list[float | None]:
    """Rank correlation within each chronological third of the sample."""
    blocks = np.array_split(order, CHRONOLOGICAL_BLOCKS)
    return [_pearson(x[block], y[block]) if len(block) >= MINIMUM_BLOCK_ROWS else None for block in blocks]


def _largest_contributor(x: np.ndarray, y: np.ndarray, tickers: np.ndarray, rho: float) -> dict[str, Any] | None:
    """The ticker whose removal moves the correlation most towards zero."""
    best: dict[str, Any] | None = None
    for ticker in np.unique(tickers):
        keep = tickers != ticker
        if keep.sum() < 3:
            continue
        without = _pearson(x[keep], y[keep])
        if without is None:
            continue
        shift = abs(rho) - (without if rho >= 0 else -without)
        if best is None or shift > best["shift"]:
            best = {"ticker": str(ticker), "rho_without": without, "shift": shift}
    return best


def analyze_numeric_features(
    records: Iterable[dict[str, Any]],
    *,
    bootstrap_samples: int = BOOTSTRAP_SAMPLES,
    seed: int = BOOTSTRAP_SEED,
) -> dict[str, Any]:
    """Rank-based feature screening over every mature appraisal.

    Each feature is rank-correlated with benchmark-excess return, both ranked
    within market. Uncertainty comes from a bootstrap over signal weeks, and a
    finding becomes a candidate only when it survives multiple-testing
    correction, holds in most chronological periods, and does not rest on a
    single ticker. Top/bottom quartile means are kept for interpretation.
    """
    rows = [row for row in records if isinstance(row.get("benchmark_excess_return_percent"), (int, float))]
    gate = sample_gate(len(rows))
    result: dict[str, Any] = {
        "mature_count": len(rows),
        "gate": gate.__dict__,
        "cohorts": {"high": 0, "middle": 0, "low": 0},
        "method": "within-market Spearman rank correlation; signal-week cluster bootstrap; Benjamini-Hochberg",
        "findings": [],
    }
    if not rows:
        return result
    cutoffs = market_cohort_cutoffs(rows)
    cohorts = [cohort_of(row, cutoffs) for row in rows]
    result["cohorts"] = {
        "high": cohorts.count("high"),
        "middle": cohorts.count("middle"),
        "low": cohorts.count("low"),
        "cutoffs_by_market": {market: {"low": low, "high": high} for market, (low, high) in cutoffs.items()},
    }
    if len(cutoffs) == 1:
        (low_cutoff, high_cutoff), = cutoffs.values()
        result["cohorts"].update({"high_cutoff": high_cutoff, "low_cutoff": low_cutoff})
    if not gate.allow_descriptive:
        return result

    markets_all = np.array([str(row.get("market") or "all") for row in rows])
    outcomes_all = np.array([float(row["benchmark_excess_return_percent"]) for row in rows])
    clusters_all = np.array([str(row.get("cluster") or row.get("event_at_utc") or index) for index, row in enumerate(rows)])
    tickers_all = np.array([str(row.get("ticker") or index) for index, row in enumerate(rows)])
    dates_all = np.array([str(row.get("event_at_utc") or "") for row in rows])
    cohorts_all = np.array(cohorts)
    rng = np.random.default_rng(seed)

    feature_names = sorted({
        name for row in rows for name, value in (row.get("features") or {}).items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    })
    provisional: list[dict[str, Any]] = []
    raw_p_values: dict[str, float] = {}
    for name in feature_names:
        present = np.array([
            isinstance((row.get("features") or {}).get(name), (int, float))
            and not isinstance((row.get("features") or {}).get(name), bool)
            and math.isfinite(float(row["features"][name]))
            for row in rows
        ])
        if present.sum() < MINIMUM_FEATURE_ROWS:
            continue
        values = np.array([float(row["features"][name]) for row, keep in zip(rows, present) if keep])
        markets = markets_all[present]
        outcomes = outcomes_all[present]
        feature_ranks = _within_market_ranks(values, markets)
        outcome_ranks = _within_market_ranks(outcomes, markets)
        rho = _pearson(feature_ranks, outcome_ranks)
        if rho is None:
            continue
        draws = _clustered_bootstrap(feature_ranks, outcome_ranks, clusters_all[present], rng, bootstrap_samples)
        if len(draws) < bootstrap_samples // 2:
            continue
        below, above = int((draws <= 0).sum()), int((draws >= 0).sum())
        raw_p = min(1.0, (2 * min(below, above) + 1) / (len(draws) + 1))
        interval = (float(np.percentile(draws, 2.5)), float(np.percentile(draws, 97.5)))
        order = np.argsort(dates_all[present], kind="stable")
        blocks = _block_correlations(feature_ranks, outcome_ranks, order)
        agreeing_blocks = sum(1 for block in blocks if block is not None and block * rho > 0)
        contributor = _largest_contributor(feature_ranks, outcome_ranks, tickers_all[present], rho)
        high_values = values[cohorts_all[present] == "high"]
        low_values = values[cohorts_all[present] == "low"]
        quintiles = _quintiles(feature_ranks, outcomes)
        top, bottom = quintiles[-1]["mean_excess"], quintiles[0]["mean_excess"]
        payload = {
            "feature": name,
            "spearman_rho": rho,
            "effect_size": rho,
            "rho_ci_low": interval[0],
            "rho_ci_high": interval[1],
            "raw_p_value": raw_p,
            "available_count": int(present.sum()),
            "missing_count": int(len(rows) - present.sum()),
            "cluster_count": int(len(np.unique(clusters_all[present]))),
            "quintiles": quintiles,
            "top_minus_bottom_quintile_excess": (top - bottom) if top is not None and bottom is not None else None,
            "block_rhos": blocks,
            "agreeing_blocks": agreeing_blocks,
            "largest_contributor": contributor,
            "high_count": int(len(high_values)),
            "low_count": int(len(low_values)),
            "high_mean": float(high_values.mean()) if len(high_values) else None,
            "high_median": float(np.median(high_values)) if len(high_values) else None,
            "low_mean": float(low_values.mean()) if len(low_values) else None,
            "low_median": float(np.median(low_values)) if len(low_values) else None,
        }
        payload["finding_id"] = _stable_id({key: payload[key] for key in ("feature", "spearman_rho", "available_count")})
        raw_p_values[name] = raw_p
        provisional.append(payload)

    adjusted = benjamini_hochberg(raw_p_values)
    for finding in provisional:
        finding["adjusted_p_value"] = adjusted[finding["feature"]]
        contributor = finding["largest_contributor"]
        reasons = []
        if finding["adjusted_p_value"] >= CANDIDATE_ADJUSTED_P:
            reasons.append(f"adjusted p is not below {CANDIDATE_ADJUSTED_P}")
        if finding["rho_ci_low"] <= 0 <= finding["rho_ci_high"]:
            reasons.append("confidence interval includes zero")
        if finding["agreeing_blocks"] < 2:
            reasons.append("direction not repeated in at least two time periods")
        if contributor and contributor["rho_without"] * finding["spearman_rho"] <= 0:
            reasons.append(f"relationship disappears without {contributor['ticker']}")
        finding["status"] = "candidate" if not reasons else "exploratory"
        finding["status_reasons"] = reasons
    result["findings"] = sorted(
        provisional,
        key=lambda finding: (finding["adjusted_p_value"], -abs(finding["spearman_rho"]), finding["feature"]),
    )
    return result
