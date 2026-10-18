"""Regularized point-in-time models with chronological validation."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Iterable

import numpy as np
from scipy.stats import spearmanr

from .analysis import cohort_of, market_cohort_cutoffs, sample_gate
from .validation import calibration_summary, chronological_partitions, event_date


def _model_dependencies():
    try:
        from sklearn.compose import ColumnTransformer
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, roc_auc_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        from sklearn.tree import DecisionTreeClassifier, export_text
    except ImportError as exc:
        raise RuntimeError("scikit-learn is required for validated Insights models") from exc
    return {
        "ColumnTransformer": ColumnTransformer,
        "HistGradientBoostingClassifier": HistGradientBoostingClassifier,
        "SimpleImputer": SimpleImputer,
        "LogisticRegression": LogisticRegression,
        "accuracy_score": accuracy_score,
        "roc_auc_score": roc_auc_score,
        "Pipeline": Pipeline,
        "StandardScaler": StandardScaler,
        "DecisionTreeClassifier": DecisionTreeClassifier,
        "export_text": export_text,
    }


def _dataset(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Every mature appraisal, labelled 1 when it reached its market's top quartile.

    Training on all appraisals (not just the extremes) means a model score is
    an estimate for an ordinary new pick, so calibration can be checked
    honestly against the real base rate.
    """
    rows = [row for row in records if isinstance(row.get("benchmark_excess_return_percent"), (int, float))]
    if not rows:
        return {"rows": [], "features": [], "x": [], "y": [], "dates": [], "excess": []}
    cutoffs = market_cohort_cutoffs(rows)
    labels = [1 if cohort_of(row, cutoffs) == "high" else 0 for row in rows]
    feature_names = sorted({name for row in rows for name, value in (row.get("features") or {}).items() if isinstance(value, (int, float)) and not isinstance(value, bool)})
    minimum_coverage = max(10, round(len(rows) * 0.6))
    feature_names = [name for name in feature_names if sum(isinstance((row.get("features") or {}).get(name), (int, float)) for row in rows) >= minimum_coverage]
    return {
        "rows": rows,
        "features": feature_names,
        "x": [[(row.get("features") or {}).get(name) for name in feature_names] for row in rows],
        "y": labels,
        "dates": [event_date(row["event_at_utc"]) for row in rows],
        "excess": [float(row["benchmark_excess_return_percent"]) for row in rows],
        "cutoffs_by_market": {market: {"low": low, "high": high} for market, (low, high) in cutoffs.items()},
    }


def _ranking_metrics(scores: list[float], excess: list[float]) -> dict[str, Any]:
    """How useful out-of-sample scores were for choosing picks."""
    if len(scores) < 5:
        return {"information_coefficient": None, "top_fifth_excess": None, "all_excess": None, "top_fifth_lift": None}
    score_array, excess_array = np.array(scores), np.array(excess)
    coefficient = spearmanr(score_array, excess_array).statistic if len(set(scores)) > 1 else None
    top = excess_array[score_array >= np.percentile(score_array, 80)]
    return {
        "information_coefficient": float(coefficient) if coefficient is not None and np.isfinite(coefficient) else None,
        "top_fifth_excess": float(top.mean()) if len(top) else None,
        "all_excess": float(excess_array.mean()),
        "top_fifth_lift": float(top.mean() - excess_array.mean()) if len(top) else None,
    }


def run_validated_models(records: Iterable[dict[str, Any]], *, horizon_days: int) -> dict[str, Any]:
    records_list = list(records)
    gate = sample_gate(len(records_list))
    if not gate.allow_regularized_models:
        return {"enabled": False, "reason": gate.message, "gate": gate.__dict__}
    data = _dataset(records_list)
    if len(data["rows"]) < 30 or not data["features"]:
        return {"enabled": False, "reason": "Too few observations with usable features", "gate": gate.__dict__}

    deps = _model_dependencies()
    partitions = chronological_partitions(data["dates"], horizon_days=horizon_days)
    if len(partitions["folds"]) < 2 or len(partitions["holdout_indices"]) < 5:
        return {"enabled": False, "reason": "Insufficient distinct appraisal dates for walk-forward validation", "gate": gate.__dict__}

    def logistic_pipeline():
        return deps["Pipeline"]([
            ("imputer", deps["SimpleImputer"](strategy="median")),
            ("scaler", deps["StandardScaler"]()),
            # Unweighted so scores stay on the real base-rate scale.
            ("model", deps["LogisticRegression"](C=0.5, max_iter=2000, random_state=17)),
        ])

    scores: list[float] = []
    labels: list[int] = []
    validation_excess: list[float] = []
    fold_metrics: list[dict[str, Any]] = []
    coefficient_signs: dict[str, list[int]] = defaultdict(list)
    for fold in partitions["folds"]:
        train, validation = fold["train_indices"], fold["validation_indices"]
        y_train = [data["y"][index] for index in train]
        y_validation = [data["y"][index] for index in validation]
        if len(set(y_train)) < 2 or len(set(y_validation)) < 2:
            continue
        model = logistic_pipeline()
        model.fit([data["x"][index] for index in train], y_train)
        fold_scores = model.predict_proba([data["x"][index] for index in validation])[:, 1].tolist()
        predictions = [1 if score >= 0.5 else 0 for score in fold_scores]
        auc = deps["roc_auc_score"](y_validation, fold_scores)
        fold_metrics.append({
            "train_count": len(train), "validation_count": len(validation),
            "train_end": fold["train_end"].isoformat(),
            "validation_start": fold["validation_start"].isoformat(),
            "validation_end": fold["validation_end"].isoformat(),
            "auc": auc,
            "accuracy": deps["accuracy_score"](y_validation, predictions),
            **_ranking_metrics(fold_scores, [data["excess"][index] for index in validation]),
        })
        coefficients = model.named_steps["model"].coef_[0]
        for name, coefficient in zip(data["features"], coefficients):
            coefficient_signs[name].append(1 if coefficient > 0 else -1 if coefficient < 0 else 0)
        scores.extend(fold_scores)
        labels.extend(y_validation)
        validation_excess.extend(data["excess"][index] for index in validation)

    if len(fold_metrics) < 2:
        return {"enabled": False, "reason": "Walk-forward folds did not contain both outcome classes", "gate": gate.__dict__}

    development = partitions["development_indices"]
    holdout = partitions["holdout_indices"]
    if len(development) < 20 or len({data["y"][index] for index in development}) < 2:
        return {"enabled": False, "reason": "Too few appraisals finished before the holdout period to train a final model", "gate": gate.__dict__}
    final_model = logistic_pipeline()
    final_model.fit([data["x"][index] for index in development], [data["y"][index] for index in development])
    holdout_labels = [data["y"][index] for index in holdout]
    holdout_scores = final_model.predict_proba([data["x"][index] for index in holdout])[:, 1].tolist()
    holdout_auc = deps["roc_auc_score"](holdout_labels, holdout_scores) if len(set(holdout_labels)) == 2 else None
    holdout_ranking = _ranking_metrics(holdout_scores, [data["excess"][index] for index in holdout])
    calibration = calibration_summary(labels, scores)
    coefficients = final_model.named_steps["model"].coef_[0]
    features = []
    for name, coefficient in sorted(zip(data["features"], coefficients), key=lambda item: abs(item[1]), reverse=True):
        signs = coefficient_signs.get(name, [])
        stability = max(signs.count(1), signs.count(-1)) / len(signs) if signs else 0
        features.append({"feature": name, "coefficient": float(coefficient), "direction_stability": stability})

    tree = deps["Pipeline"]([
        ("imputer", deps["SimpleImputer"](strategy="median")),
        ("model", deps["DecisionTreeClassifier"](max_depth=3, min_samples_leaf=max(5, len(development) // 10), class_weight="balanced", random_state=17)),
    ])
    tree.fit([data["x"][index] for index in development], [data["y"][index] for index in development])
    tree_rules = deps["export_text"](tree.named_steps["model"], feature_names=data["features"], decimals=4)

    boosting: dict[str, Any] | None = None
    if gate.allow_boosting:
        boosted = deps["Pipeline"]([
            ("imputer", deps["SimpleImputer"](strategy="median")),
            ("model", deps["HistGradientBoostingClassifier"](max_depth=3, max_iter=100, learning_rate=0.05, l2_regularization=1.0, random_state=17)),
        ])
        boosted.fit([data["x"][index] for index in development], [data["y"][index] for index in development])
        boosted_scores = boosted.predict_proba([data["x"][index] for index in holdout])[:, 1].tolist()
        boosting = {"holdout_auc": deps["roc_auc_score"](holdout_labels, boosted_scores) if len(set(holdout_labels)) == 2 else None}

    calibrated = bool(calibration["calibrated"] and holdout_auc is not None and holdout_auc > 0.5)
    return {
        "enabled": True,
        "output_name": "estimated_probability" if calibrated else "score",
        "calibrated": calibrated,
        "gate": gate.__dict__,
        "record_count": len(data["rows"]),
        "feature_count": len(data["features"]),
        "outcome_cutoffs": data["cutoffs_by_market"],
        "folds": fold_metrics,
        "walk_forward_auc_mean": sum(fold["auc"] for fold in fold_metrics) / len(fold_metrics),
        "walk_forward_ranking": _ranking_metrics(scores, validation_excess),
        "base_rate": sum(data["y"]) / len(data["y"]),
        "holdout": {
            "start": partitions["holdout_start"].isoformat(),
            "count": len(holdout),
            "training_count": len(development),
            "auc": holdout_auc,
            **holdout_ranking,
        },
        "calibration": calibration,
        "features": features,
        "tree_rules": tree_rules,
        "boosting": boosting,
    }
