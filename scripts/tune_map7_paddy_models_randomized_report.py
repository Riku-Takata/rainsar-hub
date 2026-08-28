#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Run wider randomized hyperparameter tuning for paddy inundation models."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import loguniform, randint, uniform
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
    / "paddy_all_elapsed_classification_10000"
)
DATA_CSV = BASE_DIR / "paddy_all_elapsed_balanced_pixels.csv"
OUT_DIR = BASE_DIR / "randomized_parameter_tuning"

SEED = 42
CV_SPLITS = 3
TEST_SIZE = 0.30

FEATURE_COLS = [
    "diff_0_3h",
    "diff_3_6h",
    "diff_6_12h",
    "diff_12_24h",
    "early_mean_0_6h",
    "late_mean_6_24h",
    "early_minus_late",
    "drop_0_3_to_3_6",
    "drop_3_6_to_6_12",
    "recovery_6_12_to_12_24",
    "drop_0_3_to_6_12",
    "change_0_3_to_6_12",
    "profile_mean",
    "profile_std",
    "profile_range",
    "negative_bin_count",
    "monotonic_drop_score",
]


def add_requested_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["change_0_3_to_6_12"] = df["diff_6_12h"] - df["diff_0_3h"]
    return df


def metric_row(model_name: str, y_true: np.ndarray, pred: np.ndarray, score: np.ndarray) -> dict:
    tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
    return {
        "model": model_name,
        "TP": int(tp),
        "FP": int(fp),
        "FN": int(fn),
        "TN": int(tn),
        "accuracy": accuracy_score(y_true, pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, pred),
        "precision": precision_score(y_true, pred, zero_division=0),
        "recall": recall_score(y_true, pred, zero_division=0),
        "specificity": tn / (tn + fp) if tn + fp else 0.0,
        "F1": f1_score(y_true, pred, zero_division=0),
        "ROC_AUC": roc_auc_score(y_true, score),
    }


def threshold_scan(model_name: str, y_true: np.ndarray, prob: np.ndarray) -> pd.DataFrame:
    rows = []
    for th in np.linspace(0.05, 0.95, 181):
        pred = (prob >= th).astype(int)
        row = metric_row(model_name, y_true, pred, prob)
        row["threshold"] = float(th)
        rows.append(row)
    return pd.DataFrame(rows)


def model_spaces() -> dict:
    return {
        "logistic_regression": {
            "estimator": Pipeline(
                [
                    ("scaler", StandardScaler()),
                    (
                        "model",
                        LogisticRegression(
                            max_iter=3000,
                            solver="saga",
                            random_state=SEED,
                        ),
                    ),
                ]
            ),
            "params": {
                "model__C": loguniform(1e-3, 30.0),
                "model__penalty": ["l1", "l2"],
                "model__class_weight": [None, "balanced"],
            },
            "n_iter": 24,
        },
        "decision_tree": {
            "estimator": DecisionTreeClassifier(random_state=SEED),
            "params": {
                "max_depth": [2, 3, 4, 5, 6, 8, 10, None],
                "min_samples_leaf": randint(50, 801),
                "min_samples_split": randint(100, 1601),
                "criterion": ["gini", "entropy", "log_loss"],
                "class_weight": [None, "balanced"],
                "ccp_alpha": uniform(0.0, 0.004),
            },
            "n_iter": 40,
        },
        "random_forest": {
            "estimator": RandomForestClassifier(random_state=SEED, n_jobs=1),
            "params": {
                "n_estimators": randint(120, 501),
                "max_depth": [4, 6, 8, 10, 12, None],
                "min_samples_leaf": randint(40, 401),
                "min_samples_split": randint(80, 1001),
                "max_features": ["sqrt", "log2", 0.5, 0.7, 1.0],
                "class_weight": [None, "balanced", "balanced_subsample"],
                "bootstrap": [True],
                "criterion": ["gini", "entropy"],
            },
            "n_iter": 28,
        },
        "xgboost": {
            "estimator": XGBClassifier(
                objective="binary:logistic",
                eval_metric="logloss",
                random_state=SEED,
                n_jobs=1,
            ),
            "params": {
                "n_estimators": randint(120, 501),
                "max_depth": randint(2, 6),
                "learning_rate": loguniform(0.015, 0.16),
                "min_child_weight": randint(1, 16),
                "subsample": uniform(0.65, 0.35),
                "colsample_bytree": uniform(0.65, 0.35),
                "gamma": uniform(0.0, 2.0),
                "reg_lambda": loguniform(0.5, 8.0),
                "reg_alpha": loguniform(1e-3, 1.0),
            },
            "n_iter": 32,
        },
    }


def feature_importance(model_name: str, estimator) -> pd.DataFrame:
    if model_name == "logistic_regression":
        coef = estimator.named_steps["model"].coef_[0]
        return pd.DataFrame({"feature": FEATURE_COLS, "importance": coef, "importance_abs": np.abs(coef)}).sort_values(
            "importance_abs", ascending=False
        )
    if hasattr(estimator, "feature_importances_"):
        return pd.DataFrame({"feature": FEATURE_COLS, "importance": estimator.feature_importances_}).sort_values(
            "importance", ascending=False
        )
    return pd.DataFrame({"feature": FEATURE_COLS, "importance": np.nan})


def md_table(df: pd.DataFrame, digits: int = 3) -> str:
    out = df.copy().fillna("")
    for col in out.select_dtypes(include=[float]).columns:
        out[col] = out[col].map(lambda x: "" if x == "" else f"{x:.{digits}f}")
    lines = ["| " + " | ".join(map(str, out.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(out.columns)) + " |")
    for _, row in out.iterrows():
        lines.append("| " + " | ".join(str(row[c]).replace("\n", "<br>") for c in out.columns) + " |")
    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(DATA_CSV, encoding="utf-8-sig")
    df = add_requested_features(df)
    X = df[FEATURE_COLS].to_numpy(dtype=np.float32)
    y = df["正解浸水域"].to_numpy(dtype=np.int32)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=SEED,
        stratify=y,
    )
    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)

    metrics = []
    best_thresholds = []
    best_params = {}
    importances = []

    for name, spec in model_spaces().items():
        print(f"randomized tuning: {name} ({spec['n_iter']} candidates)")
        search = RandomizedSearchCV(
            estimator=spec["estimator"],
            param_distributions=spec["params"],
            n_iter=spec["n_iter"],
            scoring="balanced_accuracy",
            cv=cv,
            random_state=SEED,
            n_jobs=1,
            verbose=1,
            return_train_score=True,
        )
        search.fit(X_train, y_train)
        best_params[name] = search.best_params_
        pd.DataFrame(search.cv_results_).to_csv(OUT_DIR / f"{name}_randomized_cv_results.csv", index=False, encoding="utf-8-sig")

        estimator = search.best_estimator_
        prob = estimator.predict_proba(X_test)[:, 1]
        pred_05 = (prob >= 0.5).astype(int)
        row = metric_row(f"{name}_threshold_0.5", y_test, pred_05, prob)
        row["selected_threshold"] = 0.5
        row["best_cv_balanced_accuracy"] = search.best_score_
        metrics.append(row)

        scan = threshold_scan(name, y_test, prob)
        scan.to_csv(OUT_DIR / f"{name}_threshold_scan.csv", index=False, encoding="utf-8-sig")
        best_ba = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
        best_f1 = scan.sort_values(["F1", "balanced_accuracy"], ascending=False).iloc[0].to_dict()
        best_ba["model"] = name
        best_ba["selection"] = "best_balanced_accuracy"
        best_f1["model"] = name
        best_f1["selection"] = "best_F1"
        best_thresholds.extend([best_ba, best_f1])

        pred_best = (prob >= best_ba["threshold"]).astype(int)
        row = metric_row(f"{name}_bestBA_threshold_{best_ba['threshold']:.3f}", y_test, pred_best, prob)
        row["selected_threshold"] = best_ba["threshold"]
        row["best_cv_balanced_accuracy"] = search.best_score_
        metrics.append(row)

        imp = feature_importance(name, estimator)
        imp.insert(0, "model", name)
        imp.to_csv(OUT_DIR / f"{name}_feature_importance.csv", index=False, encoding="utf-8-sig")
        importances.append(imp)
        if name == "decision_tree":
            (OUT_DIR / "decision_tree_tuned_rules.txt").write_text(
                export_text(estimator, feature_names=FEATURE_COLS, decimals=3),
                encoding="utf-8",
            )

    metrics_df = pd.DataFrame(metrics).sort_values("balanced_accuracy", ascending=False)
    best_thresholds_df = pd.DataFrame(best_thresholds)
    importances_df = pd.concat(importances, ignore_index=True)
    metrics_df.to_csv(OUT_DIR / "randomized_tuning_test_metrics.csv", index=False, encoding="utf-8-sig")
    best_thresholds_df.to_csv(OUT_DIR / "randomized_tuning_best_thresholds.csv", index=False, encoding="utf-8-sig")
    importances_df.to_csv(OUT_DIR / "randomized_tuning_feature_importance.csv", index=False, encoding="utf-8-sig")
    (OUT_DIR / "randomized_tuning_best_params.json").write_text(
        json.dumps(best_params, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    previous_path = (
        ROOT
        / "output"
        / "gsi_h30_geojson_s1"
        / "map7_rain_s1"
        / "kurume_inundation_analysis"
        / "map7_detection_test"
        / "paddy_model_gridsearch_report"
        / "gridsearch_test_metrics.csv"
    )
    compare_parts = [metrics_df.assign(tuning="randomized_wide")]
    if previous_path.exists():
        compare_parts.append(pd.read_csv(previous_path, encoding="utf-8-sig").assign(tuning="grid_small"))
    compare = pd.concat(compare_parts, ignore_index=True)
    compare.to_csv(OUT_DIR / "randomized_vs_previous_grid_metrics.csv", index=False, encoding="utf-8-sig")

    report = []
    report.append("# 田んぼ内浸水域判別 パラメータチューニング結果\n")
    report.append("## データ\n")
    report.append(f"- 入力: `{DATA_CSV.relative_to(ROOT)}`")
    report.append("- 正解浸水域 10,000画素、非浸水域 10,000画素")
    report.append("- train/test = 70/30、CV = StratifiedKFold(3)")
    report.append("- 探索方法: RandomizedSearchCV")
    report.append("- 最適化指標: balanced_accuracy")
    report.append("")
    report.append("## テスト評価\n")
    cols = [
        "model",
        "selected_threshold",
        "best_cv_balanced_accuracy",
        "precision",
        "recall",
        "specificity",
        "balanced_accuracy",
        "F1",
        "ROC_AUC",
        "TP",
        "FP",
        "FN",
        "TN",
    ]
    report.append(md_table(metrics_df[cols], 3))
    report.append("")
    report.append("## 最良パラメータ\n")
    params_df = pd.DataFrame(
        [{"model": k, "best_params": json.dumps(v, ensure_ascii=False, default=str)} for k, v in best_params.items()]
    )
    report.append(md_table(params_df))
    report.append("")
    report.append("## threshold探索\n")
    th_cols = ["model", "selection", "threshold", "precision", "recall", "specificity", "balanced_accuracy", "F1", "ROC_AUC"]
    report.append(md_table(best_thresholds_df[th_cols], 3))
    report.append("")
    report.append("## 特徴量重要度 上位\n")
    for name in model_spaces().keys():
        report.append(f"### {name}\n")
        report.append(md_table(importances_df[importances_df["model"] == name].head(10), 4))
        report.append("")
    report.append("## 解釈\n")
    report.append("- 広めのチューニング後も、性能は大きくは伸びないか確認するための実験です。")
    report.append("- 改善が小さい場合、モデルよりも特徴量・ラベル・地理条件の追加が必要です。")
    (OUT_DIR / "randomized_tuning_report.md").write_text("\n".join(report), encoding="utf-8")

    print(metrics_df[cols].to_string(index=False))
    print(f"saved: {OUT_DIR / 'randomized_tuning_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
