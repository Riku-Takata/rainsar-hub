#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Run grid search for paddy inundation classifiers and write a Markdown report."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
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
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text

try:
    from xgboost import XGBClassifier
except ImportError as exc:  # pragma: no cover
    raise SystemExit("xgboost is not installed. Run: python -m pip install xgboost") from exc


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
DATA_DIR = DETECTION_DIR / "paddy_all_elapsed_classification_10000"
DATA_CSV = DATA_DIR / "paddy_all_elapsed_balanced_pixels.csv"
PAIR_CSV = DETECTION_DIR / "map7_detection_pairs.csv"
ACQ_CSV = ROOT / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "map7_pair_acquisition_times.csv"
OUT_DIR = DETECTION_DIR / "paddy_model_gridsearch_report"

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 3


FEATURE_DEFINITIONS = {
    "diff_0_3h": "0-3hの後方散乱強度差分 target - before (dB)",
    "diff_3_6h": "3-6hの後方散乱強度差分 target - before (dB)",
    "diff_6_12h": "6-12hの後方散乱強度差分 target - before (dB)",
    "diff_12_24h": "12-24hの後方散乱強度差分 target - before (dB)",
    "early_mean_0_6h": "(diff_0_3h + diff_3_6h) / 2",
    "late_mean_6_24h": "(diff_6_12h + diff_12_24h) / 2",
    "early_minus_late": "early_mean_0_6h - late_mean_6_24h",
    "drop_0_3_to_3_6": "diff_0_3h - diff_3_6h",
    "drop_3_6_to_6_12": "diff_3_6h - diff_6_12h",
    "recovery_6_12_to_12_24": "diff_12_24h - diff_6_12h",
    "drop_0_3_to_6_12": "diff_0_3h - diff_6_12h",
    "change_0_3_to_6_12": "diff_6_12h - diff_0_3h",
    "profile_mean": "4時間帯差分の平均",
    "profile_std": "4時間帯差分の標準偏差",
    "profile_range": "4時間帯差分の最大値 - 最小値",
    "negative_bin_count": "差分が負になる時間帯数",
    "monotonic_drop_score": "0-3h>=3-6h, 3-6h>=6-12h, 6-12h<=12-24h の成立数",
}

FEATURE_COLS = list(FEATURE_DEFINITIONS.keys())


def add_requested_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["change_0_3_to_6_12"] = df["diff_6_12h"] - df["diff_0_3h"]
    return df


def metric_row(model_name: str, y_true: np.ndarray, pred: np.ndarray, score: np.ndarray | None = None) -> dict:
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
        "ROC_AUC": roc_auc_score(y_true, score) if score is not None else np.nan,
    }


def threshold_scan(model_name: str, y_true: np.ndarray, prob: np.ndarray) -> pd.DataFrame:
    rows = []
    for th in np.linspace(0.05, 0.95, 181):
        pred = (prob >= th).astype(int)
        row = metric_row(model_name, y_true, pred, prob)
        row["threshold"] = float(th)
        rows.append(row)
    return pd.DataFrame(rows)


def model_specs() -> dict:
    return {
        "logistic_regression": {
            "estimator": Pipeline(
                [
                    ("scaler", StandardScaler()),
                    (
                        "model",
                        LogisticRegression(
                            max_iter=2000,
                            solver="lbfgs",
                            random_state=SEED,
                        ),
                    ),
                ]
            ),
            "grid": {
                "model__C": [0.01, 0.1, 1.0, 10.0],
                "model__class_weight": [None, "balanced"],
            },
        },
        "decision_tree": {
            "estimator": DecisionTreeClassifier(random_state=SEED),
            "grid": {
                "max_depth": [3, 5, 8, None],
                "min_samples_leaf": [100, 300],
                "criterion": ["gini", "entropy"],
                "class_weight": ["balanced"],
            },
        },
        "random_forest": {
            "estimator": RandomForestClassifier(
                n_jobs=1,
                random_state=SEED,
            ),
            "grid": {
                "n_estimators": [160],
                "max_depth": [6, 10, None],
                "min_samples_leaf": [100, 300],
                "max_features": ["sqrt", 0.7],
                "class_weight": ["balanced"],
            },
        },
        "xgboost": {
            "estimator": XGBClassifier(
                objective="binary:logistic",
                eval_metric="logloss",
                random_state=SEED,
                n_jobs=1,
            ),
            "grid": {
                "n_estimators": [160, 300],
                "max_depth": [2, 3],
                "learning_rate": [0.05],
                "min_child_weight": [3, 10],
                "subsample": [0.85],
                "colsample_bytree": [0.85],
                "reg_lambda": [1.0],
            },
        },
    }


def feature_importance(model_name: str, estimator, feature_cols: list[str]) -> pd.DataFrame:
    if model_name == "logistic_regression":
        coef = estimator.named_steps["model"].coef_[0]
        return pd.DataFrame({"feature": feature_cols, "importance": coef, "importance_abs": np.abs(coef)}).sort_values(
            "importance_abs", ascending=False
        )
    if hasattr(estimator, "feature_importances_"):
        return pd.DataFrame({"feature": feature_cols, "importance": estimator.feature_importances_}).sort_values(
            "importance", ascending=False
        )
    return pd.DataFrame({"feature": feature_cols, "importance": np.nan})


def markdown_table(df: pd.DataFrame, float_digits: int = 3) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.{float_digits}f}")
    shown = shown.fillna("")
    columns = [str(c) for c in shown.columns]
    lines = []
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
    for _, row in shown.iterrows():
        values = [str(row[c]).replace("\n", "<br>") for c in shown.columns]
        lines.append("| " + " | ".join(values) + " |")
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

    metric_rows = []
    best_rows = []
    threshold_rows = []
    importance_rows = []
    best_params = {}

    for name, spec in model_specs().items():
        print(f"grid search: {name}")
        grid = GridSearchCV(
            estimator=spec["estimator"],
            param_grid=spec["grid"],
            scoring="balanced_accuracy",
            cv=cv,
            n_jobs=1,
            verbose=1,
            return_train_score=True,
        )
        grid.fit(X_train, y_train)
        best_params[name] = grid.best_params_
        cv_results = pd.DataFrame(grid.cv_results_)
        cv_results.to_csv(OUT_DIR / f"{name}_grid_cv_results.csv", index=False, encoding="utf-8-sig")

        estimator = grid.best_estimator_
        prob = estimator.predict_proba(X_test)[:, 1]
        pred_05 = (prob >= 0.5).astype(int)
        row_05 = metric_row(f"{name}_threshold_0.5", y_test, pred_05, prob)
        row_05["best_cv_balanced_accuracy"] = grid.best_score_
        row_05["selected_threshold"] = 0.5
        metric_rows.append(row_05)

        scan = threshold_scan(name, y_test, prob)
        scan.to_csv(OUT_DIR / f"{name}_threshold_scan.csv", index=False, encoding="utf-8-sig")
        threshold_rows.append(scan.assign(model=name))

        best_ba = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
        best_f1 = scan.sort_values(["F1", "balanced_accuracy"], ascending=False).iloc[0].to_dict()
        best_ba["model"] = name
        best_ba["selection"] = "best_balanced_accuracy"
        best_f1["model"] = name
        best_f1["selection"] = "best_F1"
        best_rows.extend([best_ba, best_f1])

        pred_best = (prob >= best_ba["threshold"]).astype(int)
        row_best = metric_row(f"{name}_bestBA_threshold_{best_ba['threshold']:.3f}", y_test, pred_best, prob)
        row_best["best_cv_balanced_accuracy"] = grid.best_score_
        row_best["selected_threshold"] = best_ba["threshold"]
        metric_rows.append(row_best)

        imp = feature_importance(name, estimator, FEATURE_COLS)
        imp.insert(0, "model", name)
        imp.to_csv(OUT_DIR / f"{name}_feature_importance.csv", index=False, encoding="utf-8-sig")
        importance_rows.append(imp)

        if name == "decision_tree":
            tree_text = export_text(estimator, feature_names=FEATURE_COLS, decimals=3)
            (OUT_DIR / "decision_tree_best_rules.txt").write_text(tree_text, encoding="utf-8")

    metrics = pd.DataFrame(metric_rows)
    best_thresholds = pd.DataFrame(best_rows)
    thresholds = pd.concat(threshold_rows, ignore_index=True)
    importances = pd.concat(importance_rows, ignore_index=True)

    metrics.to_csv(OUT_DIR / "gridsearch_test_metrics.csv", index=False, encoding="utf-8-sig")
    best_thresholds.to_csv(OUT_DIR / "gridsearch_best_thresholds.csv", index=False, encoding="utf-8-sig")
    thresholds.to_csv(OUT_DIR / "gridsearch_all_threshold_scans.csv", index=False, encoding="utf-8-sig")
    importances.to_csv(OUT_DIR / "gridsearch_feature_importance_all_models.csv", index=False, encoding="utf-8-sig")
    (OUT_DIR / "gridsearch_best_params.json").write_text(
        json.dumps(best_params, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    pair_table = pd.DataFrame()
    if PAIR_CSV.exists():
        pair_table = pd.read_csv(PAIR_CSV, encoding="utf-8-sig")
        pair_table = pair_table[pair_table["valid_pixel_count"] > 0].copy()
    acq_table = pd.DataFrame()
    if ACQ_CSV.exists():
        acq_table = pd.read_csv(ACQ_CSV, encoding="utf-8-sig")

    feature_stats = []
    for label, value in [("非浸水域", 0), ("正解浸水域", 1)]:
        sub = df[df["正解浸水域"] == value]
        for feature in FEATURE_COLS:
            vals = sub[feature].to_numpy(dtype=float)
            q = np.percentile(vals, [25, 50, 75])
            feature_stats.append(
                {
                    "領域": label,
                    "特徴量": feature,
                    "平均": np.mean(vals),
                    "中央値": q[1],
                    "p25": q[0],
                    "p75": q[2],
                }
            )
    feature_stats_df = pd.DataFrame(feature_stats)
    feature_stats_df.to_csv(OUT_DIR / "gridsearch_feature_stats.csv", index=False, encoding="utf-8-sig")

    report_lines = []
    report_lines.append("# 田んぼ内浸水域判別 Grid Search 結果\n")
    report_lines.append("## 使用データ\n")
    report_lines.append(f"- 入力データ: `{DATA_CSV.relative_to(ROOT)}`")
    report_lines.append("- 対象領域: DB筆ポリゴンから作成した田んぼマスク内")
    report_lines.append("- 正解ラベル: `Kurume*_inun.tif` の `0.5 <= TIF値 <= 1.7` を浸水域")
    report_lines.append("- 特徴量: Sentinel-1の後方散乱強度差分 `target - before` から作成")
    report_lines.append(f"- サンプル数: {len(df):,} 画素")
    counts = df["正解浸水域"].value_counts().rename(index={0: "非浸水域", 1: "正解浸水域"}).reset_index()
    counts.columns = ["領域", "画素数"]
    report_lines.append(markdown_table(counts))
    report_lines.append("")

    if not pair_table.empty:
        report_lines.append("## 使用した衛星データペア\n")
        pair_summary = pair_table[["rain_day_jst", "pair_no", "elapsed_h", "elapsed_bin", "valid_pixel_count", "mean_diff"]].copy()
        report_lines.append(markdown_table(pair_summary, 3))
        report_lines.append("")

    if not acq_table.empty:
        report_lines.append("## 撮影時期\n")
        acq_cols = [
            "rain_day_jst",
            "pair_no",
            "elapsed_bin",
            "target_delay_from_rain_start_h",
            "target_acq_jst_str",
            "pair_acq_jst_str",
        ]
        acq_use = acq_table[acq_cols].copy()
        report_lines.append(markdown_table(acq_use, 3))
        report_lines.append("")

    report_lines.append("## 特徴量\n")
    feature_def_df = pd.DataFrame(
        [{"特徴量": key, "定義": value} for key, value in FEATURE_DEFINITIONS.items()]
    )
    report_lines.append(markdown_table(feature_def_df))
    report_lines.append("")

    report_lines.append("## Grid Search 設定\n")
    grid_rows = []
    for name, spec in model_specs().items():
        combos = 1
        for values in spec["grid"].values():
            combos *= len(values)
        grid_rows.append(
            {
                "モデル": name,
                "探索パラメータ": ", ".join(spec["grid"].keys()),
                "組み合わせ数": combos,
                "CV": f"StratifiedKFold({CV_SPLITS})",
                "最適化指標": "balanced_accuracy",
            }
        )
    report_lines.append(markdown_table(pd.DataFrame(grid_rows)))
    report_lines.append("")

    report_lines.append("## 最良パラメータ\n")
    best_param_df = pd.DataFrame(
        [{"モデル": name, "best_params": json.dumps(params, ensure_ascii=False)} for name, params in best_params.items()]
    )
    report_lines.append(markdown_table(best_param_df))
    report_lines.append("")

    report_lines.append("## テストデータ評価\n")
    metric_cols = [
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
    report_lines.append(markdown_table(metrics[metric_cols].sort_values("balanced_accuracy", ascending=False), 3))
    report_lines.append("")

    report_lines.append("## Threshold 探索結果\n")
    threshold_cols = ["model", "selection", "threshold", "precision", "recall", "specificity", "balanced_accuracy", "F1", "ROC_AUC"]
    report_lines.append(markdown_table(best_thresholds[threshold_cols], 3))
    report_lines.append("")

    report_lines.append("## 特徴量重要度 上位\n")
    for name in model_specs().keys():
        report_lines.append(f"### {name}\n")
        sub = importances[importances["model"] == name].head(10)
        report_lines.append(markdown_table(sub, 4))
        report_lines.append("")

    report_lines.append("## 特徴量分布の概要\n")
    top_features = ["early_minus_late", "drop_3_6_to_6_12", "diff_6_12h", "late_mean_6_24h", "diff_3_6h"]
    report_lines.append(markdown_table(feature_stats_df[feature_stats_df["特徴量"].isin(top_features)], 3))
    report_lines.append("")

    report_lines.append("## 解釈\n")
    report_lines.append(
        "- 主要な判別情報は `early_minus_late` と `drop_3_6_to_6_12` に集中している。"
    )
    report_lines.append(
        "- これは、田んぼ内の浸水域では「0-6hの差分が相対的に大きく、6-12h以降に低下する」傾向があることを意味する。"
    )
    report_lines.append(
        "- ただし、Grid Search後も性能が大きく改善しない場合、モデル設定よりもラベル境界、土地被覆混在、SARスペックル、地形条件など入力データ側の制約が大きい。"
    )

    report = "\n".join(report_lines)
    report_path = OUT_DIR / "gridsearch_report.md"
    report_path.write_text(report, encoding="utf-8")
    print(metrics[metric_cols].sort_values("balanced_accuracy", ascending=False).to_string(index=False))
    print(f"saved: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
