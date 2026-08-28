#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Run Kurume/map7 paddy classification using 5,000 characteristic pixels per class."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
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
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
OUT_DIR = DETECTION_DIR / "paddy_characteristic_5000_model_report"
SEED = 42
N_PER_CLASS = 5000
TEST_SIZE = 0.30
CV_SPLITS = 3
LABEL_COL = "正解浸水域"

FEATURE_DEFINITIONS = {
    "diff_0_3h": "降雨開始後0-3hの平均差分 target - before (dB)",
    "diff_3_6h": "降雨開始後3-6hの平均差分 target - before (dB)",
    "diff_6_12h": "降雨開始後6-12hの平均差分 target - before (dB)",
    "diff_12_24h": "降雨開始後12-24hの平均差分 target - before (dB)",
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
CHARACTERISTIC_FEATURES = [
    "early_minus_late",
    "drop_3_6_to_6_12",
    "late_mean_6_24h",
    "diff_6_12h",
    "early_mean_0_6h",
    "drop_0_3_to_6_12",
    "change_0_3_to_6_12",
]


def read_float(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def read_bool(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1) > 0


def build_feature_frame() -> pd.DataFrame:
    d0 = read_float(DETECTION_DIR / "map7_mean_diff_0_3h.tif")
    d3 = read_float(DETECTION_DIR / "map7_mean_diff_3_6h.tif")
    d6 = read_float(DETECTION_DIR / "map7_mean_diff_6_12h.tif")
    d12 = read_float(DETECTION_DIR / "map7_mean_diff_12_24h.tif")
    truth = read_bool(DETECTION_DIR / "map7_inundation_truth_mask.tif")
    paddy = read_bool(DETECTION_DIR / "landmask_filter" / "map7_paddy_mask.tif")
    valid = np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12) & paddy

    row, col = np.where(valid)
    df = pd.DataFrame(
        {
            "row": row,
            "col": col,
            LABEL_COL: truth[valid].astype(np.uint8),
            "diff_0_3h": d0[valid],
            "diff_3_6h": d3[valid],
            "diff_6_12h": d6[valid],
            "diff_12_24h": d12[valid],
        }
    )
    df["early_mean_0_6h"] = (df["diff_0_3h"] + df["diff_3_6h"]) / 2.0
    df["late_mean_6_24h"] = (df["diff_6_12h"] + df["diff_12_24h"]) / 2.0
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df["diff_0_3h"] - df["diff_3_6h"]
    df["drop_3_6_to_6_12"] = df["diff_3_6h"] - df["diff_6_12h"]
    df["recovery_6_12_to_12_24"] = df["diff_12_24h"] - df["diff_6_12h"]
    df["drop_0_3_to_6_12"] = df["diff_0_3h"] - df["diff_6_12h"]
    df["change_0_3_to_6_12"] = df["diff_6_12h"] - df["diff_0_3h"]
    profile = df[["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]].to_numpy(np.float32)
    df["profile_mean"] = profile.mean(axis=1)
    df["profile_std"] = profile.std(axis=1)
    df["profile_range"] = profile.max(axis=1) - profile.min(axis=1)
    df["negative_bin_count"] = (profile < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h"] >= df["diff_3_6h"]).astype(int)
        + (df["diff_3_6h"] >= df["diff_6_12h"]).astype(int)
        + (df["diff_6_12h"] <= df["diff_12_24h"]).astype(int)
    )
    return df


def robust_z_distance(values: pd.DataFrame, median: pd.Series, iqr: pd.Series) -> np.ndarray:
    scale = iqr.replace(0, np.nan).fillna(iqr[iqr > 0].median())
    z = (values - median) / scale
    return np.sqrt(np.nanmean(np.square(z.to_numpy(dtype=np.float64)), axis=1))


def characteristic_sample(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    meta = []
    for label in [1, 0]:
        own = df[df[LABEL_COL] == label].copy()
        other = df[df[LABEL_COL] != label].copy()
        q25 = own[CHARACTERISTIC_FEATURES].quantile(0.25)
        q50 = own[CHARACTERISTIC_FEATURES].quantile(0.50)
        q75 = own[CHARACTERISTIC_FEATURES].quantile(0.75)
        other_q50 = other[CHARACTERISTIC_FEATURES].quantile(0.50)
        iqr = q75 - q25
        own_dist = robust_z_distance(own[CHARACTERISTIC_FEATURES], q50, iqr)
        other_dist = robust_z_distance(own[CHARACTERISTIC_FEATURES], other_q50, iqr)
        own["class_typicality_distance"] = own_dist
        own["opposite_distance"] = other_dist
        own["characteristic_score"] = own["opposite_distance"] - own["class_typicality_distance"]
        selected = own.sort_values(
            ["class_typicality_distance", "characteristic_score"],
            ascending=[True, False],
        ).head(N_PER_CLASS)
        rows.append(selected)
        meta.append(
            {
                "label": int(label),
                "available_pixels": int(len(own)),
                "selected_pixels": int(len(selected)),
                "median_class_typicality_distance": float(selected["class_typicality_distance"].median()),
                "median_characteristic_score": float(selected["characteristic_score"].median()),
            }
        )
    sampled = pd.concat(rows, ignore_index=True).sample(frac=1.0, random_state=SEED).reset_index(drop=True)
    return sampled, pd.DataFrame(meta)


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


def md_table(df: pd.DataFrame, digits: int = 3) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.{digits}f}")
    shown = shown.fillna("")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]).replace("\n", "<br>") for c in shown.columns) + " |")
    return "\n".join(lines)


def model_specs_grid() -> dict:
    return {
        "logistic_regression": {
            "estimator": Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, solver="lbfgs", random_state=SEED))]),
            "grid": {"model__C": [0.01, 0.1, 1.0, 10.0], "model__class_weight": [None, "balanced"]},
        },
        "decision_tree": {
            "estimator": DecisionTreeClassifier(random_state=SEED),
            "grid": {"max_depth": [3, 5, 8, None], "min_samples_leaf": [50, 100, 200], "criterion": ["gini", "entropy"], "class_weight": [None, "balanced"]},
        },
        "random_forest": {
            "estimator": RandomForestClassifier(n_jobs=1, random_state=SEED),
            "grid": {"n_estimators": [160, 300], "max_depth": [6, 10, None], "min_samples_leaf": [50, 100, 200], "max_features": ["sqrt", 0.7], "class_weight": [None, "balanced"]},
        },
        "xgboost": {
            "estimator": XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            "grid": {"n_estimators": [160, 300], "max_depth": [2, 3], "learning_rate": [0.03, 0.05], "min_child_weight": [3, 10], "subsample": [0.85], "colsample_bytree": [0.85], "reg_lambda": [1.0]},
        },
    }


def model_specs_randomized() -> dict:
    return {
        "logistic_regression": {
            "estimator": Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, solver="saga", random_state=SEED))]),
            "params": {"model__C": loguniform(1e-3, 30.0), "model__penalty": ["l1", "l2"], "model__class_weight": [None, "balanced"]},
            "n_iter": 20,
        },
        "decision_tree": {
            "estimator": DecisionTreeClassifier(random_state=SEED),
            "params": {"max_depth": [2, 3, 4, 5, 6, 8, 10, None], "min_samples_leaf": randint(30, 401), "min_samples_split": randint(60, 801), "criterion": ["gini", "entropy", "log_loss"], "class_weight": [None, "balanced"], "ccp_alpha": uniform(0.0, 0.004)},
            "n_iter": 28,
        },
        "random_forest": {
            "estimator": RandomForestClassifier(random_state=SEED, n_jobs=1),
            "params": {"n_estimators": randint(120, 401), "max_depth": [4, 6, 8, 10, 12, None], "min_samples_leaf": randint(30, 251), "min_samples_split": randint(60, 701), "max_features": ["sqrt", "log2", 0.5, 0.7, 1.0], "class_weight": [None, "balanced", "balanced_subsample"], "bootstrap": [True], "criterion": ["gini", "entropy"]},
            "n_iter": 24,
        },
        "xgboost": {
            "estimator": XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            "params": {"n_estimators": randint(120, 401), "max_depth": randint(2, 6), "learning_rate": loguniform(0.015, 0.16), "min_child_weight": randint(1, 16), "subsample": uniform(0.65, 0.35), "colsample_bytree": uniform(0.65, 0.35), "gamma": uniform(0.0, 2.0), "reg_lambda": loguniform(0.5, 8.0), "reg_alpha": loguniform(1e-3, 1.0)},
            "n_iter": 28,
        },
    }


def feature_importance(model_name: str, estimator) -> pd.DataFrame:
    if model_name == "logistic_regression":
        coef = estimator.named_steps["model"].coef_[0]
        return pd.DataFrame({"feature": FEATURE_COLS, "importance": coef, "importance_abs": np.abs(coef)}).sort_values("importance_abs", ascending=False)
    if hasattr(estimator, "feature_importances_"):
        return pd.DataFrame({"feature": FEATURE_COLS, "importance": estimator.feature_importances_}).sort_values("importance", ascending=False)
    return pd.DataFrame({"feature": FEATURE_COLS, "importance": np.nan})


def run_searches(sampled: pd.DataFrame, mode: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    X = sampled[FEATURE_COLS].to_numpy(dtype=np.float32)
    y = sampled[LABEL_COL].to_numpy(dtype=np.int32)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE, random_state=SEED, stratify=y)
    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)
    specs = model_specs_grid() if mode == "grid" else model_specs_randomized()
    metrics, thresholds, importances, best_params = [], [], [], {}
    for name, spec in specs.items():
        print(f"{mode} tuning: {name}")
        if mode == "grid":
            search = GridSearchCV(spec["estimator"], spec["grid"], scoring="balanced_accuracy", cv=cv, n_jobs=1, verbose=1, return_train_score=True)
        else:
            search = RandomizedSearchCV(spec["estimator"], spec["params"], n_iter=spec["n_iter"], scoring="balanced_accuracy", cv=cv, n_jobs=1, verbose=1, random_state=SEED, return_train_score=True)
        search.fit(X_train, y_train)
        best_params[name] = search.best_params_
        pd.DataFrame(search.cv_results_).to_csv(OUT_DIR / f"{mode}_{name}_cv_results.csv", index=False, encoding="utf-8-sig")
        estimator = search.best_estimator_
        prob = estimator.predict_proba(X_test)[:, 1]
        pred05 = (prob >= 0.5).astype(int)
        row = metric_row(f"{name}_threshold_0.5", y_test, pred05, prob)
        row["selected_threshold"] = 0.5
        row["best_cv_balanced_accuracy"] = search.best_score_
        metrics.append(row)
        scan = threshold_scan(name, y_test, prob)
        scan.to_csv(OUT_DIR / f"{mode}_{name}_threshold_scan.csv", index=False, encoding="utf-8-sig")
        best = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
        best["model"] = name
        best["selection"] = "best_balanced_accuracy"
        thresholds.append(best)
        pred_best = (prob >= best["threshold"]).astype(int)
        row = metric_row(f"{name}_bestBA_threshold_{best['threshold']:.3f}", y_test, pred_best, prob)
        row["selected_threshold"] = best["threshold"]
        row["best_cv_balanced_accuracy"] = search.best_score_
        metrics.append(row)
        imp = feature_importance(name, estimator)
        imp.insert(0, "model", name)
        imp.insert(0, "search", mode)
        importances.append(imp)
        imp.to_csv(OUT_DIR / f"{mode}_{name}_feature_importance.csv", index=False, encoding="utf-8-sig")
        if name == "decision_tree":
            (OUT_DIR / f"{mode}_decision_tree_rules.txt").write_text(export_text(estimator, feature_names=FEATURE_COLS, decimals=3), encoding="utf-8")
    metrics_df = pd.DataFrame(metrics).sort_values("balanced_accuracy", ascending=False)
    thresholds_df = pd.DataFrame(thresholds)
    importances_df = pd.concat(importances, ignore_index=True)
    metrics_df.to_csv(OUT_DIR / f"{mode}_test_metrics.csv", index=False, encoding="utf-8-sig")
    thresholds_df.to_csv(OUT_DIR / f"{mode}_best_thresholds.csv", index=False, encoding="utf-8-sig")
    importances_df.to_csv(OUT_DIR / f"{mode}_feature_importance.csv", index=False, encoding="utf-8-sig")
    (OUT_DIR / f"{mode}_best_params.json").write_text(json.dumps(best_params, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return metrics_df, thresholds_df, importances_df, best_params


def summarize_features(df: pd.DataFrame, sampled: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for source, source_df in [("all_paddy_valid", df), ("characteristic_5000_each", sampled)]:
        for label, group in source_df.groupby(LABEL_COL):
            zone = "inundated_truth" if int(label) == 1 else "non_inundated"
            for feature in FEATURE_COLS + ["class_typicality_distance", "characteristic_score"]:
                if feature not in group:
                    continue
                vals = group[feature].to_numpy(np.float64)
                q = np.nanpercentile(vals, [5, 25, 50, 75, 95])
                rows.append(
                    {
                        "source": source,
                        "zone": zone,
                        "feature": feature,
                        "count": int(np.isfinite(vals).sum()),
                        "mean": float(np.nanmean(vals)),
                        "std": float(np.nanstd(vals)),
                        "p05": float(q[0]),
                        "p25": float(q[1]),
                        "median": float(q[2]),
                        "p75": float(q[3]),
                        "p95": float(q[4]),
                    }
                )
    return pd.DataFrame(rows)


def plot_profile(stats: pd.DataFrame) -> None:
    sub = stats[(stats["source"] == "characteristic_5000_each") & (stats["feature"].isin(["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]))]
    features = ["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]
    labels = ["0-3h", "3-6h", "6-12h", "12-24h"]
    fig, ax = plt.subplots(figsize=(7, 4))
    for zone, color in [("inundated_truth", "tab:red"), ("non_inundated", "tab:blue")]:
        z = sub[sub["zone"] == zone].set_index("feature").loc[features]
        ax.plot(labels, z["mean"], marker="o", color=color, label=zone)
        ax.fill_between(labels, z["p25"], z["p75"], color=color, alpha=0.15)
    ax.axhline(0, color="black", lw=0.8)
    ax.set_ylabel("target - before (dB)")
    ax.set_title("Kurume paddy characteristic 5,000 pixels per class")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "kurume_paddy_characteristic_5000_diff_profile.png", dpi=160)
    plt.close(fig)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    full_df = build_feature_frame()
    sampled, sample_meta = characteristic_sample(full_df)
    full_counts = full_df[LABEL_COL].value_counts().rename(index={0: "non_inundated", 1: "inundated_truth"}).reset_index()
    full_counts.columns = ["label", "pixels"]
    sampled.to_csv(OUT_DIR / "kurume_paddy_characteristic_5000_pixels.csv", index=False, encoding="utf-8-sig")
    sample_meta.to_csv(OUT_DIR / "kurume_paddy_characteristic_5000_sampling_summary.csv", index=False, encoding="utf-8-sig")
    stats = summarize_features(full_df, sampled)
    stats.to_csv(OUT_DIR / "kurume_paddy_characteristic_5000_feature_stats.csv", index=False, encoding="utf-8-sig")
    plot_profile(stats)
    grid_metrics, _, grid_imp, grid_params = run_searches(sampled, "grid")
    random_metrics, _, random_imp, random_params = run_searches(sampled, "randomized")
    all_metrics = pd.concat([grid_metrics.assign(search="grid"), random_metrics.assign(search="randomized")], ignore_index=True)
    all_importance = pd.concat([grid_imp, random_imp], ignore_index=True)
    all_metrics.to_csv(OUT_DIR / "kurume_paddy_characteristic_5000_test_metrics.csv", index=False, encoding="utf-8-sig")
    all_importance.to_csv(OUT_DIR / "kurume_paddy_characteristic_5000_feature_importance.csv", index=False, encoding="utf-8-sig")
    previous = DETECTION_DIR / "paddy_model_gridsearch_report" / "gridsearch_test_metrics.csv"
    compare = [all_metrics.assign(dataset="characteristic_5000_each")]
    if previous.exists():
        compare.append(pd.read_csv(previous, encoding="utf-8-sig").assign(search="previous_grid", dataset="random_10000_each"))
    pd.concat(compare, ignore_index=True).to_csv(OUT_DIR / "kurume_paddy_5000_vs_10000_metrics.csv", index=False, encoding="utf-8-sig")

    metric_cols = ["search", "model", "selected_threshold", "best_cv_balanced_accuracy", "precision", "recall", "specificity", "balanced_accuracy", "F1", "ROC_AUC", "TP", "FP", "FN", "TN"]
    report = []
    report.append("# Kurume/map7 田んぼ内 特徴的5000画素モデルレポート\n")
    report.append("## 抽出方法\n")
    report.append("- 対象: DB筆ポリゴンから作成済みの田んぼマスク内")
    report.append("- 正解浸水域・非浸水域それぞれから 5,000 画素を抽出")
    report.append("- 抽出基準: 主要特徴量について、各クラスの中央値に近い画素を優先")
    report.append("- 主要特徴量: `" + "`, `".join(CHARACTERISTIC_FEATURES) + "`")
    report.append("- 注意: クラスラベルを使って代表画素を選ぶため、全画素への汎化性能ではなく「特徴が見えやすい画素に限定した場合」の検証")
    report.append("")
    report.append("## 画素数\n")
    report.append(md_table(full_counts))
    report.append("")
    report.append("## 抽出サマリー\n")
    report.append(md_table(sample_meta, 3))
    report.append("")
    report.append("## GridSearchCV / RandomizedSearchCV 結果\n")
    report.append(md_table(all_metrics[metric_cols].sort_values("balanced_accuracy", ascending=False), 3))
    report.append("")
    report.append("## GridSearchCV 最良パラメータ\n")
    report.append(md_table(pd.DataFrame([{"model": k, "best_params": json.dumps(v, ensure_ascii=False)} for k, v in grid_params.items()])))
    report.append("")
    report.append("## RandomizedSearchCV 最良パラメータ\n")
    report.append(md_table(pd.DataFrame([{"model": k, "best_params": json.dumps(v, ensure_ascii=False, default=str)} for k, v in random_params.items()])))
    report.append("")
    report.append("## 特徴量分布概要\n")
    show_features = ["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h", "early_minus_late", "drop_3_6_to_6_12", "change_0_3_to_6_12"]
    report.append(md_table(stats[(stats["source"] == "characteristic_5000_each") & (stats["feature"].isin(show_features))], 3))
    report.append("")
    report.append("## 図\n")
    report.append("- `kurume_paddy_characteristic_5000_diff_profile.png`: 特徴的5000画素の経過時間別差分プロファイル")
    report.append("")
    report.append("## 解釈\n")
    report.append("- 精度が上がる場合、外れ値や土地被覆内のばらつきが減り、浸水域らしい時系列変化が強く出る画素に限定されたことを示す。")
    report.append("- ただし、ラベルに基づく代表画素抽出なので、実運用上の検出精度としては過大評価になりやすい。")
    (OUT_DIR / "kurume_paddy_characteristic_5000_model_report.md").write_text("\n".join(report), encoding="utf-8")
    print(all_metrics[metric_cols].sort_values("balanced_accuracy", ascending=False).to_string(index=False))
    print(f"saved: {OUT_DIR / 'kurume_paddy_characteristic_5000_model_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
