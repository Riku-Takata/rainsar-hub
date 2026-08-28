#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Evaluate paddy inundation classification with XGBoost."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
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
OUT_DIR = BASE_DIR / "xgboost_recheck"
SEED = 42


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def metric_row(name: str, y_true: np.ndarray, pred: np.ndarray, score: np.ndarray) -> dict:
    tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
    return {
        "手法": name,
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


def threshold_scan(y_true: np.ndarray, prob: np.ndarray) -> pd.DataFrame:
    rows = []
    for th in np.linspace(0.05, 0.95, 181):
        pred = prob >= th
        row = metric_row(f"threshold_{th:.3f}", y_true, pred.astype(int), prob)
        row["threshold"] = float(th)
        rows.append(row)
    return pd.DataFrame(rows)


def fit_xgb(X_train, y_train) -> XGBClassifier:
    model = XGBClassifier(
        n_estimators=400,
        max_depth=3,
        learning_rate=0.035,
        subsample=0.85,
        colsample_bytree=0.85,
        min_child_weight=5,
        reg_lambda=2.0,
        reg_alpha=0.05,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=SEED,
        n_jobs=1,
    )
    model.fit(X_train, y_train)
    return model


def evaluate_feature_set(df: pd.DataFrame, feature_cols: list[str], label: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    X = df[feature_cols].to_numpy(dtype=np.float32)
    y = df["正解浸水域"].to_numpy(dtype=np.int32)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=SEED, stratify=y)
    model = fit_xgb(X_train, y_train)
    prob = model.predict_proba(X_test)[:, 1]
    pred = (prob >= 0.5).astype(int)

    metrics = pd.DataFrame([metric_row(f"XGBoost_{label}_threshold0.5", y_test, pred, prob)])
    scan = threshold_scan(y_test, prob)
    best_ba = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).head(1).copy()
    best_f1 = scan.sort_values(["F1", "balanced_accuracy"], ascending=False).head(1).copy()
    best_ba["選択基準"] = "balanced_accuracy最大"
    best_f1["選択基準"] = "F1最大"
    best = pd.concat([best_ba, best_f1], ignore_index=True)
    best["特徴量セット"] = label

    importance = pd.DataFrame(
        {
            "特徴量セット": label,
            "特徴量": feature_cols,
            "重要度_gain相当": model.feature_importances_,
        }
    ).sort_values("重要度_gain相当", ascending=False)

    # Recompute metrics at best balanced-accuracy threshold with a descriptive name.
    best_th = float(best_ba.iloc[0]["threshold"])
    best_pred = (prob >= best_th).astype(int)
    best_metric = pd.DataFrame([metric_row(f"XGBoost_{label}_bestBA_threshold{best_th:.3f}", y_test, best_pred, prob)])
    metrics = pd.concat([metrics, best_metric], ignore_index=True)

    pred_rows = pd.DataFrame(
        {
            "特徴量セット": label,
            "y_true": y_test,
            "prob_inundated": prob,
            "pred_threshold0p5": pred,
            "pred_bestBA": best_pred,
        }
    )
    return metrics, best, importance, pred_rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plt = setup_matplotlib()

    df = pd.read_csv(BASE_DIR / "paddy_all_elapsed_balanced_pixels.csv", encoding="utf-8-sig")
    all_features = [
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
        "profile_mean",
        "profile_std",
        "profile_range",
        "negative_bin_count",
        "monotonic_drop_score",
    ]
    top5 = pd.read_csv(BASE_DIR / "top5_feature_recheck" / "selected_top5_features.csv", encoding="utf-8-sig")[
        "特徴量"
    ].tolist()

    outputs = []
    bests = []
    importances = []
    preds = []
    for label, features in [("all_features", all_features), ("top5_features", top5)]:
        metrics, best, importance, pred_rows = evaluate_feature_set(df, features, label)
        outputs.append(metrics)
        bests.append(best)
        importances.append(importance)
        preds.append(pred_rows)

    metrics_df = pd.concat(outputs, ignore_index=True)
    best_df = pd.concat(bests, ignore_index=True)
    importance_df = pd.concat(importances, ignore_index=True)
    pred_df = pd.concat(preds, ignore_index=True)

    prev_all = pd.read_csv(BASE_DIR / "paddy_all_elapsed_model_metrics.csv", encoding="utf-8-sig").assign(
        特徴量セット="全特徴量_既存"
    )
    prev_top5 = pd.read_csv(BASE_DIR / "top5_feature_recheck" / "top5_model_metrics.csv", encoding="utf-8-sig").assign(
        特徴量セット="Top5_既存"
    )
    compare = pd.concat([prev_all, prev_top5, metrics_df.assign(特徴量セット="XGBoost")], ignore_index=True)

    metrics_df.to_csv(OUT_DIR / "xgboost_model_metrics.csv", index=False, encoding="utf-8-sig")
    best_df.to_csv(OUT_DIR / "xgboost_best_thresholds.csv", index=False, encoding="utf-8-sig")
    importance_df.to_csv(OUT_DIR / "xgboost_feature_importance.csv", index=False, encoding="utf-8-sig")
    pred_df.to_csv(OUT_DIR / "xgboost_test_predictions.csv", index=False, encoding="utf-8-sig")
    compare.to_csv(OUT_DIR / "xgboost_vs_existing_model_metrics.csv", index=False, encoding="utf-8-sig")

    fig, ax = plt.subplots(figsize=(8.2, 5.0), dpi=180)
    plot = importance_df[importance_df["特徴量セット"] == "all_features"].head(10).sort_values("重要度_gain相当")
    ax.barh(plot["特徴量"], plot["重要度_gain相当"], color="#4c78a8")
    ax.set_title("XGBoost 特徴量重要度（全特徴量）")
    ax.set_xlabel("重要度")
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_xgboost_feature_importance_all.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7.6, 4.8), dpi=180)
    for label, color in [("all_features", "#4c78a8"), ("top5_features", "#d62728")]:
        sub = pred_df[pred_df["特徴量セット"] == label]
        ax.hist(sub[sub["y_true"] == 0]["prob_inundated"], bins=40, alpha=0.42, density=True, color=color, label=f"{label} 非浸水")
        ax.hist(sub[sub["y_true"] == 1]["prob_inundated"], bins=40, alpha=0.42, density=True, histtype="step", linewidth=1.8, color=color, label=f"{label} 浸水")
    ax.set_title("XGBoostの浸水確率分布")
    ax.set_xlabel("推定浸水確率")
    ax.set_ylabel("密度")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_xgboost_probability_distribution.png")
    plt.close(fig)

    print("metrics")
    print(metrics_df.to_string(index=False))
    print("best thresholds")
    print(best_df[["特徴量セット", "選択基準", "threshold", "precision", "recall", "specificity", "balanced_accuracy", "F1", "ROC_AUC"]].to_string(index=False))
    print("importance")
    print(importance_df.groupby("特徴量セット").head(10).to_string(index=False))
    print("compare")
    print(compare[["特徴量セット", "手法", "precision", "recall", "specificity", "balanced_accuracy", "F1", "ROC_AUC"]].to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
