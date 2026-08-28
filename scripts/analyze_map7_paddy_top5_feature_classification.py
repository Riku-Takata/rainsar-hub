#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Re-evaluate paddy inundation classification using the top 5 features."""

from __future__ import annotations

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
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text


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
OUT_DIR = BASE_DIR / "top5_feature_recheck"
SEED = 42


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def metric_row(name: str, y_true: np.ndarray, pred: np.ndarray, score: np.ndarray | None = None) -> dict:
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
        "ROC_AUC": roc_auc_score(y_true, score) if score is not None else np.nan,
    }


def threshold_scan(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    y = df["正解浸水域"].to_numpy(bool)
    rows = []
    for feature in feature_cols:
        x = df[feature].to_numpy(float)
        for direction in [">=", "<="]:
            score = x if direction == ">=" else -x
            for th in np.unique(np.percentile(x, np.linspace(1, 99, 99))):
                pred = x >= th if direction == ">=" else x <= th
                row = metric_row(f"{feature} {direction} {th:.3f}", y, pred.astype(int), score)
                row.update({"特徴量": feature, "方向": direction, "閾値": float(th)})
                rows.append(row)
    return pd.DataFrame(rows)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plt = setup_matplotlib()

    df = pd.read_csv(BASE_DIR / "paddy_all_elapsed_balanced_pixels.csv", encoding="utf-8-sig")
    importance = pd.read_csv(BASE_DIR / "paddy_all_elapsed_random_forest_feature_importance.csv", encoding="utf-8-sig")
    top5 = importance.head(5)["特徴量"].tolist()

    selected = pd.DataFrame({"rank": range(1, len(top5) + 1), "特徴量": top5})
    selected.to_csv(OUT_DIR / "selected_top5_features.csv", index=False, encoding="utf-8-sig")

    # Single-feature baseline within the selected top 5.
    scan = threshold_scan(df, top5)
    best_single = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).groupby("特徴量").head(1)
    scan.to_csv(OUT_DIR / "top5_single_threshold_scan.csv", index=False, encoding="utf-8-sig")
    best_single.to_csv(OUT_DIR / "top5_best_single_thresholds.csv", index=False, encoding="utf-8-sig")

    X = df[top5].to_numpy(float)
    y = df["正解浸水域"].to_numpy(int)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=SEED, stratify=y)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    models = {
        "Top5_ロジスティック回帰": LogisticRegression(max_iter=1000, class_weight="balanced", random_state=SEED),
        "Top5_決定木_depth3": DecisionTreeClassifier(max_depth=3, min_samples_leaf=100, class_weight="balanced", random_state=SEED),
        "Top5_ランダムフォレスト": RandomForestClassifier(
            n_estimators=250,
            max_depth=8,
            min_samples_leaf=80,
            class_weight="balanced",
            n_jobs=1,
            random_state=SEED,
        ),
    }

    rows = []
    fitted = {}
    for name, model in models.items():
        if "ロジスティック" in name:
            model.fit(X_train_scaled, y_train)
            prob = model.predict_proba(X_test_scaled)[:, 1]
        else:
            model.fit(X_train, y_train)
            prob = model.predict_proba(X_test)[:, 1]
        pred = (prob >= 0.5).astype(int)
        rows.append(metric_row(name, y_test, pred, prob))
        fitted[name] = model
    metrics = pd.DataFrame(rows).sort_values("balanced_accuracy", ascending=False)
    metrics.to_csv(OUT_DIR / "top5_model_metrics.csv", index=False, encoding="utf-8-sig")

    rf = fitted["Top5_ランダムフォレスト"]
    rf_imp = pd.DataFrame({"特徴量": top5, "重要度_top5モデル": rf.feature_importances_}).sort_values("重要度_top5モデル", ascending=False)
    rf_imp.to_csv(OUT_DIR / "top5_random_forest_feature_importance.csv", index=False, encoding="utf-8-sig")

    logi = fitted["Top5_ロジスティック回帰"]
    coef = pd.DataFrame({"特徴量": top5, "標準化係数": logi.coef_[0]}).sort_values("標準化係数", ascending=False)
    coef.to_csv(OUT_DIR / "top5_logistic_coefficients.csv", index=False, encoding="utf-8-sig")

    tree_text = export_text(fitted["Top5_決定木_depth3"], feature_names=top5, decimals=3)
    (OUT_DIR / "top5_decision_tree_rules.txt").write_text(tree_text, encoding="utf-8")

    # Compare with previous all-feature model metrics.
    prev = pd.read_csv(BASE_DIR / "paddy_all_elapsed_model_metrics.csv", encoding="utf-8-sig")
    compare = pd.concat([prev.assign(特徴量セット="全特徴量"), metrics.assign(特徴量セット="Top5")], ignore_index=True)
    compare.to_csv(OUT_DIR / "top5_vs_all_feature_model_metrics.csv", index=False, encoding="utf-8-sig")

    colors = {"正解浸水域": "#d62728", "非浸水域": "#4c78a8"}
    fig, axes = plt.subplots(2, 3, figsize=(13.5, 7.2), dpi=180)
    for ax, feature in zip(axes.ravel(), top5):
        a = df[df["正解浸水域"] == 1][feature].to_numpy()
        b = df[df["正解浸水域"] == 0][feature].to_numpy()
        lo, hi = np.percentile(np.concatenate([a, b]), [1, 99])
        bins = np.linspace(lo, hi, 50)
        ax.hist(b, bins=bins, density=True, alpha=0.45, color=colors["非浸水域"], label="非浸水域")
        ax.hist(a, bins=bins, density=True, alpha=0.45, color=colors["正解浸水域"], label="正解浸水域")
        ax.set_title(feature, fontsize=9)
        ax.grid(True, axis="y", alpha=0.25)
    axes.ravel()[-1].axis("off")
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("田んぼ内: Top5特徴量の分布", y=1.02)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_top5_feature_histograms.png", bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.0, 4.8), dpi=180)
    plot = rf_imp.sort_values("重要度_top5モデル")
    ax.barh(plot["特徴量"], plot["重要度_top5モデル"], color="#4c78a8")
    ax.set_title("Top5ランダムフォレストの特徴量重要度")
    ax.set_xlabel("重要度")
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_top5_random_forest_importance.png")
    plt.close(fig)

    print("selected")
    print(selected.to_string(index=False))
    print("best single")
    print(best_single[["特徴量", "方向", "閾値", "precision", "recall", "specificity", "balanced_accuracy", "F1"]].to_string(index=False))
    print("metrics")
    print(metrics.to_string(index=False))
    print("compare")
    print(compare[["特徴量セット", "手法", "precision", "recall", "specificity", "balanced_accuracy", "F1", "ROC_AUC"]].to_string(index=False))
    print("tree")
    print(tree_text)
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
