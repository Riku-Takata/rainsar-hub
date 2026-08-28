#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Classify paddy inundation using all elapsed-time backscatter-difference features."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
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
DETECTION_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
OUT_DIR = DETECTION_DIR / "paddy_all_elapsed_classification"
SEED = 42
N_PER_CLASS = 10000


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


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

    y_idx, x_idx = np.where(valid)
    v0 = d0[valid]
    v3 = d3[valid]
    v6 = d6[valid]
    v12 = d12[valid]
    stack = np.vstack([v0, v3, v6, v12]).T
    df = pd.DataFrame(
        {
            "row": y_idx,
            "col": x_idx,
            "正解浸水域": truth[valid].astype(np.uint8),
            "diff_0_3h": v0,
            "diff_3_6h": v3,
            "diff_6_12h": v6,
            "diff_12_24h": v12,
        }
    )
    df["early_mean_0_6h"] = (df["diff_0_3h"] + df["diff_3_6h"]) / 2.0
    df["late_mean_6_24h"] = (df["diff_6_12h"] + df["diff_12_24h"]) / 2.0
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df["diff_0_3h"] - df["diff_3_6h"]
    df["drop_3_6_to_6_12"] = df["diff_3_6h"] - df["diff_6_12h"]
    df["recovery_6_12_to_12_24"] = df["diff_12_24h"] - df["diff_6_12h"]
    df["drop_0_3_to_6_12"] = df["diff_0_3h"] - df["diff_6_12h"]
    df["profile_mean"] = np.nanmean(stack, axis=1)
    df["profile_std"] = np.nanstd(stack, axis=1)
    df["profile_range"] = np.nanmax(stack, axis=1) - np.nanmin(stack, axis=1)
    df["negative_bin_count"] = np.sum(stack < 0, axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h"] >= df["diff_3_6h"]).astype(int)
        + (df["diff_3_6h"] >= df["diff_6_12h"]).astype(int)
        + (df["diff_6_12h"] <= df["diff_12_24h"]).astype(int)
    )
    return df


def balanced_sample(df: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    pos = df[df["正解浸水域"] == 1]
    neg = df[df["正解浸水域"] == 0]
    n = min(N_PER_CLASS, len(pos), len(neg))
    pos_idx = rng.choice(pos.index.to_numpy(), size=n, replace=False)
    neg_idx = rng.choice(neg.index.to_numpy(), size=n, replace=False)
    out = pd.concat([pos.loc[pos_idx], neg.loc[neg_idx]], ignore_index=True)
    return out.sample(frac=1.0, random_state=SEED).reset_index(drop=True)


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


def threshold_scan(df: pd.DataFrame, feature_cols: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    auc_rows = []
    y = df["正解浸水域"].to_numpy(bool)
    for feature in feature_cols:
        x = df[feature].to_numpy(float)
        auc = roc_auc_score(y, x)
        auc_rows.append(
            {
                "特徴量": feature,
                "AUC": auc,
                "分離力_AUC大きい側": max(auc, 1.0 - auc),
                "浸水域が大きい方向": ">=" if auc >= 0.5 else "<=",
            }
        )
        for direction in [">=", "<="]:
            for th in np.unique(np.percentile(x, np.linspace(1, 99, 99))):
                pred = x >= th if direction == ">=" else x <= th
                m = metric_row(f"{feature} {direction} {th:.3f}", y, pred.astype(int), x if direction == ">=" else -x)
                m.update({"特徴量": feature, "方向": direction, "閾値": float(th)})
                rows.append(m)
    return pd.DataFrame(auc_rows).sort_values("分離力_AUC大きい側", ascending=False), pd.DataFrame(rows)


def main() -> int:
    output_dir = OUT_DIR.parent / f"{OUT_DIR.name}_10000"
    output_dir.mkdir(parents=True, exist_ok=True)
    plt = setup_matplotlib()
    df = build_feature_frame()
    balanced = balanced_sample(df)
    balanced.to_csv(output_dir / "paddy_all_elapsed_balanced_pixels.csv", index=False, encoding="utf-8-sig")

    feature_cols = [
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

    stats_rows = []
    for zone_name, sub in [("田んぼ内_正解浸水域", balanced[balanced["正解浸水域"] == 1]), ("田んぼ内_非浸水域", balanced[balanced["正解浸水域"] == 0])]:
        for feature in feature_cols:
            vals = sub[feature].to_numpy(float)
            q = np.percentile(vals, [5, 25, 50, 75, 95])
            stats_rows.append(
                {
                    "領域": zone_name,
                    "特徴量": feature,
                    "画素数": int(vals.size),
                    "平均": float(np.mean(vals)),
                    "標準偏差": float(np.std(vals)),
                    "p05": float(q[0]),
                    "p25": float(q[1]),
                    "中央値": float(q[2]),
                    "p75": float(q[3]),
                    "p95": float(q[4]),
                }
            )
    stats_df = pd.DataFrame(stats_rows)
    stats_df.to_csv(output_dir / "paddy_all_elapsed_feature_stats_balanced.csv", index=False, encoding="utf-8-sig")

    auc_df, scan_df = threshold_scan(balanced, feature_cols)
    auc_df.to_csv(output_dir / "paddy_all_elapsed_feature_auc.csv", index=False, encoding="utf-8-sig")
    scan_df.to_csv(output_dir / "paddy_all_elapsed_single_threshold_scan.csv", index=False, encoding="utf-8-sig")
    scan_df.sort_values(["balanced_accuracy", "F1"], ascending=False).groupby("特徴量").head(1).to_csv(
        output_dir / "paddy_all_elapsed_best_single_thresholds.csv", index=False, encoding="utf-8-sig"
    )

    X = balanced[feature_cols].to_numpy(float)
    y = balanced["正解浸水域"].to_numpy(int)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=SEED, stratify=y)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    models = {}
    models["ロジスティック回帰"] = LogisticRegression(max_iter=1000, class_weight="balanced", random_state=SEED)
    models["決定木_depth3"] = DecisionTreeClassifier(max_depth=3, min_samples_leaf=200, class_weight="balanced", random_state=SEED)
    models["ランダムフォレスト"] = RandomForestClassifier(
        n_estimators=250,
        max_depth=8,
        min_samples_leaf=100,
        class_weight="balanced",
        n_jobs=1,
        random_state=SEED,
    )

    metric_rows = []
    fitted = {}
    for name, model in models.items():
        if name == "ロジスティック回帰":
            model.fit(X_train_scaled, y_train)
            prob = model.predict_proba(X_test_scaled)[:, 1]
        else:
            model.fit(X_train, y_train)
            prob = model.predict_proba(X_test)[:, 1]
        pred = (prob >= 0.5).astype(int)
        metric_rows.append(metric_row(name, y_test, pred, prob))
        fitted[name] = model
    metrics = pd.DataFrame(metric_rows).sort_values("balanced_accuracy", ascending=False)
    metrics.to_csv(output_dir / "paddy_all_elapsed_model_metrics.csv", index=False, encoding="utf-8-sig")

    rf = fitted["ランダムフォレスト"]
    importance = pd.DataFrame({"特徴量": feature_cols, "重要度": rf.feature_importances_}).sort_values("重要度", ascending=False)
    importance.to_csv(output_dir / "paddy_all_elapsed_random_forest_feature_importance.csv", index=False, encoding="utf-8-sig")

    logi = fitted["ロジスティック回帰"]
    coef = pd.DataFrame({"特徴量": feature_cols, "標準化係数": logi.coef_[0]}).sort_values("標準化係数", ascending=False)
    coef.to_csv(output_dir / "paddy_all_elapsed_logistic_coefficients.csv", index=False, encoding="utf-8-sig")

    tree_text = export_text(fitted["決定木_depth3"], feature_names=feature_cols, decimals=3)
    (output_dir / "paddy_all_elapsed_decision_tree_rules.txt").write_text(tree_text, encoding="utf-8")

    colors = {"田んぼ内_正解浸水域": "#d62728", "田んぼ内_非浸水域": "#4c78a8"}
    top_features = auc_df.head(8)["特徴量"].tolist()
    fig, axes = plt.subplots(2, 4, figsize=(15.5, 7.0), dpi=180)
    for ax, feature in zip(axes.ravel(), top_features):
        a = balanced[balanced["正解浸水域"] == 1][feature].to_numpy()
        b = balanced[balanced["正解浸水域"] == 0][feature].to_numpy()
        lo, hi = np.percentile(np.concatenate([a, b]), [1, 99])
        bins = np.linspace(lo, hi, 50)
        ax.hist(b, bins=bins, density=True, alpha=0.45, color=colors["田んぼ内_非浸水域"], label="非浸水域")
        ax.hist(a, bins=bins, density=True, alpha=0.45, color=colors["田んぼ内_正解浸水域"], label="正解浸水域")
        ax.set_title(feature, fontsize=9)
        ax.grid(True, axis="y", alpha=0.25)
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("田んぼ内: 分離力上位特徴量の分布", y=1.02)
    fig.tight_layout()
    fig.savefig(output_dir / "fig_top_feature_histograms.png", bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.0, 5.2), dpi=180)
    imp_plot = importance.head(10).sort_values("重要度")
    ax.barh(imp_plot["特徴量"], imp_plot["重要度"], color="#4c78a8")
    ax.set_title("ランダムフォレストの特徴量重要度")
    ax.set_xlabel("重要度")
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_dir / "fig_random_forest_feature_importance.png")
    plt.close(fig)

    profile_cols = ["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]
    fig, ax = plt.subplots(figsize=(8.2, 5.0), dpi=180)
    for zone, val, color in [("田んぼ内_正解浸水域", 1, "#d62728"), ("田んぼ内_非浸水域", 0, "#4c78a8")]:
        sub = stats_df[(stats_df["領域"] == zone) & (stats_df["特徴量"].isin(profile_cols))]
        sub = sub.set_index("特徴量").reindex(profile_cols)
        xlabels = ["0-3h", "3-6h", "6-12h", "12-24h"]
        ax.plot(xlabels, sub["中央値"], marker="o", color=color, label=f"{zone} 中央値")
        ax.fill_between(xlabels, sub["p25"], sub["p75"], color=color, alpha=0.12)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("田んぼ内: 4時間帯すべての差分プロファイル")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before (dB)")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "fig_all_elapsed_profile.png")
    plt.close(fig)

    print("counts")
    print(balanced["正解浸水域"].value_counts().to_string())
    print("AUC")
    print(auc_df.head(12).to_string(index=False))
    print("metrics")
    print(metrics.to_string(index=False))
    print("importance")
    print(importance.head(12).to_string(index=False))
    print("tree")
    print(tree_text)
    print(f"saved: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
