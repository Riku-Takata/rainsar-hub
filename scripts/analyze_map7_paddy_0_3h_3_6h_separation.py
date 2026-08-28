#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Check whether paddy inundated/non-inundated pixels separate between 0-3h and 3-6h."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import rasterio


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
OUT_DIR = DETECTION_DIR / "paddy_0_3h_3_6h_separation"
N_SAMPLE = 10000
SEED = 42


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


def summarize(values: np.ndarray) -> dict[str, float | int]:
    values = values[np.isfinite(values)]
    q = np.percentile(values, [5, 25, 50, 75, 95])
    return {
        "画素数": int(values.size),
        "平均": float(np.mean(values)),
        "標準偏差": float(np.std(values)),
        "p05": float(q[0]),
        "p25": float(q[1]),
        "中央値": float(q[2]),
        "p75": float(q[3]),
        "p95": float(q[4]),
    }


def auc_score(feature: np.ndarray, label: np.ndarray) -> float:
    ok = np.isfinite(feature)
    x = feature[ok]
    y = label[ok].astype(bool)
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty_like(order, dtype=np.float64)
    ranks[order] = np.arange(1, x.size + 1)
    sorted_x = x[order]
    start = 0
    while start < sorted_x.size:
        end = start + 1
        while end < sorted_x.size and sorted_x[end] == sorted_x[start]:
            end += 1
        if end - start > 1:
            ranks[order[start:end]] = (start + 1 + end) / 2.0
        start = end
    n_pos = int(y.sum())
    n_neg = int((~y).sum())
    return float((ranks[y].sum() - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg))


def threshold_scan(values: np.ndarray, labels: np.ndarray) -> pd.DataFrame:
    ok = np.isfinite(values)
    x = values[ok]
    y = labels[ok].astype(bool)
    rows = []
    for direction in [">=", "<="]:
        for th in np.unique(np.percentile(x, np.linspace(1, 99, 99))):
            pred = x >= th if direction == ">=" else x <= th
            tp = int(np.sum(pred & y))
            fp = int(np.sum(pred & ~y))
            fn = int(np.sum(~pred & y))
            tn = int(np.sum(~pred & ~y))
            precision = tp / (tp + fp) if tp + fp else 0.0
            recall = tp / (tp + fn) if tp + fn else 0.0
            specificity = tn / (tn + fp) if tn + fp else 0.0
            f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
            rows.append(
                {
                    "方向": direction,
                    "閾値": float(th),
                    "TP": tp,
                    "FP": fp,
                    "FN": fn,
                    "TN": tn,
                    "precision": precision,
                    "recall": recall,
                    "specificity": specificity,
                    "balanced_accuracy": (recall + specificity) / 2.0,
                    "F1": f1,
                }
            )
    return pd.DataFrame(rows)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)
    plt = setup_matplotlib()

    d0 = read_float(DETECTION_DIR / "map7_mean_diff_0_3h.tif")
    d3 = read_float(DETECTION_DIR / "map7_mean_diff_3_6h.tif")
    truth = read_bool(DETECTION_DIR / "map7_inundation_truth_mask.tif")
    paddy = read_bool(DETECTION_DIR / "landmask_filter" / "map7_paddy_mask.tif")
    valid = np.isfinite(d0) & np.isfinite(d3) & paddy

    features = {
        "diff_0_3h": d0,
        "diff_3_6h": d3,
        "change_3_6h_minus_0_3h": d3 - d0,
        "drop_0_3h_minus_3_6h": d0 - d3,
        "mean_0_6h": (d0 + d3) / 2.0,
    }

    sampled_masks = {}
    for zone, mask in {
        "田んぼ内_正解浸水域": valid & truth,
        "田んぼ内_非浸水域": valid & ~truth,
    }.items():
        y, x = np.where(mask)
        take = min(N_SAMPLE, y.size)
        idx = rng.choice(y.size, size=take, replace=False)
        sm = np.zeros_like(mask, dtype=bool)
        sm[y[idx], x[idx]] = True
        sampled_masks[zone] = sm

    stats_rows = []
    value_rows = []
    for zone, mask in sampled_masks.items():
        for name, arr in features.items():
            values = arr[mask]
            row = {"領域": zone, "特徴量": name}
            row.update(summarize(values))
            stats_rows.append(row)
            for value in values[np.isfinite(values)]:
                value_rows.append({"領域": zone, "特徴量": name, "値": float(value)})

    stats = pd.DataFrame(stats_rows)
    values_df = pd.DataFrame(value_rows)
    stats.to_csv(OUT_DIR / "paddy_0_3h_3_6h_feature_stats_balanced10000.csv", index=False, encoding="utf-8-sig")
    values_df.to_csv(OUT_DIR / "paddy_0_3h_3_6h_feature_values_balanced10000.csv", index=False, encoding="utf-8-sig")

    label = (truth & valid)[valid]
    auc_rows = []
    best_rows = []
    for name, arr in features.items():
        vals = arr[valid].astype(np.float64)
        auc = auc_score(vals, label)
        auc_rows.append(
            {
                "特徴量": name,
                "AUC": auc,
                "分離力_AUC大きい側": max(auc, 1.0 - auc),
                "浸水域が大きい方向": ">=" if auc >= 0.5 else "<=",
            }
        )
        scan = threshold_scan(vals, label)
        best = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).head(1).copy()
        best.insert(0, "特徴量", name)
        best_rows.append(best)
    auc_df = pd.DataFrame(auc_rows).sort_values("分離力_AUC大きい側", ascending=False)
    best_df = pd.concat(best_rows, ignore_index=True).sort_values("balanced_accuracy", ascending=False)
    auc_df.to_csv(OUT_DIR / "paddy_0_3h_3_6h_feature_auc.csv", index=False, encoding="utf-8-sig")
    best_df.to_csv(OUT_DIR / "paddy_0_3h_3_6h_best_single_thresholds.csv", index=False, encoding="utf-8-sig")

    colors = {"田んぼ内_正解浸水域": "#d62728", "田んぼ内_非浸水域": "#4c78a8"}
    for name in ["diff_0_3h", "diff_3_6h", "change_3_6h_minus_0_3h", "drop_0_3h_minus_3_6h"]:
        fig, ax = plt.subplots(figsize=(7.6, 4.8), dpi=180)
        sub = values_df[values_df["特徴量"] == name]
        all_values = sub["値"].to_numpy()
        lo, hi = np.percentile(all_values, [1, 99])
        bins = np.linspace(lo, hi, 55)
        for zone in ["田んぼ内_非浸水域", "田んぼ内_正解浸水域"]:
            vals = sub[sub["領域"] == zone]["値"].to_numpy()
            ax.hist(vals, bins=bins, density=True, alpha=0.45, color=colors[zone], label=zone)
        ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
        ax.set_title(f"田んぼ内: {name} の分布")
        ax.set_xlabel("dB")
        ax.set_ylabel("密度")
        ax.grid(True, axis="y", alpha=0.25)
        ax.legend()
        fig.tight_layout()
        fig.savefig(OUT_DIR / f"fig_{name}_hist.png")
        plt.close(fig)

    fig, ax = plt.subplots(figsize=(6.2, 5.4), dpi=180)
    for zone, mask in sampled_masks.items():
        ax.scatter(d0[mask], d3[mask], s=3, alpha=0.18, color=colors[zone], label=zone)
    ax.axline((0, 0), slope=1, color="black", linestyle="--", linewidth=0.8)
    ax.set_title("田んぼ内: 0-3h差分と3-6h差分")
    ax.set_xlabel("0-3h 差分 dB")
    ax.set_ylabel("3-6h 差分 dB")
    ax.grid(True, alpha=0.25)
    ax.legend(markerscale=4)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "fig_diff_0_3h_vs_3_6h_scatter.png")
    plt.close(fig)

    print(stats[stats["特徴量"].isin(["diff_0_3h", "diff_3_6h", "change_3_6h_minus_0_3h", "drop_0_3h_minus_3_6h"])].to_string(index=False))
    print(auc_df.to_string(index=False))
    print(best_df[["特徴量", "方向", "閾値", "precision", "recall", "specificity", "balanced_accuracy", "F1"]].to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
