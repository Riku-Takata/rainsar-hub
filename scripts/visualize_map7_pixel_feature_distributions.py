#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Visualize distributions of per-pixel time-series difference features."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = (
    ROOT_DIR
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
    / "pixel_time_series"
)
DEFAULT_OUTPUT_DIR = DEFAULT_INPUT_DIR / "feature_distributions"
TIME_COLS = ["0-3h", "3-6h", "6-12h", "12-24h"]
FEATURE_COLS = [
    "画素平均差分",
    "画素中央値差分",
    "画素標準偏差",
    "画素レンジ",
    "早期_minus_後期",
    "0_3h_minus_6_12h",
    "3_6h_minus_6_12h",
    "負の時間帯数",
]


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    values = out[TIME_COLS].to_numpy(dtype=float)
    out["画素平均差分"] = np.mean(values, axis=1)
    out["画素中央値差分"] = np.median(values, axis=1)
    out["画素標準偏差"] = np.std(values, axis=1)
    out["画素レンジ"] = np.max(values, axis=1) - np.min(values, axis=1)
    if "早期_minus_後期" not in out.columns:
        out["早期平均_0_6h"] = (out["0-3h"] + out["3-6h"]) / 2.0
        out["後期平均_6_24h"] = (out["6-12h"] + out["12-24h"]) / 2.0
        out["早期_minus_後期"] = out["早期平均_0_6h"] - out["後期平均_6_24h"]
    if "0_3h_minus_6_12h" not in out.columns:
        out["0_3h_minus_6_12h"] = out["0-3h"] - out["6-12h"]
    if "3_6h_minus_6_12h" not in out.columns:
        out["3_6h_minus_6_12h"] = out["3-6h"] - out["6-12h"]
    out["負の時間帯数"] = np.sum(values < 0, axis=1)
    return out


def stats_by_feature(df: pd.DataFrame, group_col: str | None = None) -> pd.DataFrame:
    rows = []
    groups = [(None, df)] if group_col is None else list(df.groupby(group_col, observed=True))
    for group_name, sub in groups:
        for feature in FEATURE_COLS:
            values = sub[feature].dropna().to_numpy(dtype=float)
            q = np.percentile(values, [5, 25, 50, 75, 95])
            row = {
                "分類": group_name if group_name is not None else "正解浸水域",
                "特徴量": feature,
                "画素数": int(values.size),
                "平均": float(np.mean(values)),
                "標準偏差": float(np.std(values)),
                "p05": float(q[0]),
                "p25": float(q[1]),
                "中央値": float(q[2]),
                "p75": float(q[3]),
                "p95": float(q[4]),
            }
            rows.append(row)
    return pd.DataFrame(rows)


def save_truth_histograms(df: pd.DataFrame, output_dir: Path, plt) -> None:
    fig, axes = plt.subplots(2, 4, figsize=(14, 7), dpi=180)
    for ax, feature in zip(axes.ravel(), FEATURE_COLS):
        values = df[feature].dropna().to_numpy(dtype=float)
        ax.hist(values, bins=80, color="#4c78a8", alpha=0.78)
        ax.axvline(np.mean(values), color="#d62728", linewidth=1.5, label="平均")
        ax.axvline(np.median(values), color="#111827", linewidth=1.3, linestyle="--", label="中央値")
        ax.set_title(feature)
        ax.grid(alpha=0.22)
    axes[0, 0].legend(fontsize=8)
    fig.suptitle("正解浸水域：画素ごとの時系列特徴量分布", y=0.995)
    fig.tight_layout()
    fig.savefig(output_dir / "図1_正解浸水域_画素特徴量ヒストグラム.png")
    plt.close(fig)


def save_zone_boxplots(df: pd.DataFrame, output_dir: Path, plt) -> None:
    zone_order = [
        "TP_正解浸水域かつ検出",
        "FN_正解浸水域だが未検出",
        "FP_正解外だが誤検出",
        "TN_正解外かつ非検出",
    ]
    fig, axes = plt.subplots(2, 4, figsize=(15, 7.5), dpi=180)
    for ax, feature in zip(axes.ravel(), FEATURE_COLS):
        data = [df.loc[df["分類"] == zone, feature].dropna().to_numpy(dtype=float) for zone in zone_order]
        ax.boxplot(data, labels=["TP", "FN", "FP", "TN"], showfliers=False, patch_artist=True)
        ax.set_title(feature)
        ax.grid(axis="y", alpha=0.22)
    fig.suptitle("分類別：画素ごとの時系列特徴量分布", y=0.995)
    fig.tight_layout()
    fig.savefig(output_dir / "図2_TP_FN_FP_TN_画素特徴量箱ひげ図.png")
    plt.close(fig)


def save_scatter(df: pd.DataFrame, output_dir: Path, plt) -> None:
    zone_order = [
        "TP_正解浸水域かつ検出",
        "FN_正解浸水域だが未検出",
        "FP_正解外だが誤検出",
        "TN_正解外かつ非検出",
    ]
    colors = {
        "TP_正解浸水域かつ検出": "#2ca02c",
        "FN_正解浸水域だが未検出": "#1f77b4",
        "FP_正解外だが誤検出": "#d62728",
        "TN_正解外かつ非検出": "#7f7f7f",
    }
    fig, ax = plt.subplots(figsize=(8.2, 6.2), dpi=180)
    for zone in zone_order:
        sub = df[df["分類"] == zone]
        if len(sub) > 7000:
            sub = sub.sample(7000, random_state=20260518)
        ax.scatter(
            sub["早期_minus_後期"],
            sub["6-12h"],
            s=4,
            alpha=0.22,
            color=colors[zone],
            label=zone.split("_")[0],
        )
    ax.axhline(0, color="black", linewidth=0.8)
    ax.axvline(1.0, color="black", linewidth=0.8, linestyle="--")
    ax.set_xlabel("早期平均(0-6h) - 後期平均(6-24h)")
    ax.set_ylabel("6-12h 差分")
    ax.set_title("画素特徴量の散布図")
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "図3_早期後期差_vs_6_12h散布図.png")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    truth = add_features(pd.read_csv(args.input_dir / "正解浸水域_画素別_差分時系列.csv", encoding="utf-8-sig"))
    zones = add_features(pd.read_csv(args.input_dir / "分類別_画素別_差分時系列サンプル.csv", encoding="utf-8-sig"))

    truth.to_csv(args.output_dir / "正解浸水域_画素別_時系列特徴量.csv", index=False, encoding="utf-8-sig")
    zones.to_csv(args.output_dir / "分類別_画素別_時系列特徴量サンプル.csv", index=False, encoding="utf-8-sig")
    stats_by_feature(truth).to_csv(args.output_dir / "正解浸水域_画素特徴量分布統計.csv", index=False, encoding="utf-8-sig")
    stats_by_feature(zones, "分類").to_csv(args.output_dir / "分類別_画素特徴量分布統計.csv", index=False, encoding="utf-8-sig")

    plt = setup_matplotlib()
    save_truth_histograms(truth, args.output_dir, plt)
    save_zone_boxplots(zones, args.output_dir, plt)
    save_scatter(zones, args.output_dir, plt)

    print(stats_by_feature(truth).to_string(index=False))
    print(f"saved: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
