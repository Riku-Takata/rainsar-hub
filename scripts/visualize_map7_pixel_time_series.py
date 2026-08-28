#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Visualize per-pixel target-before difference time series for map7."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = (
    ROOT_DIR
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
DEFAULT_OUTPUT_DIR = DEFAULT_INPUT_DIR / "pixel_time_series"
ELAPSED = [
    ("0-3h", "map7_mean_diff_0_3h.tif"),
    ("3-6h", "map7_mean_diff_3_6h.tif"),
    ("6-12h", "map7_mean_diff_6_12h.tif"),
    ("12-24h", "map7_mean_diff_12_24h.tif"),
]


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def read_raster(path: Path) -> tuple[np.ndarray, dict]:
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32), src.profile.copy()


def pixel_table(mask: np.ndarray, stack: np.ndarray, transform, sample_size: int, seed: int) -> pd.DataFrame:
    rows, cols = np.where(mask)
    if rows.size > sample_size:
        rng = np.random.default_rng(seed)
        idx = rng.choice(rows.size, size=sample_size, replace=False)
        rows = rows[idx]
        cols = cols[idx]
    xs, ys = rasterio.transform.xy(transform, rows, cols, offset="center")
    data = {
        "row": rows.astype(int),
        "col": cols.astype(int),
        "lon": np.asarray(xs, dtype=float),
        "lat": np.asarray(ys, dtype=float),
    }
    for i, (label, _filename) in enumerate(ELAPSED):
        data[label] = stack[i, rows, cols]
    df = pd.DataFrame(data)
    df["早期平均_0_6h"] = (df["0-3h"] + df["3-6h"]) / 2.0
    df["後期平均_6_24h"] = (df["6-12h"] + df["12-24h"]) / 2.0
    df["早期_minus_後期"] = df["早期平均_0_6h"] - df["後期平均_6_24h"]
    df["0_3h_minus_6_12h"] = df["0-3h"] - df["6-12h"]
    df["3_6h_minus_6_12h"] = df["3-6h"] - df["6-12h"]
    return df


def save_spaghetti(df: pd.DataFrame, out_path: Path, title: str, plt, max_lines: int = 800) -> None:
    labels = [label for label, _filename in ELAPSED]
    plot_df = df.copy()
    if len(plot_df) > max_lines:
        plot_df = plot_df.sample(max_lines, random_state=20260518)
    x = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(9.2, 5.0), dpi=180)
    for _, row in plot_df.iterrows():
        ax.plot(x, [row[label] for label in labels], color="#4c78a8", alpha=0.035, linewidth=0.8)
    mean_values = df[labels].mean()
    median_values = df[labels].median()
    ax.plot(x, mean_values, color="#d62728", marker="o", linewidth=2.4, label="平均")
    ax.plot(x, median_values, color="#111827", marker="s", linewidth=2.0, label="中央値")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before")
    ax.set_title(title)
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def save_representative(df: pd.DataFrame, out_path: Path, title: str, plt) -> pd.DataFrame:
    labels = [label for label, _filename in ELAPSED]
    mean_vec = df[labels].mean().to_numpy(dtype=float)
    values = df[labels].to_numpy(dtype=float)
    dist = np.sqrt(np.mean((values - mean_vec[None, :]) ** 2, axis=1))
    work = df.copy()
    work["平均時系列からのRMSE"] = dist
    reps = pd.concat(
        [
            work.nsmallest(5, "平均時系列からのRMSE").assign(代表種別="平均に近い画素"),
            work.nlargest(5, "早期_minus_後期").assign(代表種別="早期低下が強い画素"),
            work.nsmallest(5, "早期_minus_後期").assign(代表種別="低下が弱い画素"),
        ],
        ignore_index=True,
    )
    x = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(9.2, 5.0), dpi=180)
    colors = {
        "平均に近い画素": "#2ca02c",
        "早期低下が強い画素": "#d62728",
        "低下が弱い画素": "#1f77b4",
    }
    for _, row in reps.iterrows():
        ax.plot(
            x,
            [row[label] for label in labels],
            marker="o",
            linewidth=1.4,
            alpha=0.78,
            color=colors[row["代表種別"]],
        )
    ax.plot(x, df[labels].mean(), color="#111827", marker="s", linewidth=2.6, label="全体平均")
    # Deduplicate legend for representative types.
    handles = [
        plt.Line2D([0], [0], color=color, marker="o", label=name)
        for name, color in colors.items()
    ]
    handles.append(plt.Line2D([0], [0], color="#111827", marker="s", label="全体平均"))
    ax.legend(handles=handles, fontsize=8)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before")
    ax.set_title(title)
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)
    return reps


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--sample-size", type=int, default=30000)
    parser.add_argument("--seed", type=int, default=20260518)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    arrays = []
    profile = None
    for _label, filename in ELAPSED:
        arr, profile = read_raster(args.input_dir / filename)
        arrays.append(arr)
    stack = np.stack(arrays, axis=0)
    valid = np.all(np.isfinite(stack), axis=0)
    truth, _ = read_raster(args.input_dir / "map7_inundation_truth_mask.tif")
    detected, _ = read_raster(args.input_dir / "map7_detection_mask.tif")
    truth_mask = (truth > 0) & valid
    detected_mask = (detected > 0) & valid

    transform = profile["transform"]
    truth_df = pixel_table(truth_mask, stack, transform, args.sample_size, args.seed)
    truth_df.to_csv(args.output_dir / "正解浸水域_画素別_差分時系列.csv", index=False, encoding="utf-8-sig")

    zones = {
        "TP_正解浸水域かつ検出": truth_mask & detected_mask,
        "FN_正解浸水域だが未検出": truth_mask & ~detected_mask,
        "FP_正解外だが誤検出": ~truth_mask & detected_mask & valid,
        "TN_正解外かつ非検出": ~truth_mask & ~detected_mask & valid,
    }
    zone_rows = []
    for name, mask in zones.items():
        df = pixel_table(mask, stack, transform, min(args.sample_size, int(np.sum(mask))), args.seed)
        df.insert(0, "分類", name)
        zone_rows.append(df)
    zone_df = pd.concat(zone_rows, ignore_index=True)
    zone_df.to_csv(args.output_dir / "分類別_画素別_差分時系列サンプル.csv", index=False, encoding="utf-8-sig")

    plt = setup_matplotlib()
    save_spaghetti(
        truth_df,
        args.output_dir / "図1_正解浸水域_画素別時系列_スパゲッティ.png",
        "正解浸水域：画素ごとの後方散乱強度差分時系列",
        plt,
    )
    reps = save_representative(
        truth_df,
        args.output_dir / "図2_正解浸水域_代表画素の時系列.png",
        "正解浸水域：代表画素の差分時系列",
        plt,
    )
    reps.to_csv(args.output_dir / "正解浸水域_代表画素.csv", index=False, encoding="utf-8-sig")

    labels = [label for label, _filename in ELAPSED]
    fig, ax = plt.subplots(figsize=(9.4, 5.0), dpi=180)
    colors = {
        "TP_正解浸水域かつ検出": "#2ca02c",
        "FN_正解浸水域だが未検出": "#1f77b4",
        "FP_正解外だが誤検出": "#d62728",
        "TN_正解外かつ非検出": "#7f7f7f",
    }
    x = np.arange(len(labels))
    for name, color in colors.items():
        g = zone_df[zone_df["分類"] == name]
        ax.plot(x, g[labels].mean(), marker="o", color=color, linewidth=2.0, label=name)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("平均差分 target - before")
    ax.set_title("分類別：画素ごとの時系列平均")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(args.output_dir / "図3_分類別_画素時系列平均.png")
    plt.close(fig)

    print(f"truth pixels exported: {len(truth_df)}")
    print(f"zone sample rows: {len(zone_df)}")
    print(f"saved: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
