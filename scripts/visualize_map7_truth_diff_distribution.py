#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Visualize target-before backscatter difference distributions in map7 truth inundation pixels."""

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
DEFAULT_OUTPUT_DIR = DEFAULT_INPUT_DIR / "truth_diff_distribution"
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


def read_raster(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def stats_row(label: str, values: np.ndarray) -> dict[str, float | int | str]:
    values = values[np.isfinite(values)]
    q = np.percentile(values, [1, 5, 10, 25, 50, 75, 90, 95, 99])
    return {
        "経過時間帯": label,
        "画素数": int(values.size),
        "平均": float(np.mean(values)),
        "標準偏差": float(np.std(values)),
        "最小": float(np.min(values)),
        "p01": float(q[0]),
        "p05": float(q[1]),
        "p10": float(q[2]),
        "p25": float(q[3]),
        "中央値": float(q[4]),
        "p75": float(q[5]),
        "p90": float(q[6]),
        "p95": float(q[7]),
        "p99": float(q[8]),
        "最大": float(np.max(values)),
        "負の割合_percent": float(np.mean(values < 0) * 100),
        "正の割合_percent": float(np.mean(values > 0) * 100),
        "abs_lt_0_5_percent": float(np.mean(np.abs(values) < 0.5) * 100),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    truth = read_raster(args.input_dir / "map7_inundation_truth_mask.tif") > 0
    rows = []
    sample_rows = []
    values_by_label: dict[str, np.ndarray] = {}
    for label, filename in ELAPSED:
        arr = read_raster(args.input_dir / filename)
        values = arr[truth & np.isfinite(arr)]
        values_by_label[label] = values
        rows.append(stats_row(label, values))
        sample_rows.extend({"経過時間帯": label, "差分": float(v)} for v in values)

    stats = pd.DataFrame(rows)
    samples = pd.DataFrame(sample_rows)
    stats.to_csv(args.output_dir / "正解浸水域_差分分布統計.csv", index=False, encoding="utf-8-sig")
    samples.to_csv(args.output_dir / "正解浸水域_差分値.csv", index=False, encoding="utf-8-sig")

    plt = setup_matplotlib()

    labels = [label for label, _ in ELAPSED]
    fig, ax = plt.subplots(figsize=(8.5, 4.8), dpi=180)
    data = [values_by_label[label] for label in labels]
    box = ax.boxplot(data, labels=labels, showfliers=False, patch_artist=True)
    for patch, color in zip(box["boxes"], ["#4c78a8", "#72b7b2", "#f58518", "#54a24b"]):
        patch.set_facecolor(color)
        patch.set_alpha(0.55)
    ax.axhline(0, color="black", linewidth=0.9)
    ax.set_title("正解浸水域における後方散乱強度差分の分布")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before")
    ax.grid(axis="y", alpha=0.28)
    fig.tight_layout()
    fig.savefig(args.output_dir / "図1_正解浸水域_差分箱ひげ図.png")
    plt.close(fig)

    fig, axes = plt.subplots(2, 2, figsize=(10.5, 7.2), dpi=180, sharex=True, sharey=True)
    bins = np.linspace(-12, 12, 97)
    for ax, label in zip(axes.ravel(), labels):
        values = values_by_label[label]
        ax.hist(values, bins=bins, color="#4c78a8", alpha=0.78, density=True)
        ax.axvline(np.mean(values), color="#d62728", linewidth=1.6, label="平均")
        ax.axvline(np.median(values), color="#111827", linewidth=1.4, linestyle="--", label="中央値")
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_title(label)
        ax.grid(alpha=0.22)
    axes[0, 0].legend(fontsize=8)
    fig.suptitle("正解浸水域における差分ヒストグラム", y=0.995)
    fig.supxlabel("差分 target - before")
    fig.supylabel("密度")
    fig.tight_layout()
    fig.savefig(args.output_dir / "図2_正解浸水域_差分ヒストグラム.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.8, 4.8), dpi=180)
    means = stats["平均"].to_numpy()
    p25 = stats["p25"].to_numpy()
    p75 = stats["p75"].to_numpy()
    med = stats["中央値"].to_numpy()
    x = np.arange(len(labels))
    ax.plot(x, means, marker="o", color="#d62728", label="平均")
    ax.plot(x, med, marker="s", color="#111827", label="中央値")
    ax.fill_between(x, p25, p75, color="#4c78a8", alpha=0.20, label="p25-p75")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_title("正解浸水域の差分時系列分布")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(args.output_dir / "図3_正解浸水域_差分時系列_分位点.png")
    plt.close(fig)

    print(stats.to_string(index=False))
    print(f"saved: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
