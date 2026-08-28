#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Create readable figures/tables for Kanoya signature matching results."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = ROOT_DIR / "output" / "kanoya_rain_s1" / "kurume_signature_diff_analysis"

ELAPSED_LABELS = ["0-3h", "3-6h", "6-12h", "12-24h"]


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def read_raster(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1)


def save_timeseries(input_dir: Path, plt) -> None:
    criteria = pd.read_csv(input_dir / "kanoya_diff_matching_criteria.csv", encoding="utf-8-sig")
    profiles = pd.read_csv(input_dir / "kanoya_diff_profile_summary_by_zone.csv", encoding="utf-8-sig")

    keep = [
        "類似判定画素",
        "正解浸水域_0.5-1.7",
        "正解浸水域かつ類似判定",
        "正解浸水域だが非類似",
    ]
    colors = {
        "久留米浸水域": "#111827",
        "類似判定画素": "#d62728",
        "正解浸水域_0.5-1.7": "#1f77b4",
        "正解浸水域かつ類似判定": "#2ca02c",
        "正解浸水域だが非類似": "#ff7f0e",
    }

    fig, ax = plt.subplots(figsize=(9.2, 5.2), dpi=180)
    ax.plot(
        ELAPSED_LABELS,
        criteria.set_index("経過時間帯").reindex(ELAPSED_LABELS)["久留米浸水域_平均差分"],
        marker="o",
        linewidth=2.6,
        color=colors["久留米浸水域"],
        label="久留米浸水域（基準）",
    )
    for area in keep:
        g = profiles[profiles["領域"] == area].set_index("経過時間帯").reindex(ELAPSED_LABELS)
        ax.plot(
            ELAPSED_LABELS,
            g["平均差分"],
            marker="o",
            linewidth=1.8,
            color=colors[area],
            label=area,
        )
        ax.fill_between(ELAPSED_LABELS, g["p25"], g["p75"], color=colors[area], alpha=0.10)
    ax.axhline(0, color="#555555", linewidth=0.9)
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("平均差分 target - before")
    ax.set_title("鹿屋：差分時系列と久留米浸水域パターンの比較")
    ax.grid(True, alpha=0.28)
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(input_dir / "図1_差分時系列_久留米基準との比較.png")
    plt.close(fig)

    summary = profiles.pivot(index="領域", columns="経過時間帯", values="平均差分").reindex(columns=ELAPSED_LABELS)
    summary.to_csv(input_dir / "表_領域別_平均差分時系列.csv", encoding="utf-8-sig")


def save_overlap_figures(input_dir: Path, plt) -> None:
    overlap = pd.read_csv(input_dir / "kanoya_diff_kurume_signature_overlap_summary.csv", encoding="utf-8-sig")
    row = overlap[overlap["浸水域定義"].astype(str).str.contains("0.5")].iloc[0]
    hit = int(row["類似判定に含まれた浸水画素数"])
    total = int(row["評価対象浸水画素数"])
    miss = total - hit
    rate = float(row["浸水域包含率_percent"])

    fig, ax = plt.subplots(figsize=(6.2, 4.2), dpi=180)
    bars = ax.bar(["一致", "未一致"], [hit, miss], color=["#2ca02c", "#b8c2cc"], width=0.55)
    ax.set_ylabel("正解浸水域の画素数")
    ax.set_title(f"正解浸水域に対する類似判定の包含率：{rate:.1f}%")
    for bar in bars:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{int(bar.get_height()):,}", ha="center", va="bottom")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(input_dir / "図2_正解浸水域の包含率.png")
    plt.close(fig)

    compact = pd.DataFrame(
        [
            {
                "項目": "正解浸水域画素数（0.5<=TIF値<=1.7）",
                "値": total,
                "割合_percent": 100.0,
            },
            {"項目": "類似判定に含まれた正解浸水域画素数", "値": hit, "割合_percent": rate},
            {"項目": "類似判定されなかった正解浸水域画素数", "値": miss, "割合_percent": 100.0 - rate},
            {
                "項目": "類似判定総画素数",
                "値": int(row["類似判定総画素数"]),
                "割合_percent": float(row["類似判定面積率_percent"]),
            },
        ]
    )
    compact.to_csv(input_dir / "表_浸水域包含率_要約.csv", index=False, encoding="utf-8-sig")


def save_map_panel(input_dir: Path, plt) -> None:
    similar = read_raster(input_dir / "kanoya_diff_kurume_signature_similar_mask.tif") > 0
    inundated = read_raster(input_dir / "kanoya_inundation_mask_0p5_1p7_on_diff_scene.tif") > 0
    score = read_raster(input_dir / "kanoya_diff_kurume_signature_score.tif").astype(float)
    zrmse = read_raster(input_dir / "kanoya_diff_zrmse_to_kurume_inundated.tif").astype(float)

    overlap_map = np.zeros(similar.shape, dtype=np.uint8)
    overlap_map[similar] = 1
    overlap_map[inundated] = 2
    overlap_map[similar & inundated] = 3

    from matplotlib.colors import ListedColormap, BoundaryNorm

    fig, axes = plt.subplots(1, 3, figsize=(12, 4.4), dpi=180)
    cmap = ListedColormap(["#f8fafc", "#d62728", "#1f77b4", "#2ca02c"])
    norm = BoundaryNorm([-0.5, 0.5, 1.5, 2.5, 3.5], cmap.N)
    axes[0].imshow(overlap_map, cmap=cmap, norm=norm)
    axes[0].set_title("判定と正解浸水域の重なり")
    axes[0].text(0.02, -0.08, "赤: 類似 / 青: 正解 / 緑: 一致", transform=axes[0].transAxes, fontsize=8)

    im1 = axes[1].imshow(score, cmap="RdYlBu_r", vmin=np.nanpercentile(score, 2), vmax=np.nanpercentile(score, 98))
    axes[1].set_title("浸水域らしさスコア")
    fig.colorbar(im1, ax=axes[1], fraction=0.046)

    im2 = axes[2].imshow(zrmse, cmap="viridis_r", vmin=0, vmax=min(3, np.nanpercentile(zrmse, 98)))
    axes[2].set_title("久留米浸水域との差（zRMSE）")
    fig.colorbar(im2, ax=axes[2], fraction=0.046)

    for ax in axes:
        ax.set_xticks([])
        ax.set_yticks([])
    fig.tight_layout()
    fig.savefig(input_dir / "図3_空間分布_類似判定と正解浸水域.png")
    plt.close(fig)


def save_summary_panel(input_dir: Path, plt) -> None:
    criteria = pd.read_csv(input_dir / "kanoya_diff_matching_criteria.csv", encoding="utf-8-sig")
    profiles = pd.read_csv(input_dir / "kanoya_diff_profile_summary_by_zone.csv", encoding="utf-8-sig")
    overlap = pd.read_csv(input_dir / "kanoya_diff_kurume_signature_overlap_summary.csv", encoding="utf-8-sig")
    count = pd.read_csv(input_dir / "kanoya_diff_elapsed_bin_counts.csv", encoding="utf-8-sig")

    sim = profiles[profiles["領域"] == "類似判定画素"].set_index("経過時間帯").reindex(ELAPSED_LABELS)
    kurume = criteria.set_index("経過時間帯").reindex(ELAPSED_LABELS)
    row = overlap[overlap["浸水域定義"].astype(str).str.contains("0.5")].iloc[0]

    fig = plt.figure(figsize=(11, 7.5), dpi=180)
    gs = fig.add_gridspec(2, 2, height_ratios=[1.1, 1.0])
    ax1 = fig.add_subplot(gs[0, :])
    ax1.plot(ELAPSED_LABELS, kurume["久留米浸水域_平均差分"], marker="o", label="久留米浸水域（基準）", color="#111827")
    ax1.plot(ELAPSED_LABELS, sim["平均差分"], marker="o", label="鹿屋 類似判定画素", color="#d62728")
    ax1.axhline(0, color="#555555", linewidth=0.8)
    ax1.set_title("差分時系列の比較")
    ax1.set_ylabel("平均差分 target - before")
    ax1.grid(True, alpha=0.25)
    ax1.legend()

    ax2 = fig.add_subplot(gs[1, 0])
    ax2.axis("off")
    table_rows = [
        ["正解浸水域画素数", f"{int(row['評価対象浸水画素数']):,}"],
        ["一致画素数", f"{int(row['類似判定に含まれた浸水画素数']):,}"],
        ["包含率", f"{float(row['浸水域包含率_percent']):.1f}%"],
        ["類似判定総画素数", f"{int(row['類似判定総画素数']):,}"],
        ["類似判定面積率", f"{float(row['類似判定面積率_percent']):.1f}%"],
    ]
    tbl = ax2.table(cellText=table_rows, colLabels=["指標", "値"], loc="center", cellLoc="left")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.45)
    ax2.set_title("一致率の要約")

    ax3 = fig.add_subplot(gs[1, 1])
    ax3.bar(count["経過時間帯"], count["ペア数"], color="#4c78a8")
    ax3.set_title("経過時間帯ごとのペア数")
    ax3.set_ylabel("ペア数")
    ax3.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(input_dir / "図4_結果サマリー.png")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    args = parser.parse_args()

    plt = setup_matplotlib()
    save_timeseries(args.input_dir, plt)
    save_overlap_figures(args.input_dir, plt)
    save_map_panel(args.input_dir, plt)
    save_summary_panel(args.input_dir, plt)
    print(f"saved figures to: {args.input_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
