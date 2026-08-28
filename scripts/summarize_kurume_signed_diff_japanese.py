#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Create Japanese tables and plots for signed Kurume backscatter differences."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_ANALYSIS_DIR = (
    ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "kurume_inundation_analysis"
)


AREA_LABELS = {
    "scene_all": "シーン全体",
    "kurume_inundated_union": "浸水域",
    "kurume_non_inundated_union": "非浸水域",
}


def jp_area(area: str) -> str:
    if area in AREA_LABELS:
        return AREA_LABELS[area]
    if area.endswith("_inundated"):
        return area.replace("_inundated", "_浸水域")
    if area.endswith("_non_inundated"):
        return area.replace("_non_inundated", "_非浸水域")
    return area


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis-dir", type=Path, default=DEFAULT_ANALYSIS_DIR)
    args = parser.parse_args()

    out_dir = args.analysis_dir / "signed_diff_japanese"
    out_dir.mkdir(parents=True, exist_ok=True)

    stats = pd.read_csv(args.analysis_dir / "kurume_backscatter_stats_by_area_2022.csv")
    diff_stats = stats[(stats["value_type"] == "diff") & (stats["count"] > 0)].copy()

    main_areas = ["kurume_inundated_union", "kurume_non_inundated_union", "scene_all"]
    pair_wide = diff_stats[diff_stats["area"].isin(main_areas)].pivot_table(
        index=["rain_day_jst", "pair_no", "delay_from_rain_start_h", "elapsed_bin"],
        columns="area",
        values="mean",
        aggfunc="first",
    ).reset_index()
    pair_wide["浸水域_minus_非浸水域"] = (
        pair_wide["kurume_inundated_union"] - pair_wide["kurume_non_inundated_union"]
    )
    pair_table = pair_wide.rename(
        columns={
            "rain_day_jst": "降雨日",
            "pair_no": "ペア",
            "delay_from_rain_start_h": "降雨開始からの経過時間_h",
            "elapsed_bin": "経過時間帯",
            "kurume_inundated_union": "浸水域_平均差分",
            "kurume_non_inundated_union": "非浸水域_平均差分",
            "scene_all": "シーン全体_平均差分",
        }
    ).sort_values(["降雨開始からの経過時間_h", "降雨日", "ペア"])
    pair_table.to_csv(out_dir / "ペア別_符号付き差分_経過時間表.csv", index=False, encoding="utf-8-sig")

    elapsed_summary = (
        pair_wide.dropna(subset=["kurume_inundated_union", "kurume_non_inundated_union"])
        .groupby("elapsed_bin", observed=True)
        .agg(
            有効ペア数=("pair_no", "count"),
            浸水域_平均差分=("kurume_inundated_union", "mean"),
            非浸水域_平均差分=("kurume_non_inundated_union", "mean"),
            シーン全体_平均差分=("scene_all", "mean"),
            浸水域_minus_非浸水域=("浸水域_minus_非浸水域", "mean"),
            経過時間_min_h=("delay_from_rain_start_h", "min"),
            経過時間_max_h=("delay_from_rain_start_h", "max"),
        )
        .reset_index()
        .rename(columns={"elapsed_bin": "経過時間帯"})
    )
    elapsed_summary.to_csv(out_dir / "経過時間帯別_符号付き差分_要約.csv", index=False, encoding="utf-8-sig")

    area_elapsed = (
        diff_stats.groupby(["area", "elapsed_bin"], observed=True)
        .agg(
            有効ペア数=("pair_no", "count"),
            画素数=("count", "sum"),
            平均差分=("mean", "mean"),
            中央値差分=("p50", "mean"),
            第25百分位=("p25", "mean"),
            第75百分位=("p75", "mean"),
            経過時間_min_h=("delay_from_rain_start_h", "min"),
            経過時間_max_h=("delay_from_rain_start_h", "max"),
        )
        .reset_index()
        .rename(columns={"area": "領域", "elapsed_bin": "経過時間帯"})
    )
    area_elapsed["領域"] = area_elapsed["領域"].map(jp_area)
    area_elapsed.to_csv(out_dir / "領域別_経過時間帯別_符号付き差分.csv", index=False, encoding="utf-8-sig")

    per_area = diff_stats[
        diff_stats["area"].str.endswith("_inundated") | diff_stats["area"].str.endswith("_non_inundated")
    ].copy()
    per_area["base_area"] = per_area["area"].str.replace("_non_inundated$", "", regex=True).str.replace(
        "_inundated$", "", regex=True
    )
    per_area["zone"] = np.where(per_area["area"].str.endswith("_non_inundated"), "非浸水域", "浸水域")
    per_area_wide = per_area.pivot_table(
        index=["base_area", "rain_day_jst", "pair_no", "delay_from_rain_start_h", "elapsed_bin"],
        columns="zone",
        values="mean",
        aggfunc="first",
    ).reset_index()
    if {"浸水域", "非浸水域"}.issubset(per_area_wide.columns):
        per_area_wide["浸水域_minus_非浸水域"] = per_area_wide["浸水域"] - per_area_wide["非浸水域"]
    per_area_wide = per_area_wide.rename(
        columns={
            "base_area": "浸水域ID",
            "rain_day_jst": "降雨日",
            "pair_no": "ペア",
            "delay_from_rain_start_h": "降雨開始からの経過時間_h",
            "elapsed_bin": "経過時間帯",
        }
    )
    per_area_wide.to_csv(out_dir / "各浸水域_浸水域非浸水域_符号付き差分.csv", index=False, encoding="utf-8-sig")

    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    # Scatter by pair.
    fig, ax = plt.subplots(figsize=(10, 5), dpi=160)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.scatter(pair_wide["delay_from_rain_start_h"], pair_wide["scene_all"], label="シーン全体", alpha=0.75)
    ax.scatter(pair_wide["delay_from_rain_start_h"], pair_wide["kurume_inundated_union"], label="浸水域", alpha=0.85)
    ax.scatter(
        pair_wide["delay_from_rain_start_h"],
        pair_wide["kurume_non_inundated_union"],
        label="非浸水域",
        alpha=0.85,
    )
    ax.set_xlabel("降雨開始からの経過時間 [h]")
    ax.set_ylabel("平均差分 target - before")
    ax.set_title("ペア別の符号付き後方散乱差分")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "ペア別_符号付き差分_散布図.png")
    plt.close(fig)

    # Elapsed bar chart.
    fig, ax = plt.subplots(figsize=(9, 4), dpi=160)
    x = np.arange(len(elapsed_summary))
    width = 0.25
    ax.axhline(0, color="black", linewidth=0.8)
    ax.bar(x - width, elapsed_summary["浸水域_平均差分"], width, label="浸水域")
    ax.bar(x, elapsed_summary["非浸水域_平均差分"], width, label="非浸水域")
    ax.bar(x + width, elapsed_summary["シーン全体_平均差分"], width, label="シーン全体")
    ax.set_xticks(x)
    ax.set_xticklabels(elapsed_summary["経過時間帯"])
    ax.set_xlabel("経過時間帯")
    ax.set_ylabel("平均差分 target - before")
    ax.set_title("経過時間帯別の符号付き後方散乱差分")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "経過時間帯別_符号付き差分_棒グラフ.png")
    plt.close(fig)

    # Heatmap for each inundated area.
    heat = per_area[per_area["zone"] == "浸水域"].groupby(["base_area", "elapsed_bin"], observed=True)[
        "mean"
    ].mean().unstack("elapsed_bin")
    fig, ax = plt.subplots(figsize=(8, max(4, 0.35 * len(heat))), dpi=160)
    matrix = heat.to_numpy(dtype=float)
    vmax = np.nanmax(np.abs(matrix)) if np.isfinite(matrix).any() else 1
    image = ax.imshow(matrix, aspect="auto", cmap="coolwarm", vmin=-vmax, vmax=vmax)
    ax.set_xticks(np.arange(len(heat.columns)))
    ax.set_xticklabels(heat.columns.astype(str), rotation=45, ha="right")
    ax.set_yticks(np.arange(len(heat.index)))
    ax.set_yticklabels(heat.index)
    ax.set_title("各浸水域の経過時間帯別 符号付き差分")
    fig.colorbar(image, ax=ax, label="平均差分 target - before")
    fig.tight_layout()
    fig.savefig(out_dir / "各浸水域_経過時間帯別_符号付き差分ヒートマップ.png")
    plt.close(fig)

    print(out_dir)
    print(elapsed_summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
