#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Create tables and plots for Kurume inundation-area status by elapsed time."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_ANALYSIS_DIR = (
    ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "kurume_inundation_analysis"
)


def area_sort_key(area: str) -> tuple[int, str]:
    if area == "scene_all":
        return (0, area)
    if area == "kurume_inundated_union":
        return (1, area)
    if area == "kurume_non_inundated_union":
        return (2, area)
    return (3, area)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis-dir", type=Path, default=DEFAULT_ANALYSIS_DIR)
    args = parser.parse_args()

    out_dir = args.analysis_dir / "status_tables_plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    stats = pd.read_csv(args.analysis_dir / "kurume_backscatter_stats_by_area_2022.csv")
    residual = pd.read_csv(args.analysis_dir / "kurume_abs_diff_residual_class_by_area_2022.csv")

    abs_stats = stats[(stats["value_type"] == "abs_diff") & (stats["count"] > 0)].copy()
    pair_comp = abs_stats[
        abs_stats["area"].isin(["kurume_inundated_union", "kurume_non_inundated_union", "scene_all"])
    ].pivot_table(
        index=["rain_day_jst", "pair_no", "delay_from_rain_start_h", "elapsed_bin"],
        columns="area",
        values="mean",
        aggfunc="first",
    ).reset_index()
    pair_comp["inundated_minus_non_inundated"] = (
        pair_comp["kurume_inundated_union"] - pair_comp["kurume_non_inundated_union"]
    )
    pair_comp["inundated_minus_scene_all"] = pair_comp["kurume_inundated_union"] - pair_comp["scene_all"]
    pair_comp.to_csv(args.analysis_dir / "kurume_union_abs_diff_pair_comparison_2022.csv", index=False, encoding="utf-8-sig")

    valid_pair_comp = pair_comp.dropna(subset=["kurume_inundated_union", "kurume_non_inundated_union"]).copy()
    elapsed_comp = (
        valid_pair_comp.groupby("elapsed_bin", observed=True)
        .agg(
            valid_pair_count=("pair_no", "count"),
            inundated_abs_mean=("kurume_inundated_union", "mean"),
            non_inundated_abs_mean=("kurume_non_inundated_union", "mean"),
            scene_all_abs_mean=("scene_all", "mean"),
            inundated_minus_non_mean=("inundated_minus_non_inundated", "mean"),
            inundated_minus_non_median=("inundated_minus_non_inundated", "median"),
            inundated_minus_scene_mean=("inundated_minus_scene_all", "mean"),
        )
        .reset_index()
    )
    elapsed_comp.to_csv(args.analysis_dir / "kurume_union_abs_diff_elapsed_comparison_2022.csv", index=False, encoding="utf-8-sig")

    # Pair elapsed time table.
    pair_table = (
        pair_comp[
            [
                "rain_day_jst",
                "pair_no",
                "delay_from_rain_start_h",
                "elapsed_bin",
                "kurume_inundated_union",
                "kurume_non_inundated_union",
                "scene_all",
                "inundated_minus_non_inundated",
            ]
        ]
        .sort_values(["delay_from_rain_start_h", "rain_day_jst", "pair_no"])
        .rename(
            columns={
                "delay_from_rain_start_h": "elapsed_h",
                "kurume_inundated_union": "inundated_abs_mean",
                "kurume_non_inundated_union": "non_inundated_abs_mean",
                "scene_all": "scene_all_abs_mean",
                "inundated_minus_non_inundated": "inundated_minus_non_abs_mean",
            }
        )
    )
    pair_table.to_csv(out_dir / "pair_elapsed_time_status_table.csv", index=False, encoding="utf-8-sig")

    # Area x elapsed summary for each inundation area and non-inundated counterpart.
    area_elapsed = (
        abs_stats.groupby(["area", "elapsed_bin"], observed=True)
        .agg(
            valid_pair_count=("pair_no", "count"),
            total_pixel_count=("count", "sum"),
            abs_diff_mean=("mean", "mean"),
            abs_diff_median=("p50", "mean"),
            abs_diff_p75=("p75", "mean"),
            elapsed_min_h=("delay_from_rain_start_h", "min"),
            elapsed_max_h=("delay_from_rain_start_h", "max"),
        )
        .reset_index()
    )
    area_elapsed["area_order"] = area_elapsed["area"].map(area_sort_key)
    area_elapsed = area_elapsed.sort_values(["area_order", "elapsed_bin"]).drop(columns=["area_order"])
    area_elapsed.to_csv(out_dir / "area_elapsed_abs_diff_summary.csv", index=False, encoding="utf-8-sig")

    # Latest-ish / slow-drainage candidate table: use later bins first, then high residual.
    late = area_elapsed[area_elapsed["elapsed_bin"].isin(["6-12h", "12-24h"])].copy()
    candidates = (
        late[late["area"].str.endswith("_inundated") | (late["area"] == "kurume_inundated_union")]
        .sort_values(["elapsed_bin", "abs_diff_median"], ascending=[True, False])
        .copy()
    )
    candidates.to_csv(out_dir / "late_elapsed_high_residual_candidates.csv", index=False, encoding="utf-8-sig")

    # Residual class count by area.
    class_counts = (
        residual[residual["count"] > 0]
        .groupby(["area", "residual_class"], observed=True)
        .size()
        .reset_index(name="pair_count")
        .sort_values(["area", "residual_class"])
    )
    class_counts.to_csv(out_dir / "residual_class_counts_by_area.csv", index=False, encoding="utf-8-sig")

    # Per-area inundated vs non-inundated comparison.
    per_area = abs_stats[
        abs_stats["area"].str.endswith("_inundated") | abs_stats["area"].str.endswith("_non_inundated")
    ].copy()
    per_area["base_area"] = per_area["area"].str.replace("_non_inundated$", "", regex=True).str.replace(
        "_inundated$", "", regex=True
    )
    per_area["zone"] = np.where(per_area["area"].str.endswith("_non_inundated"), "non_inundated", "inundated")
    area_pair = per_area.pivot_table(
        index=["base_area", "rain_day_jst", "pair_no", "delay_from_rain_start_h", "elapsed_bin"],
        columns="zone",
        values="mean",
        aggfunc="first",
    ).reset_index()
    if {"inundated", "non_inundated"}.issubset(area_pair.columns):
        area_pair["inundated_minus_non_abs_mean"] = area_pair["inundated"] - area_pair["non_inundated"]
    area_pair.to_csv(out_dir / "per_area_inundated_vs_non_inundated_by_pair.csv", index=False, encoding="utf-8-sig")

    # Plots.
    import matplotlib.pyplot as plt

    # Pair elapsed timeline.
    fig, ax = plt.subplots(figsize=(10, 5), dpi=160)
    x = pair_table["elapsed_h"]
    ax.scatter(x, pair_table["scene_all_abs_mean"], label="scene_all", alpha=0.75)
    ax.scatter(x, pair_table["inundated_abs_mean"], label="inundated", alpha=0.85)
    ax.scatter(x, pair_table["non_inundated_abs_mean"], label="non_inundated", alpha=0.85)
    for _, row in pair_table.iterrows():
        ax.text(row["elapsed_h"], row["scene_all_abs_mean"], f"{row['rain_day_jst']}\n{row['pair_no']}", fontsize=6)
    ax.set_xlabel("Hours from rainfall start")
    ax.set_ylabel("Mean |target - before|")
    ax.set_title("Kurume pair status by elapsed time")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "pair_elapsed_time_status_scatter.png")
    plt.close(fig)

    # Elapsed-bin comparison for union zones.
    fig, ax = plt.subplots(figsize=(8, 4), dpi=160)
    labels = elapsed_comp["elapsed_bin"].astype(str)
    positions = np.arange(len(labels))
    width = 0.28
    ax.bar(positions - width, elapsed_comp["inundated_abs_mean"], width, label="inundated")
    ax.bar(positions, elapsed_comp["non_inundated_abs_mean"], width, label="non_inundated")
    ax.bar(positions + width, elapsed_comp["scene_all_abs_mean"], width, label="scene_all")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    ax.set_xlabel("Elapsed bin")
    ax.set_ylabel("Mean |target - before|")
    ax.set_title("Kurume residual by elapsed bin")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "elapsed_bin_union_zone_comparison.png")
    plt.close(fig)

    # Heatmap: inundated areas by elapsed bin.
    heat = area_elapsed[
        area_elapsed["area"].str.endswith("_inundated") | (area_elapsed["area"] == "kurume_inundated_union")
    ].pivot(index="area", columns="elapsed_bin", values="abs_diff_median")
    heat = heat.reindex(sorted(heat.index, key=area_sort_key))
    fig, ax = plt.subplots(figsize=(8, max(4, 0.35 * len(heat))), dpi=160)
    image = ax.imshow(heat.to_numpy(dtype=float), aspect="auto", cmap="viridis")
    ax.set_xticks(np.arange(len(heat.columns)))
    ax.set_xticklabels(heat.columns.astype(str), rotation=45, ha="right")
    ax.set_yticks(np.arange(len(heat.index)))
    ax.set_yticklabels(heat.index)
    ax.set_title("Inundated-area median |target - before| by elapsed bin")
    fig.colorbar(image, ax=ax, label="Median abs diff")
    fig.tight_layout()
    fig.savefig(out_dir / "inundated_area_elapsed_heatmap.png")
    plt.close(fig)

    # Heatmap: inundated minus non-inundated by area/pair.
    if "inundated_minus_non_abs_mean" in area_pair.columns:
        diff_heat = area_pair.pivot_table(
            index="base_area",
            columns=["rain_day_jst", "pair_no"],
            values="inundated_minus_non_abs_mean",
            aggfunc="first",
        )
        diff_heat = diff_heat.reindex(sorted(diff_heat.index))
        fig, ax = plt.subplots(figsize=(12, max(4, 0.35 * len(diff_heat))), dpi=160)
        matrix = diff_heat.to_numpy(dtype=float)
        vmax = np.nanmax(np.abs(matrix)) if np.isfinite(matrix).any() else 1
        image = ax.imshow(matrix, aspect="auto", cmap="coolwarm", vmin=-vmax, vmax=vmax)
        ax.set_xticks(np.arange(len(diff_heat.columns)))
        ax.set_xticklabels([f"{a}\n{b}" for a, b in diff_heat.columns], rotation=90, fontsize=7)
        ax.set_yticks(np.arange(len(diff_heat.index)))
        ax.set_yticklabels(diff_heat.index)
        ax.set_title("Inundated minus non-inundated mean abs diff")
        fig.colorbar(image, ax=ax, label="Mean abs diff difference")
        fig.tight_layout()
        fig.savefig(out_dir / "per_area_inundated_minus_non_heatmap.png")
        plt.close(fig)

    print(out_dir)
    print(pair_table.to_string(index=False))
    print(elapsed_comp.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
