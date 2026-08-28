#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Analyze map7 backscatter differences by elapsed hours from rainfall start."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_RAIN_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"
DEFAULT_STATS_DIR = DEFAULT_RAIN_DIR / "backscatter_stats"


ELAPSED_BINS = [0, 3, 6, 12, 24, 48]
ELAPSED_LABELS = ["0-3h", "3-6h", "6-12h", "12-24h", "24-48h"]


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    valid = values.notna() & weights.notna() & (weights > 0)
    if not valid.any():
        return float("nan")
    return float(np.average(values[valid], weights=weights[valid]))


def weighted_std(values: pd.Series, weights: pd.Series) -> float:
    valid = values.notna() & weights.notna() & (weights > 0)
    if not valid.any():
        return float("nan")
    mean = np.average(values[valid], weights=weights[valid])
    variance = np.average((values[valid] - mean) ** 2, weights=weights[valid])
    return float(np.sqrt(max(0.0, variance)))


def add_tests(rows: list[dict], mask: str, df: pd.DataFrame) -> None:
    valid = df.dropna(subset=["delay_from_rain_start_h", "mean"]).copy()
    if len(valid) < 3:
        rows.append({"mask": mask, "test": "not_enough_data", "n": len(valid)})
        return

    try:
        from scipy import stats

        pearson = stats.pearsonr(valid["delay_from_rain_start_h"], valid["mean"])
        spearman = stats.spearmanr(valid["delay_from_rain_start_h"], valid["mean"])
        rows.append(
            {
                "mask": mask,
                "test": "pearson_elapsed_vs_diff_mean",
                "n": len(valid),
                "statistic": float(pearson.statistic),
                "p_value": float(pearson.pvalue),
            }
        )
        rows.append(
            {
                "mask": mask,
                "test": "spearman_elapsed_vs_diff_mean",
                "n": len(valid),
                "statistic": float(spearman.statistic),
                "p_value": float(spearman.pvalue),
            }
        )

        groups = [
            group["mean"].dropna().to_numpy()
            for _, group in valid.groupby("elapsed_bin", observed=True)
            if len(group["mean"].dropna()) >= 2
        ]
        if len(groups) >= 2:
            kruskal = stats.kruskal(*groups)
            rows.append(
                {
                    "mask": mask,
                    "test": "kruskal_diff_mean_by_elapsed_bin",
                    "n": len(valid),
                    "group_count": len(groups),
                    "statistic": float(kruskal.statistic),
                    "p_value": float(kruskal.pvalue),
                }
            )
        else:
            rows.append(
                {
                    "mask": mask,
                    "test": "kruskal_diff_mean_by_elapsed_bin",
                    "n": len(valid),
                    "group_count": len(groups),
                    "note": "not_enough_bins_with_at_least_2_pairs",
                }
            )
    except Exception as exc:
        rows.append({"mask": mask, "test": "skipped_scipy_tests", "n": len(valid), "note": str(exc)})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rain-dir", type=Path, default=DEFAULT_RAIN_DIR)
    parser.add_argument("--stats-dir", type=Path, default=DEFAULT_STATS_DIR)
    args = parser.parse_args()

    stats_path = args.stats_dir / "map7_backscatter_stats_by_mask.csv"
    delay_path = args.rain_dir / "map7_db_rain_days_s1_delay_all_jst.csv"
    output_dir = args.stats_dir / "elapsed_time_analysis"
    output_dir.mkdir(parents=True, exist_ok=True)

    stats_df = pd.read_csv(stats_path)
    hist_df = pd.read_csv(args.stats_dir / "map7_backscatter_histograms_by_mask.csv")
    delay_df = pd.read_csv(delay_path)

    diff_df = stats_df[(stats_df["value_type"] == "diff") & (stats_df["count"] > 0)].copy()
    delay_cols = [
        "stac_id",
        "acquisition_time_jst",
        "delay_from_rain_start_h",
        "delay_from_rain_end_h",
        "timing",
        "max_mm_h",
        "point_records",
        "grid_count",
    ]
    merged = diff_df.merge(
        delay_df[delay_cols],
        left_on="target_stac_id",
        right_on="stac_id",
        how="left",
        validate="many_to_one",
    )
    merged["elapsed_bin"] = pd.cut(
        merged["delay_from_rain_start_h"],
        bins=ELAPSED_BINS,
        labels=ELAPSED_LABELS,
        right=False,
        include_lowest=True,
    )
    merged = merged.sort_values(["mask", "delay_from_rain_start_h", "rain_day_jst", "pair_no"])

    pair_csv = output_dir / "map7_diff_by_pair_elapsed_time.csv"
    merged.to_csv(pair_csv, index=False, encoding="utf-8-sig")

    summary_rows = []
    for (mask, elapsed_bin), group in merged.groupby(["mask", "elapsed_bin"], observed=False):
        valid = group[group["count"] > 0]
        summary_rows.append(
            {
                "mask": mask,
                "elapsed_bin": elapsed_bin,
                "pair_count": int(valid[["rain_day_jst", "pair_no"]].drop_duplicates().shape[0]),
                "total_pixel_count": int(valid["count"].sum()),
                "diff_mean_pair_avg": float(valid["mean"].mean()) if len(valid) else np.nan,
                "diff_mean_pair_median": float(valid["mean"].median()) if len(valid) else np.nan,
                "diff_mean_pixel_weighted": weighted_mean(valid["mean"], valid["count"]),
                "diff_std_pixel_weighted": weighted_std(valid["mean"], valid["count"]),
                "delay_min_h": float(valid["delay_from_rain_start_h"].min()) if len(valid) else np.nan,
                "delay_max_h": float(valid["delay_from_rain_start_h"].max()) if len(valid) else np.nan,
            }
        )
    bin_summary = pd.DataFrame(summary_rows)
    bin_csv = output_dir / "map7_diff_elapsed_bin_summary.csv"
    bin_summary.to_csv(bin_csv, index=False, encoding="utf-8-sig")

    test_rows: list[dict] = []
    for mask, group in merged.groupby("mask"):
        add_tests(test_rows, mask, group)
    tests_df = pd.DataFrame(test_rows)
    tests_csv = output_dir / "map7_diff_elapsed_tests.csv"
    tests_df.to_csv(tests_csv, index=False, encoding="utf-8-sig")

    hist_merged = hist_df.merge(
        delay_df[["stac_id", "delay_from_rain_start_h", "timing"]],
        left_on="target_stac_id",
        right_on="stac_id",
        how="left",
        validate="many_to_one",
    )
    hist_merged["elapsed_bin"] = pd.cut(
        hist_merged["delay_from_rain_start_h"],
        bins=ELAPSED_BINS,
        labels=ELAPSED_LABELS,
        right=False,
        include_lowest=True,
    )
    elapsed_hist = (
        hist_merged.groupby(["mask", "elapsed_bin", "value_type", "bin_left", "bin_right"], observed=True)[
            "count"
        ]
        .sum()
        .reset_index()
    )
    elapsed_hist["frequency"] = elapsed_hist["count"] / elapsed_hist.groupby(
        ["mask", "elapsed_bin", "value_type"], observed=True
    )["count"].transform("sum")
    elapsed_hist_csv = output_dir / "map7_backscatter_histograms_by_elapsed_bin.csv"
    elapsed_hist.to_csv(elapsed_hist_csv, index=False, encoding="utf-8-sig")

    try:
        import matplotlib.pyplot as plt

        for mask, group in merged.groupby("mask"):
            group = group.dropna(subset=["delay_from_rain_start_h", "mean"])
            fig, axes = plt.subplots(1, 2, figsize=(12, 4), dpi=160)

            ax = axes[0]
            sizes = np.clip(group["count"].to_numpy(dtype=float) / 50000.0, 15, 160)
            ax.scatter(group["delay_from_rain_start_h"], group["mean"], s=sizes, alpha=0.75)
            ax.axhline(0, color="black", linewidth=0.8)
            ax.set_title(f"{mask}: diff mean by elapsed time")
            ax.set_xlabel("Hours from rainfall start")
            ax.set_ylabel("Mean diff (target - before)")
            ax.grid(True, alpha=0.3)

            ax = axes[1]
            data = []
            labels = []
            for label in ELAPSED_LABELS:
                values = group[group["elapsed_bin"].astype(str) == label]["mean"].dropna().to_numpy()
                if len(values):
                    data.append(values)
                    labels.append(label)
            if data:
                ax.boxplot(data, tick_labels=labels, showmeans=True)
            ax.axhline(0, color="black", linewidth=0.8)
            ax.set_title(f"{mask}: diff mean by elapsed bin")
            ax.set_xlabel("Elapsed bin")
            ax.set_ylabel("Mean diff (target - before)")
            ax.grid(True, axis="y", alpha=0.3)

            fig.tight_layout()
            fig.savefig(output_dir / f"map7_diff_elapsed_{mask}.png")
            plt.close(fig)

        for mask in sorted(elapsed_hist["mask"].dropna().unique()):
            for value_type in ["target", "before", "diff"]:
                fig, ax = plt.subplots(figsize=(8, 4), dpi=160)
                for label in ELAPSED_LABELS:
                    g = elapsed_hist[
                        (elapsed_hist["mask"] == mask)
                        & (elapsed_hist["value_type"] == value_type)
                        & (elapsed_hist["elapsed_bin"].astype(str) == label)
                    ]
                    if g.empty or g["count"].sum() == 0:
                        continue
                    centers = (g["bin_left"] + g["bin_right"]) / 2.0
                    ax.plot(centers, g["frequency"], label=label, linewidth=1.6)
                if value_type == "diff":
                    ax.axvline(0, color="black", linewidth=0.8)
                    ax.set_xlabel("Difference (target - before)")
                else:
                    ax.set_xlabel("Backscatter")
                ax.set_title(f"{mask}: {value_type} distribution by elapsed bin")
                ax.set_ylabel("Frequency")
                ax.grid(True, alpha=0.3)
                handles, labels = ax.get_legend_handles_labels()
                if handles:
                    ax.legend(handles, labels)
                fig.tight_layout()
                fig.savefig(output_dir / f"map7_{value_type}_hist_by_elapsed_bin_{mask}.png")
                plt.close(fig)
    except Exception as exc:
        print(f"plot skipped: {exc}")

    summary = {
        "pair_elapsed_csv": str(pair_csv),
        "elapsed_bin_summary_csv": str(bin_csv),
        "tests_csv": str(tests_csv),
        "elapsed_histogram_csv": str(elapsed_hist_csv),
        "elapsed_definition": "delay_from_rain_start_h = target acquisition time - rainfall first timestamp",
        "difference_definition": "diff = target - before",
        "elapsed_bins_h": ELAPSED_BINS,
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(bin_summary.to_string(index=False))
    print(tests_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
