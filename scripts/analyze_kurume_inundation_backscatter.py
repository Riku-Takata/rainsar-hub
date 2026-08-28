#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Analyze backscatter difference in and around Kurume inundation rasters up to 2022.

Kurume TIFs are used as labels, not as exclusive masks:
- scene_all: all valid pixels in the pair scene
- kurume_inundated_union: pixels with inundation depth > threshold in any Kurume TIF
- kurume_non_inundated_union: valid Kurume TIF footprint pixels without inundation
- <KurumeXX>_inundated / <KurumeXX>_non_inundated: per-raster zones
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PAIR_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed_by_date"
DEFAULT_RAIN_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"
DEFAULT_KURUME_DIR = Path(r"D:\sotsuron\kurume")
DEFAULT_OUTPUT_DIR = DEFAULT_RAIN_DIR / "kurume_inundation_analysis"

DIFF_BINS = np.arange(-15.0, 15.5, 0.5)
ABS_DIFF_BINS = np.arange(0.0, 15.5, 0.5)
ELAPSED_BINS = [0, 3, 6, 12, 24, 48]
ELAPSED_LABELS = ["0-3h", "3-6h", "6-12h", "12-24h", "24-48h"]


@dataclass
class RunningStats:
    count: int = 0
    sum_value: float = 0.0
    sum_square: float = 0.0
    min_value: float = math.inf
    max_value: float = -math.inf

    def update(self, values: np.ndarray) -> None:
        if values.size == 0:
            return
        values = values.astype("float64", copy=False)
        self.count += int(values.size)
        self.sum_value += float(values.sum())
        self.sum_square += float(np.square(values).sum())
        self.min_value = min(self.min_value, float(values.min()))
        self.max_value = max(self.max_value, float(values.max()))

    def as_dict(self) -> dict[str, Any]:
        if self.count == 0:
            return {"count": 0, "mean": None, "std": None, "min": None, "max": None}
        mean = self.sum_value / self.count
        var = max(0.0, self.sum_square / self.count - mean * mean)
        return {
            "count": self.count,
            "mean": mean,
            "std": math.sqrt(var),
            "min": self.min_value,
            "max": self.max_value,
        }


def percentile_from_hist(hist: np.ndarray, bins: np.ndarray, percentile: float) -> float | None:
    total = int(hist.sum())
    if total == 0:
        return None
    threshold = total * percentile / 100.0
    cumulative = np.cumsum(hist)
    idx = int(np.searchsorted(cumulative, threshold, side="left"))
    idx = min(idx, len(hist) - 1)
    prev = float(cumulative[idx - 1]) if idx > 0 else 0.0
    count = float(hist[idx])
    if count <= 0:
        return float((bins[idx] + bins[idx + 1]) / 2.0)
    fraction = min(1.0, max(0.0, (threshold - prev) / count))
    return float(bins[idx] + fraction * (bins[idx + 1] - bins[idx]))


def read_manifest(path: Path) -> dict[tuple[str, str], dict[str, dict[str, str]]]:
    grouped: dict[tuple[str, str], dict[str, dict[str, str]]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            key = (row["rain_day_jst"], row["pair_no"])
            grouped.setdefault(key, {})[row["role"]] = row
    return grouped


def init_accumulators() -> dict[str, Any]:
    return {
        "diff": RunningStats(),
        "abs_diff": RunningStats(),
        "diff_hist": np.zeros(len(DIFF_BINS) - 1, dtype=np.int64),
        "abs_diff_hist": np.zeros(len(ABS_DIFF_BINS) - 1, dtype=np.int64),
    }


def update_acc(acc: dict[str, Any], diff_values: np.ndarray) -> None:
    if diff_values.size == 0:
        return
    abs_values = np.abs(diff_values)
    acc["diff"].update(diff_values)
    acc["abs_diff"].update(abs_values)
    acc["diff_hist"] += np.histogram(diff_values, bins=DIFF_BINS)[0]
    acc["abs_diff_hist"] += np.histogram(abs_values, bins=ABS_DIFF_BINS)[0]


def finalize_acc(acc: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for value_type, bins, hist in [
        ("diff", DIFF_BINS, acc["diff_hist"]),
        ("abs_diff", ABS_DIFF_BINS, acc["abs_diff_hist"]),
    ]:
        stats = acc[value_type].as_dict()
        for p in [5, 25, 50, 75, 95]:
            stats[f"p{p:02d}"] = percentile_from_hist(hist, bins, p)
        out[value_type] = stats
    return out


def calculate_pair(
    target_path: Path,
    before_path: Path,
    mask_paths: list[Path],
    mask_min: float,
    mask_max: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    area_acc: dict[str, dict[str, Any]] = {
        "scene_all": init_accumulators(),
        "kurume_inundated_union": init_accumulators(),
        "kurume_non_inundated_union": init_accumulators(),
    }
    for path in mask_paths:
        area_acc[f"{path.stem}_inundated"] = init_accumulators()
        area_acc[f"{path.stem}_non_inundated"] = init_accumulators()

    with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
        mask_sources = [rasterio.open(path) for path in mask_paths]
        try:
            mask_vrts = [
                WarpedVRT(
                    src,
                    crs=target_src.crs,
                    transform=target_src.transform,
                    width=target_src.width,
                    height=target_src.height,
                    resampling=Resampling.nearest,
                    nodata=np.nan,
                )
                for src in mask_sources
            ]
            before_vrt = WarpedVRT(
                before_src,
                crs=target_src.crs,
                transform=target_src.transform,
                width=target_src.width,
                height=target_src.height,
                resampling=Resampling.bilinear,
                nodata=np.nan,
            )
            try:
                for _, window in target_src.block_windows(1):
                    target_arr = target_src.read(1, window=window, masked=False).astype("float32", copy=False)
                    before_arr = before_vrt.read(1, window=window, masked=False).astype("float32", copy=False)
                    valid = np.isfinite(target_arr) & np.isfinite(before_arr)
                    if target_src.nodata is not None:
                        valid &= target_arr != target_src.nodata
                    if before_src.nodata is not None:
                        valid &= before_arr != before_src.nodata
                    if not valid.any():
                        continue

                    diff = target_arr - before_arr
                    update_acc(area_acc["scene_all"], diff[valid])

                    union_footprint = np.zeros((window.height, window.width), dtype=bool)
                    union_inundated = np.zeros((window.height, window.width), dtype=bool)
                    for path, vrt in zip(mask_paths, mask_vrts):
                        mask_arr = vrt.read(1, window=window, masked=False)
                        footprint = np.isfinite(mask_arr)
                        if not footprint.any():
                            continue
                        inundated = footprint & (mask_arr >= mask_min) & (mask_arr <= mask_max)
                        non_inundated = footprint & ~inundated
                        union_footprint |= footprint
                        union_inundated |= inundated
                        update_acc(area_acc[f"{path.stem}_inundated"], diff[inundated & valid])
                        update_acc(area_acc[f"{path.stem}_non_inundated"], diff[non_inundated & valid])

                    if not union_footprint.any():
                        continue

                    union_non_inundated = union_footprint & ~union_inundated
                    update_acc(area_acc["kurume_inundated_union"], diff[union_inundated & valid])
                    update_acc(area_acc["kurume_non_inundated_union"], diff[union_non_inundated & valid])
            finally:
                before_vrt.close()
                for vrt in mask_vrts:
                    vrt.close()
        finally:
            for src in mask_sources:
                src.close()

    stat_rows: list[dict[str, Any]] = []
    hist_rows: list[dict[str, Any]] = []
    for area_name, acc in area_acc.items():
        finalized = finalize_acc(acc)
        for value_type, stats in finalized.items():
            stat_rows.append({"area": area_name, "value_type": value_type, **stats})
            bins = ABS_DIFF_BINS if value_type == "abs_diff" else DIFF_BINS
            hist = acc["abs_diff_hist"] if value_type == "abs_diff" else acc["diff_hist"]
            total = int(hist.sum())
            for idx, count in enumerate(hist):
                hist_rows.append(
                    {
                        "area": area_name,
                        "value_type": value_type,
                        "bin_left": float(bins[idx]),
                        "bin_right": float(bins[idx + 1]),
                        "count": int(count),
                        "frequency": float(count) / total if total else 0.0,
                    }
                )
    return stat_rows, hist_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-dir", type=Path, default=DEFAULT_PAIR_DIR)
    parser.add_argument("--rain-dir", type=Path, default=DEFAULT_RAIN_DIR)
    parser.add_argument("--kurume-dir", type=Path, default=DEFAULT_KURUME_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-year", type=int, default=2022)
    parser.add_argument("--mask-min", type=float, default=0.5)
    parser.add_argument("--mask-max", type=float, default=1.7)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    mask_paths = sorted(args.kurume_dir.glob("*.tif"))
    if not mask_paths:
        raise FileNotFoundError(f"No Kurume inundation TIFs found: {args.kurume_dir}")

    manifest = read_manifest(args.pair_dir / "manifest.csv")
    delay_df = pd.read_csv(args.rain_dir / "map7_db_rain_days_s1_delay_all_jst.csv")
    delay_cols = [
        "stac_id",
        "acquisition_time_jst",
        "delay_from_rain_start_h",
        "delay_from_rain_end_h",
        "timing",
        "max_mm_h",
    ]

    stat_rows: list[dict[str, Any]] = []
    hist_rows: list[dict[str, Any]] = []
    selected = [
        (key, roles)
        for key, roles in sorted(manifest.items())
        if "target" in roles
        and "pair" in roles
        and int(roles["target"]["rain_day_jst"][:4]) <= args.max_year
    ]

    for index, ((rain_day, pair_no), roles) in enumerate(selected, start=1):
        print(f"[{index}/{len(selected)}] {rain_day} {pair_no}")
        target_path = Path(roles["target"]["organized_path"])
        before_path = Path(roles["pair"]["organized_path"])
        pair_stats, pair_hists = calculate_pair(target_path, before_path, mask_paths, args.mask_min, args.mask_max)
        base = {
            "rain_day_jst": rain_day,
            "pair_no": pair_no,
            "target_stac_id": roles["target"]["stac_id"],
            "before_stac_id": roles["pair"]["stac_id"],
            "rain_first_ts_jst": roles["target"].get("rain_first_ts_jst"),
            "rain_last_ts_jst": roles["target"].get("rain_last_ts_jst"),
            "before_day_jst": roles["target"].get("pair_no_heavy_rain_day_jst"),
        }
        for row in pair_stats:
            stat_rows.append({**base, **row})
        for row in pair_hists:
            hist_rows.append({**base, **row})

    stats_df = pd.DataFrame(stat_rows)
    hist_df = pd.DataFrame(hist_rows)
    stats_df = stats_df.merge(
        delay_df[delay_cols],
        left_on="target_stac_id",
        right_on="stac_id",
        how="left",
        validate="many_to_one",
    )
    hist_df = hist_df.merge(
        delay_df[["stac_id", "delay_from_rain_start_h"]],
        left_on="target_stac_id",
        right_on="stac_id",
        how="left",
        validate="many_to_one",
    )
    for df in [stats_df, hist_df]:
        df["elapsed_bin"] = pd.cut(
            df["delay_from_rain_start_h"],
            bins=ELAPSED_BINS,
            labels=ELAPSED_LABELS,
            right=False,
            include_lowest=True,
        )

    stats_csv = args.output_dir / "kurume_backscatter_stats_by_area_2022.csv"
    hist_csv = args.output_dir / "kurume_backscatter_histograms_by_area_2022.csv"
    stats_df.to_csv(stats_csv, index=False, encoding="utf-8-sig")
    hist_df.to_csv(hist_csv, index=False, encoding="utf-8-sig")

    abs_df = stats_df[(stats_df["value_type"] == "abs_diff") & (stats_df["count"] > 0)].copy()
    union = abs_df[abs_df["area"] == "kurume_inundated_union"][
        ["rain_day_jst", "pair_no", "p25", "p50", "p75"]
    ].rename(columns={"p25": "inundated_union_abs_p25", "p50": "inundated_union_abs_p50", "p75": "inundated_union_abs_p75"})
    classified = abs_df.merge(union, on=["rain_day_jst", "pair_no"], how="left")
    classified["residual_class"] = np.select(
        [
            classified["p50"] >= classified["inundated_union_abs_p75"],
            classified["p50"] <= classified["inundated_union_abs_p25"],
        ],
        ["high_residual_low_drainage_candidate", "low_residual_high_drainage_candidate"],
        default="middle",
    )
    class_csv = args.output_dir / "kurume_abs_diff_residual_class_by_area_2022.csv"
    classified.to_csv(class_csv, index=False, encoding="utf-8-sig")

    bin_summary = (
        abs_df.groupby(["area", "elapsed_bin"], observed=True)
        .agg(
            pair_count=("pair_no", "count"),
            total_pixel_count=("count", "sum"),
            abs_diff_mean_avg=("mean", "mean"),
            abs_diff_median_avg=("p50", "mean"),
            delay_min_h=("delay_from_rain_start_h", "min"),
            delay_max_h=("delay_from_rain_start_h", "max"),
        )
        .reset_index()
    )
    bin_csv = args.output_dir / "kurume_abs_diff_elapsed_bin_summary_2022.csv"
    bin_summary.to_csv(bin_csv, index=False, encoding="utf-8-sig")

    try:
        import matplotlib.pyplot as plt

        union_abs = abs_df[abs_df["area"] == "kurume_inundated_union"].dropna(subset=["delay_from_rain_start_h", "mean"])
        non_inun_abs = abs_df[abs_df["area"] == "kurume_non_inundated_union"].dropna(subset=["delay_from_rain_start_h", "mean"])
        fig, axes = plt.subplots(1, 2, figsize=(12, 4), dpi=160)
        axes[0].scatter(union_abs["delay_from_rain_start_h"], union_abs["mean"], alpha=0.75, label="inundated")
        if not non_inun_abs.empty:
            axes[0].scatter(non_inun_abs["delay_from_rain_start_h"], non_inun_abs["mean"], alpha=0.75, label="non-inundated")
        axes[0].set_title("Kurume: residual abs diff by elapsed time")
        axes[0].set_xlabel("Hours from rainfall start")
        axes[0].set_ylabel("Mean |target - before|")
        axes[0].grid(True, alpha=0.3)
        axes[0].legend()

        data = []
        labels = []
        for label in ELAPSED_LABELS:
            values = union_abs[union_abs["elapsed_bin"].astype(str) == label]["mean"].dropna().to_numpy()
            if len(values):
                data.append(values)
                labels.append(label)
        if data:
            axes[1].boxplot(data, tick_labels=labels, showmeans=True)
        axes[1].set_title("Kurume inundated: residual abs diff by elapsed bin")
        axes[1].set_xlabel("Elapsed bin")
        axes[1].set_ylabel("Mean |target - before|")
        axes[1].grid(True, axis="y", alpha=0.3)
        fig.tight_layout()
        fig.savefig(args.output_dir / "kurume_abs_diff_elapsed_union_2022.png")
        plt.close(fig)
    except Exception as exc:
        print(f"plot skipped: {exc}")

    summary = {
        "max_year": args.max_year,
        "selected_pair_count": len(selected),
        "kurume_mask_count": len(mask_paths),
        "mask_min": args.mask_min,
        "mask_max": args.mask_max,
        "stats_csv": str(stats_csv),
        "histogram_csv": str(hist_csv),
        "residual_class_csv": str(class_csv),
        "elapsed_bin_summary_csv": str(bin_csv),
        "residual_definition": "abs_diff = abs(target - before); high residual in later elapsed bins suggests slower drainage candidate",
        "zone_design": "Kurume TIFs are used as labels. Inundated and non-inundated footprint pixels are both retained.",
        "inundated_condition": f"{args.mask_min} <= inundation_depth <= {args.mask_max}",
        "classification_rule": "area median abs_diff >= same-pair inundated-union p75 => low drainage candidate; <= p25 => high drainage candidate",
    }
    summary_path = args.output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(bin_summary[bin_summary["area"].isin(["kurume_inundated_union", "kurume_non_inundated_union", "scene_all"])].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
