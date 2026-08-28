#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Export per-pixel backscatter, pair differences, and inundation labels as rasters."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT


ROOT = Path(__file__).resolve().parents[1]
RAIN_DIR = ROOT / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"
PAIR_DIR = RAIN_DIR / "processed_by_date"
DEFAULT_KURUME_DIR = Path(r"D:\sotsuron\kurume")
DEFAULT_OUTPUT_DIR = RAIN_DIR / "kurume_inundation_analysis" / "map7_detection_test" / "pixel_backscatter_labeled_rasters"

MASK_MIN = 0.5
MASK_MAX = 1.7
ELAPSED = ["0-3h", "3-6h", "6-12h", "12-24h"]


def elapsed_bin(delay_h: float) -> str | None:
    if 0 <= delay_h < 3:
        return "0-3h"
    if 3 <= delay_h < 6:
        return "3-6h"
    if 6 <= delay_h < 12:
        return "6-12h"
    if 12 <= delay_h <= 24:
        return "12-24h"
    return None


def read_manifest(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    grouped: dict[tuple[str, str], dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            key = (row["rain_day_jst"], row["pair_no"])
            grouped.setdefault(key, {})[row["role"]] = row["organized_path"]
            grouped[key][f"{row['role']}_stac_id"] = row["stac_id"]
    return grouped


def valid_values(arr: np.ndarray, nodata) -> np.ndarray:
    arr = arr.astype(np.float32, copy=False)
    valid = np.isfinite(arr)
    if nodata is not None:
        valid &= arr != nodata
    # SNAP/clip outputs in this project use exact 0 as outside-valid-data fill.
    valid &= arr != 0
    out = arr.copy()
    out[~valid] = np.nan
    return out


def truth_union_on_grid(mask_paths: list[Path], template) -> np.ndarray:
    union = np.zeros((template.height, template.width), dtype=bool)
    for path in mask_paths:
        with rasterio.open(path) as src:
            with WarpedVRT(
                src,
                crs=template.crs,
                transform=template.transform,
                width=template.width,
                height=template.height,
                resampling=Resampling.nearest,
                nodata=src.nodata,
            ) as vrt:
                arr = vrt.read(1).astype(np.float32)
                valid = np.isfinite(arr)
                if vrt.nodata is not None:
                    valid &= arr != vrt.nodata
                union |= valid & (arr >= MASK_MIN) & (arr <= MASK_MAX)
    return union


def output_profile(src, output_path: Path) -> dict:
    profile = src.profile.copy()
    profile.update(
        driver="GTiff",
        count=4,
        dtype="float32",
        nodata=np.nan,
        compress="deflate",
        predictor=3,
        tiled=True,
        blockxsize=512,
        blockysize=512,
        BIGTIFF="IF_SAFER",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return profile


def export_pair(
    target_path: Path,
    before_path: Path,
    mask_paths: list[Path],
    output_tif: Path,
) -> dict[str, int | float | str]:
    valid_count = 0
    truth_count = 0
    background_count = 0
    diff_sum = 0.0
    diff_sumsq = 0.0
    diff_min = np.inf
    diff_max = -np.inf

    with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
        truth = truth_union_on_grid(mask_paths, target_src)
        same_grid = (
            target_src.crs == before_src.crs
            and target_src.transform == before_src.transform
            and target_src.width == before_src.width
            and target_src.height == before_src.height
        )

        with rasterio.open(output_tif, "w", **output_profile(target_src, output_tif)) as dst:
            dst.set_band_description(1, "before_backscatter_db")
            dst.set_band_description(2, "target_backscatter_db")
            dst.set_band_description(3, "diff_target_minus_before_db")
            dst.set_band_description(4, "inundation_label_0_or_1")

            if same_grid:
                before_reader = before_src
                vrt_ctx = None
            else:
                vrt_ctx = WarpedVRT(
                    before_src,
                    crs=target_src.crs,
                    transform=target_src.transform,
                    width=target_src.width,
                    height=target_src.height,
                    resampling=Resampling.bilinear,
                    nodata=before_src.nodata,
                )
                before_reader = vrt_ctx

            try:
                for _, window in target_src.block_windows(1):
                    target = valid_values(target_src.read(1, window=window), target_src.nodata)
                    before = valid_values(before_reader.read(1, window=window), before_reader.nodata)
                    valid = np.isfinite(target) & np.isfinite(before)
                    diff = np.full(target.shape, np.nan, dtype=np.float32)
                    diff[valid] = target[valid] - before[valid]

                    truth_block = truth[
                        int(window.row_off) : int(window.row_off + window.height),
                        int(window.col_off) : int(window.col_off + window.width),
                    ]
                    label = np.full(target.shape, np.nan, dtype=np.float32)
                    label[valid] = truth_block[valid].astype(np.float32)

                    before_out = np.where(valid, before, np.nan).astype(np.float32)
                    target_out = np.where(valid, target, np.nan).astype(np.float32)

                    dst.write(before_out, 1, window=window)
                    dst.write(target_out, 2, window=window)
                    dst.write(diff, 3, window=window)
                    dst.write(label, 4, window=window)

                    vals = diff[valid]
                    if vals.size:
                        valid_count += int(vals.size)
                        tcount = int(np.sum(truth_block[valid]))
                        truth_count += tcount
                        background_count += int(vals.size) - tcount
                        vals64 = vals.astype(np.float64, copy=False)
                        diff_sum += float(np.sum(vals64))
                        diff_sumsq += float(np.sum(vals64 * vals64))
                        diff_min = min(diff_min, float(np.min(vals)))
                        diff_max = max(diff_max, float(np.max(vals)))
            finally:
                if vrt_ctx is not None:
                    vrt_ctx.close()

    mean = diff_sum / valid_count if valid_count else np.nan
    std = np.sqrt(max(diff_sumsq / valid_count - mean * mean, 0.0)) if valid_count else np.nan
    return {
        "output_tif": str(output_tif),
        "valid_pixel_count": valid_count,
        "inundation_pixel_count": truth_count,
        "background_pixel_count": background_count,
        "diff_mean": mean,
        "diff_std": std,
        "diff_min": diff_min if valid_count else np.nan,
        "diff_max": diff_max if valid_count else np.nan,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-dir", type=Path, default=PAIR_DIR)
    parser.add_argument("--rain-dir", type=Path, default=RAIN_DIR)
    parser.add_argument("--kurume-dir", type=Path, default=DEFAULT_KURUME_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-year", type=int, default=2022)
    parser.add_argument("--include-empty", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    mask_paths = sorted(args.kurume_dir.glob("*.tif"))
    if not mask_paths:
        raise RuntimeError(f"浸水域TIFが見つかりません: {args.kurume_dir}")

    manifest = read_manifest(args.pair_dir / "manifest.csv")
    delay_df = pd.read_csv(args.rain_dir / "map7_db_rain_days_s1_delay_all_jst.csv", encoding="utf-8-sig")
    delay_by_stac = delay_df.set_index("stac_id")["delay_from_rain_start_h"].to_dict()

    rows = []
    for (rain_day, pair_no), roles in sorted(manifest.items()):
        if args.max_year and int(rain_day[:4]) > args.max_year:
            continue
        if "target" not in roles or "pair" not in roles:
            continue
        target_path = Path(roles["target"])
        before_path = Path(roles["pair"])
        if not target_path.exists() or not before_path.exists():
            rows.append(
                {
                    "rain_day_jst": rain_day,
                    "pair_no": pair_no,
                    "status": "missing_input",
                    "target_path": str(target_path),
                    "before_path": str(before_path),
                }
            )
            continue

        delay = float(delay_by_stac.get(roles.get("target_stac_id", ""), np.nan))
        label = elapsed_bin(delay)
        if label is None:
            continue

        output_tif = args.output_dir / rain_day / pair_no / "pixel_backscatter_diff_label_stack.tif"
        stats = export_pair(target_path, before_path, mask_paths, output_tif)
        status = "exported"
        if stats["valid_pixel_count"] == 0 and not args.include_empty:
            status = "empty_valid_pixels"

        row = {
            "rain_day_jst": rain_day,
            "pair_no": pair_no,
            "elapsed_h": delay,
            "elapsed_bin": label,
            "target_stac_id": roles.get("target_stac_id", ""),
            "before_stac_id": roles.get("pair_stac_id", ""),
            "target_path": str(target_path),
            "before_path": str(before_path),
            "status": status,
        }
        row.update(stats)
        rows.append(row)
        print(f"{status}: {rain_day} {pair_no} valid={stats['valid_pixel_count']} label={stats['inundation_pixel_count']}")

    df = pd.DataFrame(rows)
    manifest_path = args.output_dir / "pixel_backscatter_diff_label_manifest.csv"
    df.to_csv(manifest_path, index=False, encoding="utf-8-sig")

    band_info = pd.DataFrame(
        [
            {"band": 1, "name": "before_backscatter_db", "description": "ペア画像、降雨前の後方散乱強度 dB"},
            {"band": 2, "name": "target_backscatter_db", "description": "降雨後/対象画像の後方散乱強度 dB"},
            {"band": 3, "name": "diff_target_minus_before_db", "description": "target - before の差分 dB"},
            {"band": 4, "name": "inundation_label_0_or_1", "description": "Kurume*_inun.tif の 0.5-1.7 を浸水域=1、それ以外=0"},
        ]
    )
    band_info.to_csv(args.output_dir / "pixel_backscatter_diff_label_band_info.csv", index=False, encoding="utf-8-sig")
    print(f"saved manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
