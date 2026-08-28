#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Test how well the map7 difference-time signature detects known Kurume inundation pixels."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window, from_bounds
from rasterio.warp import transform_bounds


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PAIR_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed_by_date"
DEFAULT_RAIN_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"
DEFAULT_KURUME_DIR = Path(r"D:\sotsuron\kurume")
DEFAULT_STATS = (
    DEFAULT_RAIN_DIR / "kurume_inundation_analysis" / "feature_point_sampling" / "feature_sample_statistics.csv"
)
DEFAULT_OUTPUT_DIR = DEFAULT_RAIN_DIR / "kurume_inundation_analysis" / "map7_detection_test"

ELAPSED_LABELS = ["0-3h", "3-6h", "6-12h", "12-24h"]


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


def read_manifest(path: Path) -> dict[tuple[str, str], dict[str, dict[str, str]]]:
    grouped: dict[tuple[str, str], dict[str, dict[str, str]]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            grouped.setdefault((row["rain_day_jst"], row["pair_no"]), {})[row["role"]] = row
    return grouped


def read_reference(stats_csv: Path) -> tuple[pd.DataFrame, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    stats = pd.read_csv(stats_csv, encoding="utf-8-sig")
    inun = stats[stats["area"] == "浸水域"].set_index("elapsed_bin").reindex(ELAPSED_LABELS)
    scene = stats[stats["area"] == "シーン全体"].set_index("elapsed_bin").reindex(ELAPSED_LABELS)
    inun_mean = inun["mean"].to_numpy(dtype=np.float64)
    inun_std = inun["std"].to_numpy(dtype=np.float64)
    scene_mean = scene["mean"].to_numpy(dtype=np.float64)
    scene_std = scene["std"].to_numpy(dtype=np.float64)
    criteria = pd.DataFrame(
        {
            "経過時間帯": ELAPSED_LABELS,
            "久留米浸水域_平均差分": inun_mean,
            "久留米浸水域_標準偏差": inun_std,
            "久留米シーン全体_平均差分": scene_mean,
            "久留米シーン全体_標準偏差": scene_std,
        }
    )
    return criteria, inun_mean, inun_std, scene_mean, scene_std


def normalize_profile(values: np.ndarray) -> np.ndarray:
    std = float(np.nanstd(values))
    if not np.isfinite(std) or std <= 0:
        return np.full(values.shape, np.nan, dtype=np.float64)
    return (values - float(np.nanmean(values))) / std


def corr_with_signature(profile_stack: np.ndarray, signature: np.ndarray) -> np.ndarray:
    sig = normalize_profile(signature)
    out = np.full(profile_stack.shape[1:], np.nan, dtype=np.float32)
    valid = np.all(np.isfinite(profile_stack), axis=0)
    if not np.any(valid):
        return out
    values = profile_stack[:, valid]
    centered = values - np.mean(values, axis=0, keepdims=True)
    std = np.std(values, axis=0)
    corr = np.sum(centered * sig[:, None], axis=0) / (std * profile_stack.shape[0])
    corr[~np.isfinite(corr)] = np.nan
    out[valid] = corr.astype(np.float32)
    return out


def zrmse_to_reference(profile_stack: np.ndarray, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    std = np.where(std > 0, std, 1.0)
    out = np.full(profile_stack.shape[1:], np.nan, dtype=np.float32)
    valid = np.all(np.isfinite(profile_stack), axis=0)
    if not np.any(valid):
        return out
    z = (profile_stack[:, valid] - mean[:, None]) / std[:, None]
    out[valid] = np.sqrt(np.mean(z**2, axis=0)).astype(np.float32)
    return out


def union_mask_bounds(mask_paths: list[Path], target_crs) -> tuple[float, float, float, float]:
    bounds = []
    for path in mask_paths:
        with rasterio.open(path) as src:
            b = src.bounds
            if src.crs and target_crs and src.crs != target_crs:
                b = transform_bounds(src.crs, target_crs, *b, densify_pts=21)
            else:
                b = (b.left, b.bottom, b.right, b.top)
            bounds.append(b)
    return (
        min(b[0] for b in bounds),
        min(b[1] for b in bounds),
        max(b[2] for b in bounds),
        max(b[3] for b in bounds),
    )


def aligned_window_from_bounds(src, bounds: tuple[float, float, float, float], buffer_deg: float) -> Window:
    left, bottom, right, top = bounds
    window = from_bounds(left - buffer_deg, bottom - buffer_deg, right + buffer_deg, top + buffer_deg, src.transform)
    window = window.round_offsets().round_lengths()
    return window.intersection(Window(0, 0, src.width, src.height))


def reproject_inundation_union(
    mask_paths: list[Path],
    template,
    transform,
    width: int,
    height: int,
    mask_min: float,
    mask_max: float,
) -> tuple[np.ndarray, list[dict[str, int | str]]]:
    union = np.zeros((height, width), dtype=bool)
    rows = []
    for path in mask_paths:
        with rasterio.open(path) as src:
            with WarpedVRT(
                src,
                crs=template.crs,
                transform=transform,
                width=width,
                height=height,
                resampling=Resampling.nearest,
                nodata=src.nodata,
            ) as vrt:
                arr = vrt.read(1).astype(np.float32)
            valid = np.isfinite(arr)
            if src.nodata is not None:
                valid &= arr != src.nodata
            mask = valid & (arr >= mask_min) & (arr <= mask_max)
            union |= mask
            rows.append({"浸水TIF": path.name, "再投影後_正解浸水画素数": int(np.sum(mask))})
    rows.append({"浸水TIF": "union", "再投影後_正解浸水画素数": int(np.sum(union))})
    return union, rows


def write_tif(path: Path, template, transform, width: int, height: int, data: np.ndarray, dtype: str, nodata) -> None:
    profile = template.profile.copy()
    profile.update(
        driver="GTiff",
        height=height,
        width=width,
        transform=transform,
        count=1,
        dtype=dtype,
        nodata=nodata,
        compress="deflate",
        tiled=True,
    )
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def zone_profile_stats(zone_name: str, mask: np.ndarray, profiles: list[np.ndarray]) -> list[dict[str, float | int | str]]:
    out = []
    for label, values in zip(ELAPSED_LABELS, profiles):
        sample = values[mask & np.isfinite(values)]
        if sample.size == 0:
            out.append({"領域": zone_name, "経過時間帯": label, "画素数": 0, "平均差分": np.nan, "中央値差分": np.nan})
        else:
            out.append(
                {
                    "領域": zone_name,
                    "経過時間帯": label,
                    "画素数": int(sample.size),
                    "平均差分": float(np.mean(sample)),
                    "中央値差分": float(np.median(sample)),
                    "p25": float(np.percentile(sample, 25)),
                    "p75": float(np.percentile(sample, 75)),
                }
            )
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-dir", type=Path, default=DEFAULT_PAIR_DIR)
    parser.add_argument("--rain-dir", type=Path, default=DEFAULT_RAIN_DIR)
    parser.add_argument("--kurume-dir", type=Path, default=DEFAULT_KURUME_DIR)
    parser.add_argument("--stats-csv", type=Path, default=DEFAULT_STATS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-year", type=int, default=2022)
    parser.add_argument("--mask-min", type=float, default=0.5)
    parser.add_argument("--mask-max", type=float, default=1.7)
    parser.add_argument("--buffer-deg", type=float, default=0.02)
    parser.add_argument("--corr-threshold", type=float, default=0.5)
    parser.add_argument("--zrmse-threshold", type=float, default=1.0)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    criteria, inun_mean, inun_std, scene_mean, scene_std = read_reference(args.stats_csv)
    manifest = read_manifest(args.pair_dir / "manifest.csv")
    delay_df = pd.read_csv(args.rain_dir / "map7_db_rain_days_s1_delay_all_jst.csv", encoding="utf-8-sig")
    delay_by_stac = delay_df.set_index("stac_id")["delay_from_rain_start_h"].to_dict()

    selected = []
    for (rain_day, pair_no), roles in sorted(manifest.items()):
        if "target" not in roles or "pair" not in roles or int(rain_day[:4]) > args.max_year:
            continue
        delay = float(delay_by_stac.get(roles["target"]["stac_id"], np.nan))
        bin_label = elapsed_bin(delay)
        if bin_label is None:
            continue
        selected.append((rain_day, pair_no, roles, delay, bin_label))
    if not selected:
        raise RuntimeError("No usable map7 pairs found.")

    template_path = Path(selected[0][2]["target"]["organized_path"])
    mask_paths = sorted(args.kurume_dir.glob("*.tif"))
    with rasterio.open(template_path) as template:
        bounds = union_mask_bounds(mask_paths, template.crs)
        window = aligned_window_from_bounds(template, bounds, args.buffer_deg)
        transform = template.window_transform(window)
        height = int(window.height)
        width = int(window.width)
        inun_union, mask_rows = reproject_inundation_union(
            mask_paths, template, transform, width, height, args.mask_min, args.mask_max
        )

        sum_by_bin = {label: np.zeros((height, width), dtype=np.float32) for label in ELAPSED_LABELS}
        count_by_bin = {label: np.zeros((height, width), dtype=np.uint16) for label in ELAPSED_LABELS}
        pair_rows = []
        for rain_day, pair_no, roles, delay, bin_label in selected:
            target_path = Path(roles["target"]["organized_path"])
            before_path = Path(roles["pair"]["organized_path"])
            with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
                with WarpedVRT(
                    target_src,
                    crs=template.crs,
                    transform=transform,
                    width=width,
                    height=height,
                    resampling=Resampling.bilinear,
                    nodata=np.nan,
                ) as target_vrt, WarpedVRT(
                    before_src,
                    crs=template.crs,
                    transform=transform,
                    width=width,
                    height=height,
                    resampling=Resampling.bilinear,
                    nodata=np.nan,
                ) as before_vrt:
                    target = target_vrt.read(1).astype(np.float32)
                    before = before_vrt.read(1).astype(np.float32)
            valid = np.isfinite(target) & np.isfinite(before)
            diff = target - before
            sum_by_bin[bin_label][valid] += diff[valid]
            count_by_bin[bin_label][valid] += 1
            pair_rows.append(
                {
                    "rain_day_jst": rain_day,
                    "pair_no": pair_no,
                    "elapsed_h": delay,
                    "elapsed_bin": bin_label,
                    "valid_pixel_count": int(np.sum(valid)),
                    "mean_diff": float(np.mean(diff[valid])) if np.any(valid) else np.nan,
                }
            )

        profiles = []
        valid_all = np.ones((height, width), dtype=bool)
        count_rows = []
        for label in ELAPSED_LABELS:
            count = count_by_bin[label]
            mean = np.full((height, width), np.nan, dtype=np.float32)
            ok = count > 0
            mean[ok] = sum_by_bin[label][ok] / count[ok]
            profiles.append(mean)
            valid_all &= ok
            count_rows.append({"経過時間帯": label, "ペア数": int(sum(1 for r in pair_rows if r["elapsed_bin"] == label)), "有効画素数": int(np.sum(ok))})

        profile_stack = np.stack(profiles, axis=0)
        corr_inun = corr_with_signature(profile_stack, inun_mean)
        zrmse_inun = zrmse_to_reference(profile_stack, inun_mean, inun_std)
        zrmse_scene = zrmse_to_reference(profile_stack, scene_mean, scene_std)
        early = np.full((height, width), np.nan, dtype=np.float32)
        late = np.full((height, width), np.nan, dtype=np.float32)
        early[valid_all] = np.mean(profile_stack[:2, valid_all], axis=0)
        late[valid_all] = np.mean(profile_stack[2:, valid_all], axis=0)
        early_minus_late = early - late
        similar = (
            valid_all
            & (corr_inun >= args.corr_threshold)
            & (zrmse_inun <= args.zrmse_threshold)
            & (zrmse_inun < zrmse_scene)
            & (early_minus_late > 0)
        )

        valid_area = valid_all
        denom = int(np.sum(inun_union & valid_area))
        hit = int(np.sum(inun_union & valid_area & similar))
        similar_total = int(np.sum(similar))
        valid_total = int(np.sum(valid_area))
        summary = pd.DataFrame(
            [
                {
                    "正解浸水域定義": f"{args.mask_min}<=TIF値<={args.mask_max}",
                    "評価範囲_有効画素数": valid_total,
                    "正解浸水域画素数": denom,
                    "検出された正解浸水域画素数": hit,
                    "浸水域検出率_percent": float(hit / denom * 100.0) if denom else 0.0,
                    "検出画素総数": similar_total,
                    "検出画素面積率_percent": float(similar_total / valid_total * 100.0) if valid_total else 0.0,
                    "検出画素中の正解割合_percent": float(hit / similar_total * 100.0) if similar_total else 0.0,
                }
            ]
        )

        profile_summary = []
        profile_summary.extend(zone_profile_stats("検出画素", similar, profiles))
        profile_summary.extend(zone_profile_stats("非検出画素", valid_area & ~similar, profiles))
        profile_summary.extend(zone_profile_stats("正解浸水域", valid_area & inun_union, profiles))
        profile_summary.extend(zone_profile_stats("正解浸水域かつ検出", valid_area & inun_union & similar, profiles))
        profile_summary.extend(zone_profile_stats("正解浸水域だが未検出", valid_area & inun_union & ~similar, profiles))

        write_tif(args.output_dir / "map7_detection_mask.tif", template, transform, width, height, similar.astype(np.uint8), "uint8", 0)
        write_tif(args.output_dir / "map7_inundation_truth_mask.tif", template, transform, width, height, inun_union.astype(np.uint8), "uint8", 0)
        write_tif(args.output_dir / "map7_detection_hit_mask.tif", template, transform, width, height, (similar & inun_union).astype(np.uint8), "uint8", 0)
        write_tif(args.output_dir / "map7_zrmse_to_inundated.tif", template, transform, width, height, zrmse_inun, "float32", np.nan)
        for label, mean in zip(ELAPSED_LABELS, profiles):
            write_tif(args.output_dir / f"map7_mean_diff_{label.replace('-', '_')}.tif", template, transform, width, height, mean, "float32", np.nan)

    summary_csv = args.output_dir / "map7_detection_summary.csv"
    summary.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    criteria.to_csv(args.output_dir / "map7_detection_criteria.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(pair_rows).to_csv(args.output_dir / "map7_detection_pairs.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(count_rows).to_csv(args.output_dir / "map7_detection_elapsed_counts.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(mask_rows).to_csv(args.output_dir / "map7_truth_mask_pixel_counts.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(profile_summary).to_csv(args.output_dir / "map7_detection_profile_summary_by_zone.csv", index=False, encoding="utf-8-sig")
    (args.output_dir / "summary.json").write_text(
        json.dumps(
            {
                "summary_csv": str(summary_csv),
                "evaluation_extent": "union bounds of Kurume inundation TIFs plus buffer",
                "buffer_deg": args.buffer_deg,
                "corr_threshold": args.corr_threshold,
                "zrmse_threshold": args.zrmse_threshold,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(summary.to_string(index=False))
    print(f"saved: {summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
