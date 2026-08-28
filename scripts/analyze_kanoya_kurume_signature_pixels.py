#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Classify Kanoya pixels using target-before backscatter differences."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.vrt import WarpedVRT


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_KANOYA_DIR = Path(r"D:\sotsuron\kanoya")
DEFAULT_PROCESSED_DIR = ROOT_DIR / "output" / "kanoya_rain_s1" / "processed"
DEFAULT_PAIR_CSV = ROOT_DIR / "output" / "kanoya_rain_s1" / "kanoya_s1_rain_no_rain_pairs_download.csv"
DEFAULT_KURUME_STATS = (
    ROOT_DIR
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "feature_point_sampling"
    / "feature_sample_statistics.csv"
)
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "kanoya_rain_s1" / "kurume_signature_diff_analysis"

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


def normalize_profile(values: np.ndarray) -> np.ndarray:
    values = values.astype(np.float64, copy=True)
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
    numerator = np.sum(centered * sig[:, None], axis=0)
    denominator = std * profile_stack.shape[0]
    corr = numerator / denominator
    corr[~np.isfinite(corr)] = np.nan
    out[valid] = corr.astype(np.float32)
    return out


def read_processed_paths(path: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            output_path = Path(row["output_path"]) if row.get("output_path") else None
            if output_path and output_path.exists() and output_path.stat().st_size > 0:
                paths[row["stac_id"]] = output_path
    return paths


def read_pair_observations(pair_csv: Path, processed_paths: dict[str, Path]) -> pd.DataFrame:
    df = pd.read_csv(pair_csv, encoding="utf-8-sig")
    df = df[
        df["target_downloaded"].astype(str).str.lower().eq("true")
        & df["pair_downloaded"].astype(str).str.lower().eq("true")
    ].copy()
    df["elapsed_h"] = pd.to_numeric(df["delay_from_rain_start_h"], errors="coerce")
    df["elapsed_bin"] = df["elapsed_h"].apply(elapsed_bin)
    df["target_tif"] = df["target_stac_id"].map(processed_paths)
    df["before_tif"] = df["pair_stac_id"].map(processed_paths)
    df = df[
        df["elapsed_bin"].isin(ELAPSED_LABELS)
        & df["target_tif"].notna()
        & df["before_tif"].notna()
    ].copy()
    return df


def read_kurume_signature(stats_csv: Path) -> tuple[pd.DataFrame, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    stats = pd.read_csv(stats_csv, encoding="utf-8-sig")
    inundated_df = stats[stats["area"] == "浸水域"].set_index("elapsed_bin").reindex(ELAPSED_LABELS)
    scene_df = stats[stats["area"] == "シーン全体"].set_index("elapsed_bin").reindex(ELAPSED_LABELS)
    inundated_mean = inundated_df["mean"].to_numpy(dtype=np.float64)
    inundated_std = inundated_df["std"].to_numpy(dtype=np.float64)
    scene_mean = scene_df["mean"].to_numpy(dtype=np.float64)
    scene_std = scene_df["std"].to_numpy(dtype=np.float64)
    if (
        np.any(~np.isfinite(inundated_mean))
        or np.any(~np.isfinite(inundated_std))
        or np.any(~np.isfinite(scene_mean))
        or np.any(~np.isfinite(scene_std))
    ):
        raise ValueError("Kurume signature is incomplete.")
    criteria = pd.DataFrame(
        {
            "経過時間帯": ELAPSED_LABELS,
            "久留米浸水域_平均差分": inundated_mean,
            "久留米浸水域_標準偏差": inundated_std,
            "久留米浸水域_p25": inundated_df["p25"].to_numpy(dtype=np.float64),
            "久留米浸水域_p50": inundated_df["p50"].to_numpy(dtype=np.float64),
            "久留米浸水域_p75": inundated_df["p75"].to_numpy(dtype=np.float64),
            "久留米シーン全体_平均差分": scene_mean,
            "久留米シーン全体_標準偏差": scene_std,
        }
    )
    return criteria, inundated_mean, inundated_std, scene_mean, scene_std


def zrmse_to_reference(profile_stack: np.ndarray, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    std = np.where(std > 0, std, 1.0)
    out = np.full(profile_stack.shape[1:], np.nan, dtype=np.float32)
    valid = np.all(np.isfinite(profile_stack), axis=0)
    if not np.any(valid):
        return out
    z = (profile_stack[:, valid] - mean[:, None]) / std[:, None]
    out[valid] = np.sqrt(np.mean(z**2, axis=0)).astype(np.float32)
    return out


def zone_profile_stats(zone_name: str, mask: np.ndarray, profiles: list[np.ndarray]) -> list[dict[str, float | int | str]]:
    rows: list[dict[str, float | int | str]] = []
    for label, values in zip(ELAPSED_LABELS, profiles):
        sample = values[mask & np.isfinite(values)]
        if sample.size == 0:
            rows.append(
                {
                    "領域": zone_name,
                    "経過時間帯": label,
                    "画素数": 0,
                    "平均差分": np.nan,
                    "中央値差分": np.nan,
                    "p25": np.nan,
                    "p75": np.nan,
                }
            )
            continue
        q25, q50, q75 = np.percentile(sample, [25, 50, 75])
        rows.append(
            {
                "領域": zone_name,
                "経過時間帯": label,
                "画素数": int(sample.size),
                "平均差分": float(np.mean(sample)),
                "中央値差分": float(q50),
                "p25": float(q25),
                "p75": float(q75),
            }
        )
    return rows


def read_geojson_mask(geojson_path: Path, template) -> np.ndarray:
    data = json.loads(geojson_path.read_text(encoding="utf-8"))
    geoms = [feature["geometry"] for feature in data.get("features", []) if feature.get("geometry")]
    if not geoms:
        return np.ones((template.height, template.width), dtype=bool)
    return geometry_mask(geoms, out_shape=(template.height, template.width), transform=template.transform, invert=True)


def reproject_mask(mask_path: Path, template, min_value: float | None, max_value: float | None) -> np.ndarray:
    with rasterio.open(mask_path) as src:
        with WarpedVRT(
            src,
            crs=template.crs,
            transform=template.transform,
            width=template.width,
            height=template.height,
            resampling=Resampling.nearest,
            nodata=src.nodata,
        ) as vrt:
            values = vrt.read(1).astype(np.float32)
    valid = np.isfinite(values)
    if src.nodata is not None:
        valid &= values != src.nodata
    if min_value is not None:
        valid &= values >= min_value
    if max_value is not None:
        valid &= values <= max_value
    return valid


def write_tif(path: Path, template, data: np.ndarray, dtype: str, nodata) -> None:
    profile = template.profile.copy()
    profile.update(driver="GTiff", count=1, dtype=dtype, nodata=nodata, compress="deflate", tiled=True)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    parser.add_argument("--pair-csv", type=Path, default=DEFAULT_PAIR_CSV)
    parser.add_argument("--kanoya-dir", type=Path, default=DEFAULT_KANOYA_DIR)
    parser.add_argument("--kurume-stats", type=Path, default=DEFAULT_KURUME_STATS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--corr-threshold", type=float, default=0.5)
    parser.add_argument("--zrmse-threshold", type=float, default=1.0)
    parser.add_argument("--mask-min", type=float, default=0.5)
    parser.add_argument("--mask-max", type=float, default=1.7)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    processed_paths = read_processed_paths(args.processed_dir / "preprocess_results_VV.csv")
    observations = read_pair_observations(args.pair_csv, processed_paths)
    if observations.empty:
        raise RuntimeError("No complete target-before observations were found.")

    criteria, kurume_inundated, kurume_inundated_std, kurume_scene, kurume_scene_std = read_kurume_signature(
        args.kurume_stats
    )
    template_path = Path(observations.iloc[0]["target_tif"])

    with rasterio.open(template_path) as template:
        geojson_mask = read_geojson_mask(args.kanoya_dir / "kanoya.geojson", template)
        sum_by_bin = {label: np.zeros((template.height, template.width), dtype=np.float32) for label in ELAPSED_LABELS}
        count_by_bin = {label: np.zeros((template.height, template.width), dtype=np.uint16) for label in ELAPSED_LABELS}

        pair_rows = []
        for _, row in observations.iterrows():
            bin_label = row["elapsed_bin"]
            target_path = Path(row["target_tif"])
            before_path = Path(row["before_tif"])
            with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
                with WarpedVRT(
                    target_src,
                    crs=template.crs,
                    transform=template.transform,
                    width=template.width,
                    height=template.height,
                    resampling=Resampling.bilinear,
                    nodata=np.nan,
                ) as target_vrt, WarpedVRT(
                    before_src,
                    crs=template.crs,
                    transform=template.transform,
                    width=template.width,
                    height=template.height,
                    resampling=Resampling.bilinear,
                    nodata=np.nan,
                ) as before_vrt:
                    target = target_vrt.read(1).astype(np.float32)
                    before = before_vrt.read(1).astype(np.float32)
            valid = np.isfinite(target) & np.isfinite(before) & geojson_mask
            diff = target - before
            sum_by_bin[bin_label][valid] += diff[valid]
            count_by_bin[bin_label][valid] += 1
            pair_rows.append(
                {
                    "rain_day_jst": row["rain_day_jst"],
                    "elapsed_h": row["elapsed_h"],
                    "elapsed_bin": bin_label,
                    "target_stac_id": row["target_stac_id"],
                    "pair_stac_id": row["pair_stac_id"],
                    "valid_pixel_count": int(np.sum(valid)),
                    "mean_diff": float(np.nanmean(diff[valid])) if np.any(valid) else np.nan,
                }
            )

        profiles = []
        valid_all = geojson_mask.copy()
        count_rows = []
        for label in ELAPSED_LABELS:
            count = count_by_bin[label]
            mean = np.full((template.height, template.width), np.nan, dtype=np.float32)
            ok = count > 0
            mean[ok] = sum_by_bin[label][ok] / count[ok]
            profiles.append(mean)
            valid_all &= ok
            count_rows.append(
                {
                    "経過時間帯": label,
                    "ペア数": int((observations["elapsed_bin"] == label).sum()),
                    "有効画素数": int(np.sum(ok & geojson_mask)),
                }
            )

        profile_stack = np.stack(profiles, axis=0)
        corr_inundated = corr_with_signature(profile_stack, kurume_inundated)
        inundated_zrmse = zrmse_to_reference(profile_stack, kurume_inundated, kurume_inundated_std)
        scene_zrmse = zrmse_to_reference(profile_stack, kurume_scene, kurume_scene_std)
        signature_score = scene_zrmse - inundated_zrmse
        early = np.full((template.height, template.width), np.nan, dtype=np.float32)
        late = np.full((template.height, template.width), np.nan, dtype=np.float32)
        early[valid_all] = np.mean(profile_stack[:2, valid_all], axis=0)
        late[valid_all] = np.mean(profile_stack[2:, valid_all], axis=0)
        early_minus_late = early - late

        similar = (
            valid_all
            & (corr_inundated >= args.corr_threshold)
            & (inundated_zrmse <= args.zrmse_threshold)
            & (inundated_zrmse < scene_zrmse)
            & (early_minus_late > 0)
        )
        valid_area = valid_all & geojson_mask

        inundation_valid = reproject_mask(args.kanoya_dir / "Inun_shinkawacho.tif", template, 0.0, None)
        inundation_range = reproject_mask(args.kanoya_dir / "Inun_shinkawacho.tif", template, args.mask_min, args.mask_max)

        def overlap_row(name: str, mask: np.ndarray) -> dict[str, float | int | str]:
            denom = int(np.sum(mask & valid_area))
            hit = int(np.sum(mask & valid_area & similar))
            similar_total = int(np.sum(similar))
            valid_total = int(np.sum(valid_area))
            return {
                "浸水域定義": name,
                "評価対象浸水画素数": denom,
                "類似判定に含まれた浸水画素数": hit,
                "浸水域包含率_percent": float(hit / denom * 100.0) if denom else 0.0,
                "類似判定総画素数": similar_total,
                "評価対象シーン画素数": valid_total,
                "類似判定面積率_percent": float(similar_total / valid_total * 100.0) if valid_total else 0.0,
            }

        summary = pd.DataFrame(
            [
                overlap_row("TIF有効値>0", inundation_valid),
                overlap_row(f"{args.mask_min}<=TIF値<={args.mask_max}", inundation_range),
            ]
        )
        profile_summary_rows = []
        profile_summary_rows.extend(zone_profile_stats("類似判定画素", similar, profiles))
        profile_summary_rows.extend(zone_profile_stats("非類似判定画素", valid_area & ~similar, profiles))
        profile_summary_rows.extend(zone_profile_stats("正解浸水域_0.5-1.7", valid_area & inundation_range, profiles))
        profile_summary_rows.extend(zone_profile_stats("正解浸水域かつ類似判定", valid_area & inundation_range & similar, profiles))
        profile_summary_rows.extend(zone_profile_stats("正解浸水域だが非類似", valid_area & inundation_range & ~similar, profiles))

        write_tif(args.output_dir / "kanoya_diff_kurume_signature_similar_mask.tif", template, similar.astype(np.uint8), "uint8", 0)
        write_tif(
            args.output_dir / "kanoya_diff_kurume_signature_not_similar_mask.tif",
            template,
            (valid_area & ~similar).astype(np.uint8),
            "uint8",
            0,
        )
        write_tif(args.output_dir / "kanoya_diff_signature_valid_area_mask.tif", template, valid_area.astype(np.uint8), "uint8", 0)
        write_tif(args.output_dir / "kanoya_diff_kurume_signature_score.tif", template, signature_score, "float32", np.nan)
        write_tif(args.output_dir / "kanoya_diff_corr_to_kurume_inundated.tif", template, corr_inundated, "float32", np.nan)
        write_tif(args.output_dir / "kanoya_diff_zrmse_to_kurume_inundated.tif", template, inundated_zrmse, "float32", np.nan)
        write_tif(args.output_dir / "kanoya_diff_zrmse_to_kurume_scene.tif", template, scene_zrmse, "float32", np.nan)
        write_tif(args.output_dir / "kanoya_diff_early_minus_late.tif", template, early_minus_late, "float32", np.nan)
        write_tif(args.output_dir / "kanoya_inundation_mask_0p5_1p7_on_diff_scene.tif", template, inundation_range.astype(np.uint8), "uint8", 0)
        for label, mean in zip(ELAPSED_LABELS, profiles):
            safe_label = label.replace("-", "_").replace("h", "h")
            write_tif(args.output_dir / f"kanoya_mean_diff_{safe_label}.tif", template, mean, "float32", np.nan)

    summary_csv = args.output_dir / "kanoya_diff_kurume_signature_overlap_summary.csv"
    pair_csv = args.output_dir / "kanoya_diff_signature_pairs.csv"
    count_csv = args.output_dir / "kanoya_diff_elapsed_bin_counts.csv"
    criteria_csv = args.output_dir / "kanoya_diff_matching_criteria.csv"
    profile_summary_csv = args.output_dir / "kanoya_diff_profile_summary_by_zone.csv"
    summary.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    pd.DataFrame(pair_rows).to_csv(pair_csv, index=False, encoding="utf-8-sig")
    pd.DataFrame(count_rows).to_csv(count_csv, index=False, encoding="utf-8-sig")
    criteria.to_csv(criteria_csv, index=False, encoding="utf-8-sig")
    pd.DataFrame(profile_summary_rows).to_csv(profile_summary_csv, index=False, encoding="utf-8-sig")

    metadata = {
        "method": "Pixel-wise target-before mean difference by elapsed bin; similar pixels satisfy corr_to_kurume_inundated >= threshold, zRMSE to Kurume inundated profile <= threshold, closer to inundated than scene profile, and early mean > late mean.",
        "elapsed_labels": ELAPSED_LABELS,
        "kurume_inundated_signature": dict(zip(ELAPSED_LABELS, kurume_inundated.tolist())),
        "kurume_scene_signature": dict(zip(ELAPSED_LABELS, kurume_scene.tolist())),
        "corr_threshold": args.corr_threshold,
        "zrmse_threshold": args.zrmse_threshold,
        "summary_csv": str(summary_csv),
        "pair_csv": str(pair_csv),
        "count_csv": str(count_csv),
        "criteria_csv": str(criteria_csv),
        "profile_summary_csv": str(profile_summary_csv),
    }
    (args.output_dir / "summary.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    print(summary.to_string(index=False))
    print(f"saved: {summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
