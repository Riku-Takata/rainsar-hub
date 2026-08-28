#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Apply the revised map7 inundation rule with a paddy mask."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DETECTION_DIR = (
    ROOT_DIR
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
DEFAULT_PADDY_MASK = DEFAULT_DETECTION_DIR / "landmask_filter" / "map7_paddy_mask.tif"
DEFAULT_OUTPUT_DIR = DEFAULT_DETECTION_DIR / "revised_paddy_rule"


def read_raster(path: Path) -> tuple[np.ndarray, dict]:
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32), src.profile.copy()


def normalize_profile(values: np.ndarray) -> np.ndarray:
    std = float(np.nanstd(values))
    if not np.isfinite(std) or std <= 0:
        return np.full(values.shape, np.nan, dtype=np.float64)
    return (values - float(np.nanmean(values))) / std


def corr_with_signature(profile_stack: np.ndarray, signature: np.ndarray, valid: np.ndarray) -> np.ndarray:
    sig = normalize_profile(signature)
    out = np.full(profile_stack.shape[1:], np.nan, dtype=np.float32)
    values = profile_stack[:, valid]
    centered = values - np.mean(values, axis=0, keepdims=True)
    std = np.std(values, axis=0)
    corr = np.sum(centered * sig[:, None], axis=0) / (std * profile_stack.shape[0])
    corr[~np.isfinite(corr)] = np.nan
    out[valid] = corr.astype(np.float32)
    return out


def zrmse_to_reference(profile_stack: np.ndarray, mean: np.ndarray, std: np.ndarray, valid: np.ndarray) -> np.ndarray:
    std = np.where(std > 0, std, 1.0)
    out = np.full(profile_stack.shape[1:], np.nan, dtype=np.float32)
    z = (profile_stack[:, valid] - mean[:, None]) / std[:, None]
    out[valid] = np.sqrt(np.mean(z * z, axis=0)).astype(np.float32)
    return out


def metrics(pred: np.ndarray, truth: np.ndarray, valid: np.ndarray) -> dict[str, float | int]:
    tp = int(np.sum(pred & truth & valid))
    fp = int(np.sum(pred & ~truth & valid))
    fn = int(np.sum(~pred & truth & valid))
    tn = int(np.sum(~pred & ~truth & valid))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (tp + tn) / (tp + fp + fn + tn) if tp + fp + fn + tn else 0.0
    return {
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
        "pred_pixels": int(np.sum(pred & valid)),
        "truth_pixels": int(np.sum(truth & valid)),
        "valid_pixels": int(np.sum(valid)),
    }


def write_tif(path: Path, profile: dict, data: np.ndarray, dtype: str, nodata) -> None:
    out_profile = profile.copy()
    out_profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate", tiled=True)
    with rasterio.open(path, "w", **out_profile) as dst:
        dst.write(data.astype(dtype), 1)


def summarize_zone(zone_name: str, mask: np.ndarray, arrays: dict[str, np.ndarray]) -> list[dict[str, float | int | str]]:
    rows: list[dict[str, float | int | str]] = []
    for name, values in arrays.items():
        sample = values[mask & np.isfinite(values)]
        if sample.size == 0:
            rows.append({"領域": zone_name, "特徴量": name, "画素数": 0})
            continue
        q = np.percentile(sample, [5, 25, 50, 75, 95])
        rows.append(
            {
                "領域": zone_name,
                "特徴量": name,
                "画素数": int(sample.size),
                "平均": float(np.mean(sample)),
                "標準偏差": float(np.std(sample)),
                "p05": float(q[0]),
                "p25": float(q[1]),
                "中央値": float(q[2]),
                "p75": float(q[3]),
                "p95": float(q[4]),
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--detection-dir", type=Path, default=DEFAULT_DETECTION_DIR)
    parser.add_argument("--paddy-mask", type=Path, default=DEFAULT_PADDY_MASK)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--corr-threshold", type=float, default=0.3)
    parser.add_argument("--zrmse-threshold", type=float, default=0.87)
    parser.add_argument("--early-late-threshold", type=float, default=1.0)
    parser.add_argument("--mid-diff-max", type=float, default=0.0)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    d0, profile = read_raster(args.detection_dir / "map7_mean_diff_0_3h.tif")
    d3, _ = read_raster(args.detection_dir / "map7_mean_diff_3_6h.tif")
    d6, _ = read_raster(args.detection_dir / "map7_mean_diff_6_12h.tif")
    d12, _ = read_raster(args.detection_dir / "map7_mean_diff_12_24h.tif")
    truth, _ = read_raster(args.detection_dir / "map7_inundation_truth_mask.tif")
    paddy, _ = read_raster(args.paddy_mask)

    profile_stack = np.stack([d0, d3, d6, d12], axis=0)
    valid = np.all(np.isfinite(profile_stack), axis=0)
    truth_mask = truth > 0
    paddy_mask = paddy > 0

    criteria = pd.read_csv(args.detection_dir / "map7_detection_criteria.csv", encoding="utf-8-sig")
    ref_mean = criteria.iloc[:, 1].to_numpy(dtype=np.float64)
    ref_std = criteria.iloc[:, 2].to_numpy(dtype=np.float64)

    corr = corr_with_signature(profile_stack, ref_mean, valid)
    zrmse = zrmse_to_reference(profile_stack, ref_mean, ref_std, valid)
    early_mean = (d0 + d3) / 2.0
    late_mean = (d6 + d12) / 2.0
    early_minus_late = early_mean - late_mean

    pred = (
        valid
        & paddy_mask
        & (corr >= args.corr_threshold)
        & (zrmse <= args.zrmse_threshold)
        & (early_minus_late >= args.early_late_threshold)
        & (d6 <= args.mid_diff_max)
    )

    all_metrics = {"評価方法": "全正解浸水域を母数", **metrics(pred, truth_mask, valid)}
    paddy_metrics = {"評価方法": "田んぼ内のみを母数", **metrics(pred, truth_mask, valid & paddy_mask)}
    result = pd.DataFrame([all_metrics, paddy_metrics])
    result["precision_percent"] = result["precision"] * 100
    result["recall_percent"] = result["recall"] * 100
    result["f1_percent"] = result["f1"] * 100
    result["accuracy_percent"] = result["accuracy"] * 100
    result.to_csv(args.output_dir / "map7_revised_paddy_rule_metrics.csv", index=False, encoding="utf-8-sig")

    arrays = {
        "差分_0_3h": d0,
        "差分_3_6h": d3,
        "差分_6_12h": d6,
        "差分_12_24h": d12,
        "早期平均_0_6h": early_mean,
        "後期平均_6_24h": late_mean,
        "早期_minus_後期": early_minus_late,
        "久留米浸水域_corr": corr,
        "久留米浸水域_zRMSE": zrmse,
    }
    zones = {
        "TP_正解浸水域かつ検出": pred & truth_mask & valid,
        "FN_正解浸水域だが未検出": ~pred & truth_mask & valid,
        "FP_正解外だが誤検出": pred & ~truth_mask & valid,
        "TN_正解外かつ非検出": ~pred & ~truth_mask & valid,
        "田んぼ内_検出": pred & valid & paddy_mask,
        "田んぼ内_非検出": ~pred & valid & paddy_mask,
    }
    rows = []
    for zone_name, mask in zones.items():
        rows.extend(summarize_zone(zone_name, mask, arrays))
    pd.DataFrame(rows).to_csv(args.output_dir / "map7_revised_paddy_rule_feature_stats.csv", index=False, encoding="utf-8-sig")

    write_tif(args.output_dir / "map7_revised_paddy_rule_detection_mask.tif", profile, pred.astype(np.uint8), "uint8", 0)
    write_tif(args.output_dir / "map7_revised_paddy_rule_hit_mask.tif", profile, (pred & truth_mask).astype(np.uint8), "uint8", 0)
    write_tif(args.output_dir / "map7_revised_paddy_rule_fp_mask.tif", profile, (pred & ~truth_mask).astype(np.uint8), "uint8", 0)
    write_tif(args.output_dir / "map7_revised_corr.tif", profile, corr, "float32", np.nan)
    write_tif(args.output_dir / "map7_revised_zrmse.tif", profile, zrmse, "float32", np.nan)
    write_tif(args.output_dir / "map7_revised_early_minus_late.tif", profile, early_minus_late, "float32", np.nan)

    summary = {
        "rule": {
            "land_mask": "paddy only",
            "corr_threshold": args.corr_threshold,
            "zrmse_threshold": args.zrmse_threshold,
            "early_late_threshold": args.early_late_threshold,
            "mid_diff_max_6_12h": args.mid_diff_max,
        },
        "metrics_csv": str(args.output_dir / "map7_revised_paddy_rule_metrics.csv"),
        "feature_stats_csv": str(args.output_dir / "map7_revised_paddy_rule_feature_stats.csv"),
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(result[["評価方法", "TP", "FP", "FN", "precision_percent", "recall_percent", "f1_percent", "pred_pixels"]].to_string(index=False))
    print(f"saved: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
