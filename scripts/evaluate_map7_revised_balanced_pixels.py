#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Balanced-pixel evaluation for the revised map7 paddy-rule result."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_RULE_DIR = (
    ROOT_DIR
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
    / "revised_paddy_rule"
)
DEFAULT_DETECTION_DIR = DEFAULT_RULE_DIR.parent
DEFAULT_LAND_DIR = DEFAULT_DETECTION_DIR / "landmask_filter"


def read_mask(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1) > 0


def calc_metrics(pred: np.ndarray, truth: np.ndarray, indices: np.ndarray) -> dict[str, float | int]:
    p = pred.ravel()[indices]
    t = truth.ravel()[indices]
    tp = int(np.sum(p & t))
    fp = int(np.sum(p & ~t))
    fn = int(np.sum(~p & t))
    tn = int(np.sum(~p & ~t))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (tp + tn) / len(indices) if len(indices) else 0.0
    return {
        "sample_pixels": int(len(indices)),
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": accuracy,
    }


def summarize_runs(rows: list[dict[str, float | int]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    metrics = ["TP", "FP", "FN", "TN", "precision", "recall", "f1", "accuracy"]
    out = []
    for metric in metrics:
        values = df[metric].to_numpy(dtype=float)
        out.append(
            {
                "指標": metric,
                "平均": float(np.mean(values)),
                "標準偏差": float(np.std(values)),
                "最小": float(np.min(values)),
                "最大": float(np.max(values)),
            }
        )
    return pd.DataFrame(out)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rule-dir", type=Path, default=DEFAULT_RULE_DIR)
    parser.add_argument("--detection-dir", type=Path, default=DEFAULT_DETECTION_DIR)
    parser.add_argument("--land-dir", type=Path, default=DEFAULT_LAND_DIR)
    parser.add_argument("--runs", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20260518)
    args = parser.parse_args()

    pred = read_mask(args.rule_dir / "map7_revised_paddy_rule_detection_mask.tif")
    truth = read_mask(args.detection_dir / "map7_inundation_truth_mask.tif")
    paddy = read_mask(args.land_dir / "map7_paddy_mask.tif")
    valid = paddy

    positive_indices = np.flatnonzero((truth & valid).ravel())
    negative_indices = np.flatnonzero((~truth & valid).ravel())
    n = min(len(positive_indices), len(negative_indices))
    rng = np.random.default_rng(args.seed)

    run_rows = []
    for run in range(args.runs):
        sampled_negative = rng.choice(negative_indices, size=n, replace=False)
        sampled_positive = rng.choice(positive_indices, size=n, replace=False)
        indices = np.concatenate([sampled_positive, sampled_negative])
        rng.shuffle(indices)
        row = {"run": run + 1, "positive_pixels": n, "negative_pixels": n}
        row.update(calc_metrics(pred, truth, indices))
        run_rows.append(row)

    runs_df = pd.DataFrame(run_rows)
    summary_df = summarize_runs(run_rows)
    summary_df["平均_percent"] = np.where(
        summary_df["指標"].isin(["precision", "recall", "f1", "accuracy"]),
        summary_df["平均"] * 100,
        np.nan,
    )

    out_dir = args.rule_dir / "balanced_evaluation"
    out_dir.mkdir(parents=True, exist_ok=True)
    runs_df.to_csv(out_dir / "map7_revised_balanced_runs.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(out_dir / "map7_revised_balanced_summary.csv", index=False, encoding="utf-8-sig")

    # One deterministic balanced sample for feature extraction and inspection.
    sampled_negative = rng.choice(negative_indices, size=n, replace=False)
    indices = np.concatenate([positive_indices[:n], sampled_negative])
    balanced_mask = np.zeros(pred.size, dtype=np.uint8)
    balanced_mask[indices] = 1
    with rasterio.open(args.rule_dir / "map7_revised_paddy_rule_detection_mask.tif") as src:
        profile = src.profile.copy()
        profile.update(count=1, dtype="uint8", nodata=0, compress="deflate", tiled=True)
        with rasterio.open(out_dir / "map7_revised_balanced_sample_mask.tif", "w", **profile) as dst:
            dst.write(balanced_mask.reshape(pred.shape), 1)

    print(summary_df.to_string(index=False))
    print(f"positive={len(positive_indices)} negative={len(negative_indices)} sampled_each={n}")
    print(f"saved: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
