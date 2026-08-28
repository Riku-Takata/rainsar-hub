#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Evaluate map7 inundation candidates after filtering by paddy/road masks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DETECTION_DIR = (
    ROOT_DIR
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
DEFAULT_LAND_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_land_polygons"
DEFAULT_OUTPUT_DIR = DEFAULT_DETECTION_DIR / "landmask_filter"


def iter_geojson_geometries(path: Path) -> Iterable[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    for feature in data.get("features", []):
        geom = feature.get("geometry")
        if geom:
            yield geom


def rasterize_geojson(path: Path, *, out_shape, transform, all_touched: bool = True) -> np.ndarray:
    return rasterize(
        ((geom, 1) for geom in iter_geojson_geometries(path)),
        out_shape=out_shape,
        transform=transform,
        fill=0,
        dtype="uint8",
        all_touched=all_touched,
    ).astype(bool)


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
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "pred_pixels": int(np.sum(pred & valid)),
        "truth_pixels": int(np.sum(truth & valid)),
        "valid_pixels": int(np.sum(valid)),
    }


def write_tif(path: Path, template, data: np.ndarray) -> None:
    profile = template.profile.copy()
    profile.update(count=1, dtype="uint8", nodata=0, compress="deflate", tiled=True)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype("uint8"), 1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--detection-dir", type=Path, default=DEFAULT_DETECTION_DIR)
    parser.add_argument("--land-dir", type=Path, default=DEFAULT_LAND_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    detection_path = args.detection_dir / "map7_detection_mask.tif"
    truth_path = args.detection_dir / "map7_inundation_truth_mask.tif"
    paddy_geojson = args.land_dir / "map7_fude_paddy_polygons_from_db.geojson"
    road_geojson = args.land_dir / "map7_road_polygons.geojson"

    with rasterio.open(detection_path) as template:
        detected = template.read(1) > 0
        shape = (template.height, template.width)
        transform = template.transform
        paddy = rasterize_geojson(paddy_geojson, out_shape=shape, transform=transform)
        road = rasterize_geojson(road_geojson, out_shape=shape, transform=transform)
        write_tif(args.output_dir / "map7_paddy_mask.tif", template, paddy)
        write_tif(args.output_dir / "map7_road_mask.tif", template, road)
        write_tif(args.output_dir / "map7_paddy_or_road_mask.tif", template, paddy | road)
        write_tif(args.output_dir / "map7_detection_paddy_only.tif", template, detected & paddy)
        write_tif(args.output_dir / "map7_detection_road_only.tif", template, detected & road)
        write_tif(args.output_dir / "map7_detection_paddy_or_road.tif", template, detected & (paddy | road))
    with rasterio.open(truth_path) as src:
        truth = src.read(1) > 0

    base_valid = np.isfinite(detected)  # all pixels in this raster; kept for symmetry.
    masks = {
        "全域_地理条件なし": np.ones_like(detected, dtype=bool),
        "田んぼのみ": paddy,
        "道路のみ": road,
        "田んぼまたは道路": paddy | road,
    }

    rows = []
    for name, land_mask in masks.items():
        pred = detected & land_mask
        row = {"条件": name, "評価方法": "全正解浸水域を母数"}
        row.update(metrics(pred, truth, base_valid))
        rows.append(row)

        scoped_valid = base_valid & land_mask
        row = {"条件": name, "評価方法": "地理条件内のみを母数"}
        row.update(metrics(pred, truth, scoped_valid))
        rows.append(row)

        rows.append(
            {
                "条件": name,
                "評価方法": "マスク面積",
                "TP": np.nan,
                "FP": np.nan,
                "FN": np.nan,
                "TN": np.nan,
                "accuracy": np.nan,
                "precision": np.nan,
                "recall": np.nan,
                "f1": np.nan,
                "pred_pixels": int(np.sum(pred)),
                "truth_pixels": int(np.sum(truth & land_mask)),
                "valid_pixels": int(np.sum(land_mask)),
            }
        )

    result = pd.DataFrame(rows)
    result.to_csv(args.output_dir / "map7_detection_landmask_metrics.csv", index=False, encoding="utf-8-sig")

    # Compact summary for quick reporting.
    compact = result[result["評価方法"] == "全正解浸水域を母数"].copy()
    compact["precision_percent"] = compact["precision"] * 100
    compact["recall_percent"] = compact["recall"] * 100
    compact["f1_percent"] = compact["f1"] * 100
    compact["accuracy_percent"] = compact["accuracy"] * 100
    compact[
        [
            "条件",
            "TP",
            "FP",
            "FN",
            "precision_percent",
            "recall_percent",
            "f1_percent",
            "pred_pixels",
            "truth_pixels",
        ]
    ].to_csv(args.output_dir / "map7_detection_landmask_summary.csv", index=False, encoding="utf-8-sig")

    print(compact[["条件", "TP", "FP", "FN", "precision_percent", "recall_percent", "f1_percent", "pred_pixels"]].to_string(index=False))
    print(f"saved: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
