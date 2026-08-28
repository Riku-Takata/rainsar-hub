#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Calculate target/before/difference backscatter stats for map7 masks."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.vrt import WarpedVRT


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PAIR_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed_by_date"
DEFAULT_MASK_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_land_polygons"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "backscatter_stats"


BACKSCATTER_BINS = np.arange(-35.0, 16.0, 1.0)
DIFF_BINS = np.arange(-15.0, 15.5, 0.5)


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
        values64 = values.astype("float64", copy=False)
        self.count += int(values64.size)
        self.sum_value += float(values64.sum())
        self.sum_square += float(np.square(values64).sum())
        self.min_value = min(self.min_value, float(values64.min()))
        self.max_value = max(self.max_value, float(values64.max()))

    def as_dict(self) -> dict[str, float | int | None]:
        if self.count == 0:
            return {
                "count": 0,
                "mean": None,
                "std": None,
                "min": None,
                "max": None,
            }
        mean = self.sum_value / self.count
        variance = max(0.0, (self.sum_square / self.count) - mean * mean)
        return {
            "count": self.count,
            "mean": mean,
            "std": math.sqrt(variance),
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


def load_mask(path: Path, name: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.empty:
        raise ValueError(f"{name} mask is empty: {path}")
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    return gdf[["geometry"]].copy()


def subset_geometries(gdf: gpd.GeoDataFrame, bounds, crs) -> list[Any]:
    if gdf.crs != crs:
        gdf = gdf.to_crs(crs)
    minx, miny, maxx, maxy = bounds
    subset = gdf.cx[minx:maxx, miny:maxy]
    return [geom for geom in subset.geometry if geom is not None and not geom.is_empty]


def valid_values(values: np.ndarray) -> np.ndarray:
    values = values[np.isfinite(values)]
    return values


def calculate_pair_stats(
    target_path: Path,
    before_path: Path,
    masks: dict[str, gpd.GeoDataFrame],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    stat_rows: list[dict[str, Any]] = []
    hist_rows: list[dict[str, Any]] = []
    pair_meta: dict[str, Any] = {}

    with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
        pair_meta = {
            "target_width": target_src.width,
            "target_height": target_src.height,
            "target_crs": str(target_src.crs),
            "target_bounds": list(target_src.bounds),
            "before_width": before_src.width,
            "before_height": before_src.height,
            "before_crs": str(before_src.crs),
            "before_bounds": list(before_src.bounds),
        }

        with WarpedVRT(
            before_src,
            crs=target_src.crs,
            transform=target_src.transform,
            width=target_src.width,
            height=target_src.height,
            resampling=Resampling.bilinear,
            nodata=np.nan,
        ) as before_vrt:
            for mask_name, gdf in masks.items():
                geometries = subset_geometries(gdf, target_src.bounds, target_src.crs)
                if geometries:
                    mask = geometry_mask(
                        geometries,
                        out_shape=(target_src.height, target_src.width),
                        transform=target_src.transform,
                        invert=True,
                        all_touched=True,
                    )
                else:
                    mask = np.zeros((target_src.height, target_src.width), dtype=bool)

                accum = {
                    "target": RunningStats(),
                    "before": RunningStats(),
                    "diff": RunningStats(),
                }
                hists = {
                    "target": np.zeros(len(BACKSCATTER_BINS) - 1, dtype=np.int64),
                    "before": np.zeros(len(BACKSCATTER_BINS) - 1, dtype=np.int64),
                    "diff": np.zeros(len(DIFF_BINS) - 1, dtype=np.int64),
                }

                for _, window in target_src.block_windows(1):
                    block_mask = mask[
                        window.row_off : window.row_off + window.height,
                        window.col_off : window.col_off + window.width,
                    ]
                    if not block_mask.any():
                        continue

                    target_arr = target_src.read(1, window=window, masked=False).astype("float32", copy=False)
                    before_arr = before_vrt.read(1, window=window, masked=False).astype("float32", copy=False)

                    valid = block_mask & np.isfinite(target_arr) & np.isfinite(before_arr)
                    if target_src.nodata is not None:
                        valid &= target_arr != target_src.nodata
                    if before_src.nodata is not None:
                        valid &= before_arr != before_src.nodata
                    if not valid.any():
                        continue

                    target_values = valid_values(target_arr[valid])
                    before_values = valid_values(before_arr[valid])
                    diff_values = valid_values(target_values - before_values)

                    accum["target"].update(target_values)
                    accum["before"].update(before_values)
                    accum["diff"].update(diff_values)
                    hists["target"] += np.histogram(target_values, bins=BACKSCATTER_BINS)[0]
                    hists["before"] += np.histogram(before_values, bins=BACKSCATTER_BINS)[0]
                    hists["diff"] += np.histogram(diff_values, bins=DIFF_BINS)[0]

                for value_type, stats in accum.items():
                    bins = DIFF_BINS if value_type == "diff" else BACKSCATTER_BINS
                    hist = hists[value_type]
                    stats_dict = stats.as_dict()
                    for p in [5, 25, 50, 75, 95]:
                        stats_dict[f"p{p:02d}"] = percentile_from_hist(hist, bins, p)
                    stat_rows.append(
                        {
                            "mask": mask_name,
                            "value_type": value_type,
                            **stats_dict,
                        }
                    )

                    total = int(hist.sum())
                    for idx, count in enumerate(hist):
                        hist_rows.append(
                            {
                                "mask": mask_name,
                                "value_type": value_type,
                                "bin_left": float(bins[idx]),
                                "bin_right": float(bins[idx + 1]),
                                "count": int(count),
                                "frequency": (float(count) / total) if total else 0.0,
                            }
                        )

    return stat_rows, hist_rows, pair_meta


def read_manifest(path: Path) -> dict[tuple[str, str], dict[str, dict[str, str]]]:
    grouped: dict[tuple[str, str], dict[str, dict[str, str]]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            key = (row["rain_day_jst"], row["pair_no"])
            grouped.setdefault(key, {})[row["role"]] = row
    return grouped


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-dir", type=Path, default=DEFAULT_PAIR_DIR)
    parser.add_argument("--mask-dir", type=Path, default=DEFAULT_MASK_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    masks = {
        "road": load_mask(args.mask_dir / "map7_road_polygons.geojson", "road"),
        "paddy": load_mask(args.mask_dir / "map7_fude_paddy_polygons_from_db.geojson", "paddy"),
    }

    manifest = read_manifest(args.pair_dir / "manifest.csv")
    stat_rows: list[dict[str, Any]] = []
    hist_rows: list[dict[str, Any]] = []
    meta_rows: list[dict[str, Any]] = []

    for index, ((rain_day, pair_no), roles) in enumerate(sorted(manifest.items()), start=1):
        if "target" not in roles or "pair" not in roles:
            continue
        target_path = Path(roles["target"]["organized_path"])
        before_path = Path(roles["pair"]["organized_path"])
        print(f"[{index}/{len(manifest)}] {rain_day} {pair_no}")

        pair_stats, pair_hists, pair_meta = calculate_pair_stats(target_path, before_path, masks)
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
        meta_rows.append({**base, **pair_meta})

    stats_path = args.output_dir / "map7_backscatter_stats_by_mask.csv"
    hist_path = args.output_dir / "map7_backscatter_histograms_by_mask.csv"
    meta_path = args.output_dir / "map7_backscatter_pair_metadata.csv"
    summary_path = args.output_dir / "summary.json"

    write_csv(
        stats_path,
        stat_rows,
        [
            "rain_day_jst",
            "pair_no",
            "mask",
            "value_type",
            "count",
            "mean",
            "std",
            "min",
            "p05",
            "p25",
            "p50",
            "p75",
            "p95",
            "max",
            "target_stac_id",
            "before_stac_id",
            "rain_first_ts_jst",
            "rain_last_ts_jst",
            "before_day_jst",
        ],
    )
    write_csv(
        hist_path,
        hist_rows,
        [
            "rain_day_jst",
            "pair_no",
            "mask",
            "value_type",
            "bin_left",
            "bin_right",
            "count",
            "frequency",
            "target_stac_id",
            "before_stac_id",
            "rain_first_ts_jst",
            "rain_last_ts_jst",
            "before_day_jst",
        ],
    )
    write_csv(meta_path, meta_rows, list(meta_rows[0].keys()) if meta_rows else [])

    summary = {
        "pair_count": len(meta_rows),
        "stats_rows": len(stat_rows),
        "histogram_rows": len(hist_rows),
        "masks": list(masks.keys()),
        "stats_csv": str(stats_path),
        "histogram_csv": str(hist_path),
        "metadata_csv": str(meta_path),
        "difference_definition": "diff = target - before, before resampled to target grid",
        "unit": "dB if the preprocessed SNAP output is dB; values are used as stored in the TIFs",
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
