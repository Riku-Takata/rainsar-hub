#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Analyze GSI DEM5A elevation values for confirmed Wajima flood labels."""

from __future__ import annotations

import argparse
import csv
import math
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree as ET

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.geometry import Point


DEFAULT_DEM_DIR = Path(r"D:\shuron\GT-data\20260708144307708-001")
DEFAULT_TRUTH_PATH = Path(r"D:\shuron\GT-data\sinsuiiki\shinsui.shp")
DEFAULT_OUTPUT_DIR = Path(r"D:\shuron\dem_elevation_analysis")


@dataclass(frozen=True)
class DemTile:
    zip_path: Path
    xml_name: str
    lower_lat: float
    lower_lon: float
    upper_lat: float
    upper_lon: float
    high_x: int
    high_y: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dem-dir", type=Path, default=DEFAULT_DEM_DIR)
    parser.add_argument("--truth-path", type=Path, default=DEFAULT_TRUTH_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--buffer-m", type=float, default=500.0)
    return parser.parse_args()


def read_text_from_zip(zip_path: Path, xml_name: str) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        return zf.read(xml_name).decode("utf-8", "replace")


def parse_tile_header(zip_path: Path, xml_name: str) -> DemTile | None:
    text = read_text_from_zip(zip_path, xml_name)
    lower = re.search(r"<gml:lowerCorner>([-0-9.]+)\s+([-0-9.]+)</gml:lowerCorner>", text)
    upper = re.search(r"<gml:upperCorner>([-0-9.]+)\s+([-0-9.]+)</gml:upperCorner>", text)
    high = re.search(r"<gml:high>(\d+)\s+(\d+)</gml:high>", text)
    if not lower or not upper or not high:
        return None
    return DemTile(
        zip_path=zip_path,
        xml_name=xml_name,
        lower_lat=float(lower.group(1)),
        lower_lon=float(lower.group(2)),
        upper_lat=float(upper.group(1)),
        upper_lon=float(upper.group(2)),
        high_x=int(high.group(1)),
        high_y=int(high.group(2)),
    )


def intersects(tile: DemTile, bounds: tuple[float, float, float, float]) -> bool:
    min_lon, min_lat, max_lon, max_lat = bounds
    return not (
        tile.upper_lon < min_lon
        or tile.lower_lon > max_lon
        or tile.upper_lat < min_lat
        or tile.lower_lat > max_lat
    )


def discover_tiles(dem_dir: Path, bounds: tuple[float, float, float, float]) -> list[DemTile]:
    tiles: list[DemTile] = []
    for zip_path in sorted(dem_dir.glob("FG-GML-*-DEM5A-*.zip")):
        with zipfile.ZipFile(zip_path) as zf:
            xml_names = [name for name in zf.namelist() if name.lower().endswith(".xml")]
        for xml_name in xml_names:
            tile = parse_tile_header(zip_path, xml_name)
            if tile and intersects(tile, bounds):
                tiles.append(tile)
    return tiles


def parse_elevations(tile: DemTile) -> pd.DataFrame:
    text = read_text_from_zip(tile.zip_path, tile.xml_name)
    root = ET.fromstring(text)
    ns = {"gml": "http://www.opengis.net/gml/3.2"}
    tuple_node = root.find(".//gml:tupleList", ns)
    if tuple_node is None or not tuple_node.text:
        return pd.DataFrame(columns=["lon", "lat", "elevation_m", "source_xml"])

    width = tile.high_x + 1
    height = tile.high_y + 1
    lon_step = (tile.upper_lon - tile.lower_lon) / width
    lat_step = (tile.upper_lat - tile.lower_lat) / height

    rows = []
    for idx, line in enumerate(tuple_node.text.strip().splitlines()):
        parts = line.strip().split(",")
        if len(parts) < 2:
            continue
        try:
            elev = float(parts[-1])
        except ValueError:
            continue
        if not math.isfinite(elev) or elev <= -9990:
            continue
        x = idx % width
        y = idx // width
        if y >= height:
            continue
        # tupleList is stored from north to south in GSI DEM GML.
        lon = tile.lower_lon + (x + 0.5) * lon_step
        lat = tile.upper_lat - (y + 0.5) * lat_step
        rows.append((lon, lat, elev, tile.xml_name))
    return pd.DataFrame(rows, columns=["lon", "lat", "elevation_m", "source_xml"])


def stats(values: pd.Series) -> dict[str, float | int]:
    values = values.dropna()
    if values.empty:
        return {
            "count": 0,
            "mean": np.nan,
            "median": np.nan,
            "std": np.nan,
            "min": np.nan,
            "p10": np.nan,
            "p25": np.nan,
            "p75": np.nan,
            "p90": np.nan,
            "max": np.nan,
        }
    return {
        "count": int(values.size),
        "mean": float(values.mean()),
        "median": float(values.median()),
        "std": float(values.std(ddof=1)),
        "min": float(values.min()),
        "p10": float(values.quantile(0.10)),
        "p25": float(values.quantile(0.25)),
        "p75": float(values.quantile(0.75)),
        "p90": float(values.quantile(0.90)),
        "max": float(values.max()),
    }


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    truth = gpd.read_file(args.truth_path)
    if truth.crs is None:
        raise RuntimeError(f"Truth data has no CRS: {args.truth_path}")
    truth = truth[truth.geometry.notna() & ~truth.geometry.is_empty].to_crs("EPSG:4326")
    truth_metric = truth.to_crs("EPSG:6675")
    aoi_metric = gpd.GeoSeries([truth_metric.union_all().buffer(args.buffer_m)], crs="EPSG:6675")
    aoi = gpd.GeoDataFrame(geometry=aoi_metric, crs="EPSG:6675").to_crs("EPSG:4326")
    bounds = tuple(float(x) for x in aoi.total_bounds)

    tiles = discover_tiles(args.dem_dir, bounds)
    if not tiles:
        raise RuntimeError("No DEM tiles overlap AOI.")

    samples = pd.concat([parse_elevations(tile) for tile in tiles], ignore_index=True)
    if samples.empty:
        raise RuntimeError("No valid DEM samples found.")

    points = gpd.GeoDataFrame(
        samples,
        geometry=gpd.points_from_xy(samples["lon"], samples["lat"]),
        crs="EPSG:4326",
    )
    points = points[points.within(aoi.union_all())].copy()
    points["label"] = np.where(points.within(truth.union_all()), "flooded", "not_flooded")

    points.drop(columns="geometry").to_csv(args.output_dir / "dem_samples_labeled.csv", index=False, encoding="utf-8-sig")

    stat_rows = []
    for label, group in points.groupby("label"):
        row = {"label": label, **stats(group["elevation_m"])}
        stat_rows.append(row)
    pd.DataFrame(stat_rows).to_csv(args.output_dir / "elevation_stats_by_label.csv", index=False, encoding="utf-8-sig")

    flooded = points.loc[points["label"] == "flooded", "elevation_m"]
    not_flooded = points.loc[points["label"] == "not_flooded", "elevation_m"]
    flooded_stats = stats(flooded)
    not_stats = stats(not_flooded)
    median_diff = float(flooded_stats["median"] - not_stats["median"])
    mean_diff = float(flooded_stats["mean"] - not_stats["mean"])

    plt.figure(figsize=(8, 5))
    bins = np.linspace(points["elevation_m"].min(), points["elevation_m"].max(), 50)
    plt.hist(not_flooded, bins=bins, alpha=0.55, label="not flooded", density=True)
    plt.hist(flooded, bins=bins, alpha=0.65, label="flooded", density=True)
    plt.xlabel("Elevation (m)")
    plt.ylabel("Density")
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.output_dir / "elevation_distribution.png", dpi=200)
    plt.close()

    tile_rows = [
        {
            "zip": str(tile.zip_path),
            "xml": tile.xml_name,
            "lower_lat": tile.lower_lat,
            "lower_lon": tile.lower_lon,
            "upper_lat": tile.upper_lat,
            "upper_lon": tile.upper_lon,
            "high_x": tile.high_x,
            "high_y": tile.high_y,
        }
        for tile in tiles
    ]
    with (args.output_dir / "overlapping_dem_tiles.csv").open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(tile_rows[0].keys()))
        writer.writeheader()
        writer.writerows(tile_rows)

    report = [
        "# 輪島 確認済み浸水域 標高分析",
        "",
        "## データ形式",
        "",
        "- 入力標高データは国土地理院 基盤地図情報の `FG-GML-*-DEM5A-*.zip`。",
        "- ZIP 内に 5m メッシュ標高の XML/GML が格納されている。",
        "- 座標は緯度経度系で、XML 内の `gml:Envelope` と `gml:tupleList` から標高値を読み取った。",
        "",
        "## 分析条件",
        "",
        f"- 正解ラベル: `{args.truth_path}`",
        f"- 非浸水比較領域: 浸水ポリゴンから {args.buffer_m:.0f} m バッファ内のうち、浸水ポリゴン外",
        f"- 使用DEMタイル数: {len(tiles)}",
        f"- 有効サンプル数: {len(points)}",
        "",
        "## 結果",
        "",
        f"- 浸水域の標高中央値: {flooded_stats['median']:.3f} m",
        f"- 非浸水域の標高中央値: {not_stats['median']:.3f} m",
        f"- 中央値差（浸水 - 非浸水）: {median_diff:.3f} m",
        f"- 浸水域の平均標高: {flooded_stats['mean']:.3f} m",
        f"- 非浸水域の平均標高: {not_stats['mean']:.3f} m",
        f"- 平均差（浸水 - 非浸水）: {mean_diff:.3f} m",
        "",
        "## 出力",
        "",
        "- `elevation_stats_by_label.csv`: 浸水/非浸水別の標高統計",
        "- `dem_samples_labeled.csv`: DEM点ごとの標高とラベル",
        "- `elevation_distribution.png`: 標高分布ヒストグラム",
        "- `overlapping_dem_tiles.csv`: 使用したDEMタイル一覧",
    ]
    (args.output_dir / "report.md").write_text("\n".join(report), encoding="utf-8-sig")
    print(f"DONE: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
