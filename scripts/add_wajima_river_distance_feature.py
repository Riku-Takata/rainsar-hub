#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Add nearest-river distance feature to Wajima DEM labeled samples."""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from shapely.ops import unary_union


DEFAULT_RIVER_PATH = Path(r"D:\shuron\GT-data\W05-07_17_GML\W05-07_17-g_Stream.shp")
DEFAULT_SAMPLES_CSV = Path(r"D:\shuron\dem_elevation_analysis\dem_samples_labeled.csv")
DEFAULT_TRUTH_PATH = Path(r"D:\shuron\GT-data\sinsuiiki\shinsui.shp")
DEFAULT_OUTPUT_DIR = Path(r"D:\shuron\river_distance_analysis")
METRIC_CRS = "EPSG:6675"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--river-path", type=Path, default=DEFAULT_RIVER_PATH)
    parser.add_argument("--samples-csv", type=Path, default=DEFAULT_SAMPLES_CSV)
    parser.add_argument("--truth-path", type=Path, default=DEFAULT_TRUTH_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--clip-buffer-m", type=float, default=1000.0)
    return parser.parse_args()


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

    samples = pd.read_csv(args.samples_csv)
    points = gpd.GeoDataFrame(
        samples,
        geometry=gpd.points_from_xy(samples["lon"], samples["lat"]),
        crs="EPSG:4326",
    ).to_crs(METRIC_CRS)

    truth = gpd.read_file(args.truth_path)
    if truth.crs is None:
        raise RuntimeError(f"Truth data has no CRS: {args.truth_path}")
    truth_metric = truth.to_crs(METRIC_CRS)
    clip_geom = truth_metric.union_all().buffer(args.clip_buffer_m)

    rivers = gpd.read_file(args.river_path)
    if rivers.crs is None:
        rivers = rivers.set_crs("EPSG:4326")
    rivers_metric = rivers.to_crs(METRIC_CRS)
    rivers_near = rivers_metric[rivers_metric.intersects(clip_geom)].copy()
    if rivers_near.empty:
        raise RuntimeError("No river lines overlap the analysis area.")

    river_union = unary_union(rivers_near.geometry)
    points["river_distance_m"] = points.geometry.distance(river_union)

    out = pd.DataFrame(points.drop(columns="geometry"))
    out.to_csv(args.output_dir / "dem_samples_with_river_distance.csv", index=False, encoding="utf-8-sig")

    stat_rows = []
    for label, group in out.groupby("label"):
        row = {"label": label, **stats(group["river_distance_m"])}
        stat_rows.append(row)
    stats_df = pd.DataFrame(stat_rows)
    stats_df.to_csv(args.output_dir / "river_distance_stats_by_label.csv", index=False, encoding="utf-8-sig")

    river_rows = rivers_near.to_crs("EPSG:4326").copy()
    river_rows.to_file(args.output_dir / "rivers_used.geojson", driver="GeoJSON")

    flooded = out.loc[out["label"] == "flooded", "river_distance_m"]
    not_flooded = out.loc[out["label"] == "not_flooded", "river_distance_m"]

    plt.figure(figsize=(8, 5))
    max_plot = min(float(out["river_distance_m"].quantile(0.99)), 1000.0)
    bins = np.linspace(0, max_plot, 50)
    plt.hist(not_flooded.clip(upper=max_plot), bins=bins, alpha=0.55, label="not flooded", density=True)
    plt.hist(flooded.clip(upper=max_plot), bins=bins, alpha=0.65, label="flooded", density=True)
    plt.xlabel("Distance to nearest river (m)")
    plt.ylabel("Density")
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.output_dir / "river_distance_distribution.png", dpi=200)
    plt.close()

    flooded_stats = stats(flooded)
    not_stats = stats(not_flooded)
    report = [
        "# 輪島 河川距離特徴量 分析",
        "",
        "## データ形式",
        "",
        "- 河川データは `W05-07_17-g_Stream.shp` の LineString データ。",
        "- `.prj` は無いが、座標値は経緯度のため EPSG:4326 として読み込み、距離計算時に EPSG:6675 へ変換した。",
        "- 各 DEM サンプル点から最寄り河川ラインまでの距離をメートル単位で計算した。",
        "",
        "## 結果",
        "",
        f"- 使用河川ライン数: {len(rivers_near)}",
        f"- 浸水域の河川距離中央値: {flooded_stats['median']:.2f} m",
        f"- 非浸水域の河川距離中央値: {not_stats['median']:.2f} m",
        f"- 中央値差（浸水 - 非浸水）: {float(flooded_stats['median'] - not_stats['median']):.2f} m",
        f"- 浸水域の平均河川距離: {flooded_stats['mean']:.2f} m",
        f"- 非浸水域の平均河川距離: {not_stats['mean']:.2f} m",
        "",
        "## 出力",
        "",
        "- `dem_samples_with_river_distance.csv`: DEM点ごとの標高・ラベル・河川距離",
        "- `river_distance_stats_by_label.csv`: 浸水/非浸水別の河川距離統計",
        "- `river_distance_distribution.png`: 河川距離分布",
        "- `rivers_used.geojson`: 計算に使った河川ライン",
    ]
    (args.output_dir / "report.md").write_text("\n".join(report), encoding="utf-8-sig")
    print(f"DONE: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
