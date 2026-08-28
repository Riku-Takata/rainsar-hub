#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Add JAXA land-cover class feature to Wajima labeled DEM samples."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio


DEFAULT_LC_DIR = Path(r"D:\shuron\GT-data\2024JPN_v25.04\2024JPN_v25.04")
DEFAULT_SAMPLES_CSV = Path(r"D:\shuron\river_distance_analysis\dem_samples_with_river_distance.csv")
DEFAULT_OUTPUT_DIR = Path(r"D:\shuron\landcover_analysis")

CLASS_NAMES = {
    1: "Water bodies",
    2: "Built-up",
    3: "Paddy field",
    4: "Cropland",
    5: "Grassland",
    6: "DBF",
    7: "DNF",
    8: "EBF",
    9: "ENF",
    10: "Bare",
    11: "Bamboo forest",
    12: "Solar panel",
    13: "Wetland",
    14: "Greenhouse",
    15: "Rock reef and Tidal flat",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landcover-dir", type=Path, default=DEFAULT_LC_DIR)
    parser.add_argument("--samples-csv", type=Path, default=DEFAULT_SAMPLES_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def tile_path_for_point(landcover_dir: Path, lat: float, lon: float) -> Path:
    lat_floor = math.floor(lat)
    lon_floor = math.floor(lon)
    return landcover_dir / f"LC_N{lat_floor:02d}E{lon_floor:03d}.tif"


def sample_landcover(samples: pd.DataFrame, landcover_dir: Path) -> pd.DataFrame:
    samples = samples.copy()
    samples["landcover_code"] = np.nan

    tile_groups: dict[Path, list[int]] = {}
    for idx, row in samples.iterrows():
        tile_path = tile_path_for_point(landcover_dir, float(row["lat"]), float(row["lon"]))
        tile_groups.setdefault(tile_path, []).append(idx)

    for tile_path, indices in tile_groups.items():
        if not tile_path.exists():
            continue
        coords = [(float(samples.at[idx, "lon"]), float(samples.at[idx, "lat"])) for idx in indices]
        with rasterio.open(tile_path) as ds:
            values = [int(v[0]) for v in ds.sample(coords)]
        samples.loc[indices, "landcover_code"] = values

    samples["landcover_code"] = samples["landcover_code"].astype("Int64")
    samples["landcover_name"] = samples["landcover_code"].map(CLASS_NAMES).fillna("Unknown")
    return samples


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    samples = pd.read_csv(args.samples_csv)
    out = sample_landcover(samples, args.landcover_dir)
    out.to_csv(args.output_dir / "samples_with_landcover.csv", index=False, encoding="utf-8-sig")

    counts = (
        out.groupby(["label", "landcover_code", "landcover_name"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    totals = out.groupby("label").size().rename("label_total").reset_index()
    counts = counts.merge(totals, on="label", how="left")
    counts["ratio"] = counts["count"] / counts["label_total"]
    counts.to_csv(args.output_dir / "landcover_counts_by_label.csv", index=False, encoding="utf-8-sig")

    pivot = counts.pivot_table(
        index=["landcover_code", "landcover_name"],
        columns="label",
        values="ratio",
        fill_value=0.0,
    ).reset_index()
    for col in ["flooded", "not_flooded"]:
        if col not in pivot.columns:
            pivot[col] = 0.0
    pivot["ratio_diff_flooded_minus_not"] = pivot["flooded"] - pivot["not_flooded"]
    pivot.sort_values("ratio_diff_flooded_minus_not", ascending=False).to_csv(
        args.output_dir / "landcover_ratio_comparison.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_df = counts.copy()
    plot_df["label_class"] = plot_df["label"] + " / " + plot_df["landcover_name"]
    fig, ax = plt.subplots(figsize=(10, 6))
    for label, group in counts.groupby("label"):
        group = group.sort_values("landcover_code")
        ax.plot(group["landcover_name"], group["ratio"], marker="o", label=label)
    ax.set_ylabel("Ratio")
    ax.set_xlabel("Land-cover class")
    ax.tick_params(axis="x", rotation=60)
    ax.legend()
    fig.tight_layout()
    fig.savefig(args.output_dir / "landcover_ratio_by_label.png", dpi=200)
    plt.close(fig)

    top_flooded = counts[counts["label"] == "flooded"].sort_values("ratio", ascending=False).head(5)
    top_not = counts[counts["label"] == "not_flooded"].sort_values("ratio", ascending=False).head(5)

    report = [
        "# 輪島 土地利用特徴量 分析",
        "",
        "## データ形式",
        "",
        "- 入力データは JAXA High-Resolution Land-Use and Land-Cover Map of Japan 2024 v25.04。",
        "- 1度タイルの GeoTIFF 形式で、輪島周辺では `LC_N37E136.tif` を使用した。",
        "- CRS は EPSG:4326、値は 1〜15 の土地利用カテゴリ。",
        "",
        "## 分析方法",
        "",
        "- 既存の DEM サンプル点に対して、該当ピクセルの土地利用コードを抽出した。",
        "- 浸水ラベル別に土地利用カテゴリの構成比を比較した。",
        "",
        "## 浸水域で多い土地利用 上位5件",
        "",
    ]
    for _, row in top_flooded.iterrows():
        report.append(f"- {int(row['landcover_code'])}: {row['landcover_name']} {row['ratio']:.3f} ({int(row['count'])}点)")
    report.extend(["", "## 非浸水域で多い土地利用 上位5件", ""])
    for _, row in top_not.iterrows():
        report.append(f"- {int(row['landcover_code'])}: {row['landcover_name']} {row['ratio']:.3f} ({int(row['count'])}点)")
    report.extend(
        [
            "",
            "## 出力",
            "",
            "- `samples_with_landcover.csv`: DEM・河川距離・土地利用を付与したサンプル",
            "- `landcover_counts_by_label.csv`: ラベル別土地利用カテゴリ集計",
            "- `landcover_ratio_comparison.csv`: 浸水/非浸水の構成比差",
            "- `landcover_ratio_by_label.png`: 構成比分布図",
        ]
    )
    (args.output_dir / "report.md").write_text("\n".join(report), encoding="utf-8-sig")
    print(f"DONE: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
