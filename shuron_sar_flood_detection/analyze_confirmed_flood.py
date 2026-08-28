from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask, rasterize, shapes
from rasterio.warp import Resampling, reproject
from rasterio.windows import from_bounds
from shapely.geometry import box, shape
from shapely.ops import unary_union


DEFAULT_DATA_DIR = Path(r"D:\shuron\GT-data")
DEFAULT_TRUTH_PATH = Path(r"D:\shuron\GT-data\sinsuiiki\shinsui.shp")
DEFAULT_OUTPUT_DIR = Path(r"D:\shuron\confirmed_flood_sar_analysis\output")
CALIBRATION_FACTOR_DB = -83.0


@dataclass(frozen=True)
class SarProduct:
    path: Path
    frame: str
    date: str
    role: str


@dataclass(frozen=True)
class SarPair:
    frame: str
    pre: SarProduct
    post: SarProduct


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze ALOS-2 SAR backscatter change using visually confirmed flood polygons."
    )
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--truth-path", type=Path, default=DEFAULT_TRUTH_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-area-m2", type=float, default=100.0)
    parser.add_argument("--min-threshold-db", type=float, default=-10.0)
    parser.add_argument("--max-threshold-db", type=float, default=-0.5)
    parser.add_argument("--fallback-buffer-m", type=float, default=500.0)
    return parser.parse_args()


def dn_to_db(dn: np.ndarray) -> np.ndarray:
    out = np.full(dn.shape, np.nan, dtype=np.float32)
    valid = dn > 0
    out[valid] = (20.0 * np.log10(dn[valid].astype(np.float32)) + CALIBRATION_FACTOR_DB).astype(
        np.float32
    )
    return out


def clean_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    gdf["geometry"] = gdf.geometry.buffer(0)
    return gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()


def discover_sar_pairs(data_dir: Path) -> list[SarPair]:
    products: list[SarProduct] = []
    for tif in sorted(data_dir.glob("L21_*/*/IMG-HH-*.tif")):
        match = re.search(r"ALOS2\d*(\d{4})-(\d{6})", tif.name)
        if not match:
            continue
        frame, date = match.group(1), match.group(2)
        if date in {"240811", "240812"}:
            role = "pre"
        elif date in {"240922", "240923"}:
            role = "post"
        else:
            continue
        products.append(SarProduct(path=tif, frame=frame, date=date, role=role))

    pairs: list[SarPair] = []
    for frame in sorted({p.frame for p in products}):
        pres = sorted((p for p in products if p.frame == frame and p.role == "pre"), key=lambda p: p.date)
        posts = sorted((p for p in products if p.frame == frame and p.role == "post"), key=lambda p: p.date)
        if pres and posts:
            pairs.append(SarPair(frame=frame, pre=pres[-1], post=posts[0]))
    return pairs


def choose_pair(truth: gpd.GeoDataFrame, pairs: list[SarPair]) -> SarPair:
    best_pair: SarPair | None = None
    best_overlap = 0.0
    for pair in pairs:
        with rasterio.open(pair.pre.path) as ds:
            projected = truth.to_crs(ds.crs)
            overlap = projected.union_all().intersection(box(*ds.bounds)).area
        if overlap > best_overlap:
            best_overlap = overlap
            best_pair = pair
    if best_pair is None or best_overlap <= 0:
        raise RuntimeError("Confirmed flood polygons do not overlap any SAR pre/post pair.")
    return best_pair


def load_aoi(data_dir: Path, truth: gpd.GeoDataFrame, raster_crs, fallback_buffer_m: float):
    inputareas = []
    truth_wgs = truth.to_crs("EPSG:4326")
    truth_union_wgs = truth_wgs.union_all()
    for path in sorted(data_dir.glob("wajima_*_shp/inputarea.shp")):
        gdf = clean_gdf(gpd.read_file(path)).to_crs("EPSG:4326")
        if gdf.empty:
            continue
        if gdf.union_all().intersects(truth_union_wgs):
            inputareas.append(gdf)

    if inputareas:
        aoi = gpd.GeoDataFrame(pd.concat(inputareas, ignore_index=True), geometry="geometry", crs="EPSG:4326")
        return aoi.to_crs(raster_crs).union_all(), "intersecting wajima inputarea.shp"

    truth_metric = truth.to_crs(raster_crs)
    return truth_metric.union_all().buffer(fallback_buffer_m), f"{fallback_buffer_m:.0f} m buffer around truth"


def make_window(ds: rasterio.DatasetReader, geom) -> rasterio.windows.Window:
    window = from_bounds(*geom.bounds, transform=ds.transform)
    window = window.round_offsets().round_lengths()
    col_off = max(0, int(window.col_off))
    row_off = max(0, int(window.row_off))
    width = min(ds.width - col_off, int(window.width))
    height = min(ds.height - row_off, int(window.height))
    if width <= 0 or height <= 0:
        raise ValueError("AOI is outside raster extent.")
    return rasterio.windows.Window(col_off, row_off, width, height)


def stats(values: np.ndarray, prefix: str) -> dict:
    values = values[np.isfinite(values)]
    if values.size == 0:
        return {
            f"{prefix}_n": 0,
            f"{prefix}_mean": np.nan,
            f"{prefix}_median": np.nan,
            f"{prefix}_p10": np.nan,
            f"{prefix}_p25": np.nan,
            f"{prefix}_p75": np.nan,
            f"{prefix}_p90": np.nan,
        }
    return {
        f"{prefix}_n": int(values.size),
        f"{prefix}_mean": float(np.nanmean(values)),
        f"{prefix}_median": float(np.nanmedian(values)),
        f"{prefix}_p10": float(np.nanpercentile(values, 10)),
        f"{prefix}_p25": float(np.nanpercentile(values, 25)),
        f"{prefix}_p75": float(np.nanpercentile(values, 75)),
        f"{prefix}_p90": float(np.nanpercentile(values, 90)),
    }


def optimize_threshold(
    delta_db: np.ndarray,
    valid_mask: np.ndarray,
    aoi_mask: np.ndarray,
    truth_mask: np.ndarray,
    min_threshold_db: float,
    max_threshold_db: float,
) -> tuple[float, dict]:
    eval_mask = valid_mask & aoi_mask
    labels = eval_mask & truth_mask
    candidates = np.linspace(min_threshold_db, max_threshold_db, 200)
    best_threshold = float(max_threshold_db)
    best = {"f1": -1.0, "precision": 0.0, "recall": 0.0, "tp": 0, "fp": 0, "fn": 0, "tn": 0}

    for threshold in candidates:
        pred = (delta_db <= threshold) & eval_mask
        tp = int(np.count_nonzero(pred & labels))
        fp = int(np.count_nonzero(pred & ~labels & eval_mask))
        fn = int(np.count_nonzero(~pred & labels))
        tn = int(np.count_nonzero(~pred & ~labels & eval_mask))
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        if f1 > best["f1"]:
            best_threshold = float(threshold)
            best = {
                "f1": float(f1),
                "precision": float(precision),
                "recall": float(recall),
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "tn": tn,
            }
    return best_threshold, best


def polygonize(mask: np.ndarray, transform, crs, min_area_m2: float, attrs: dict) -> gpd.GeoDataFrame:
    rows = []
    for geom, value in shapes(mask.astype("uint8"), mask=mask, transform=transform):
        if int(value) != 1:
            continue
        polygon = shape(geom)
        if polygon.area < min_area_m2:
            continue
        rows.append({**attrs, "area_m2": float(polygon.area), "geometry": polygon})
    if not rows:
        return gpd.GeoDataFrame(columns=[*attrs.keys(), "area_m2", "geometry"], geometry="geometry", crs=crs)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    truth = clean_gdf(gpd.read_file(args.truth_path))
    if truth.crs is None:
        raise RuntimeError(f"Truth data has no CRS: {args.truth_path}")
    pairs = discover_sar_pairs(args.data_dir)
    pair = choose_pair(truth, pairs)

    with rasterio.open(pair.pre.path) as pre_ds, rasterio.open(pair.post.path) as post_ds:
        truth_metric = truth.to_crs(pre_ds.crs)
        aoi_geom, aoi_source = load_aoi(args.data_dir, truth, pre_ds.crs, args.fallback_buffer_m)
        window = make_window(pre_ds, aoi_geom)
        transform = pre_ds.window_transform(window)

        pre_dn = pre_ds.read(1, window=window, masked=True).astype(np.float32)
        post_dn = np.zeros(pre_dn.shape, dtype=np.float32)
        reproject(
            source=rasterio.band(post_ds, 1),
            destination=post_dn,
            src_transform=post_ds.transform,
            src_crs=post_ds.crs,
            dst_transform=transform,
            dst_crs=pre_ds.crs,
            dst_nodata=0,
            resampling=Resampling.bilinear,
        )

        pre_db = dn_to_db(np.asarray(pre_dn.filled(0), dtype=np.float32))
        post_db = dn_to_db(post_dn)
        delta_db = post_db - pre_db

        truth_geom = truth_metric.union_all()
        aoi_mask = geometry_mask([aoi_geom], out_shape=delta_db.shape, transform=transform, invert=True)
        truth_mask = geometry_mask([truth_geom], out_shape=delta_db.shape, transform=transform, invert=True)
        valid_mask = np.isfinite(delta_db) & (np.asarray(pre_dn.filled(0)) > 0) & (post_dn > 0)
        background_mask = valid_mask & aoi_mask & ~truth_mask

        threshold, metrics = optimize_threshold(
            delta_db,
            valid_mask,
            aoi_mask,
            truth_mask,
            args.min_threshold_db,
            args.max_threshold_db,
        )
        candidate_mask_raw = (delta_db <= threshold) & valid_mask & aoi_mask

        attrs = {
            "frame": pair.frame,
            "pre_date": pair.pre.date,
            "post_date": pair.post.date,
            "threshold": threshold,
        }
        candidate_gdf = polygonize(candidate_mask_raw, transform, pre_ds.crs, args.min_area_m2, attrs)
        if not candidate_gdf.empty:
            candidate_mask = rasterize(
                candidate_gdf.geometry,
                out_shape=candidate_mask_raw.shape,
                transform=transform,
                fill=0,
                default_value=1,
                dtype="uint8",
            ).astype(bool)
        else:
            candidate_mask = np.zeros(candidate_mask_raw.shape, dtype=bool)

        profile = pre_ds.profile.copy()
        profile.update(
            driver="GTiff",
            height=delta_db.shape[0],
            width=delta_db.shape[1],
            transform=transform,
            count=1,
            compress="lzw",
        )

        delta_profile = profile.copy()
        delta_profile.update(dtype="float32", nodata=np.nan)
        with rasterio.open(args.output_dir / "confirmed_area_delta_db.tif", "w", **delta_profile) as dst:
            dst.write(delta_db.astype(np.float32), 1)

        mask_profile = profile.copy()
        mask_profile.update(dtype="uint8", nodata=0)
        with rasterio.open(args.output_dir / "confirmed_flood_label.tif", "w", **mask_profile) as dst:
            dst.write((truth_mask & valid_mask & aoi_mask).astype("uint8"), 1)
        with rasterio.open(args.output_dir / "sar_decrease_candidate.tif", "w", **mask_profile) as dst:
            dst.write(candidate_mask.astype("uint8"), 1)

        candidate_gdf.to_crs("EPSG:4326").to_file(
            args.output_dir / "sar_decrease_candidate.geojson", driver="GeoJSON"
        )
        truth.to_crs("EPSG:4326").to_file(args.output_dir / "confirmed_flood_truth.geojson", driver="GeoJSON")

        truth_values = delta_db[valid_mask & aoi_mask & truth_mask]
        background_values = delta_db[background_mask]
        summary = {
            "truth_path": str(args.truth_path),
            "aoi_source": aoi_source,
            "frame": pair.frame,
            "pre_scene": pair.pre.path.name,
            "post_scene": pair.post.path.name,
            "threshold_db": threshold,
            "confirmed_area_m2": float(truth_metric.area.sum()),
            "candidate_area_m2": float(np.count_nonzero(candidate_mask) * abs(pre_ds.transform.a * pre_ds.transform.e)),
            **metrics,
            **stats(truth_values, "confirmed_delta_db"),
            **stats(background_values, "background_delta_db"),
        }

        per_polygon_rows = []
        for idx, geom in enumerate(truth_metric.geometry):
            poly_mask = geometry_mask([geom], out_shape=delta_db.shape, transform=transform, invert=True)
            values = delta_db[valid_mask & aoi_mask & poly_mask]
            row = {"polygon_index": idx, "area_m2": float(geom.area), **stats(values, "delta_db")}
            per_polygon_rows.append(row)

    with (args.output_dir / "summary.csv").open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)

    with (args.output_dir / "per_polygon_stats.csv").open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(per_polygon_rows[0].keys()))
        writer.writeheader()
        writer.writerows(per_polygon_rows)

    report = [
        "# 確認済み浸水域 SAR 差分分析",
        "",
        "## 入力",
        "",
        f"- 正解ラベル: `{args.truth_path}`",
        f"- 評価範囲: {summary['aoi_source']}",
        f"- SAR ペア: `{summary['pre_scene']}` -> `{summary['post_scene']}`",
        "",
        "## 方法",
        "",
        "- DN を `20*log10(DN) - 83` で dB に変換した。",
        "- 被災後画像を被災前画像のグリッドへ再投影し、`post_db - pre_db` を計算した。",
        "- `shinsui.shp` は確認済み正例として扱い、周辺未確認領域は参考比較領域として集計した。",
        "- SAR 低下候補は、確認済み正例との F1 が最大になる `delta_db <= threshold` で抽出した。",
        "",
        "## 結果",
        "",
        f"- しきい値: {summary['threshold_db']:.3f} dB",
        f"- 確認済み浸水面積: {summary['confirmed_area_m2']:.1f} m2",
        f"- SAR 低下候補面積: {summary['candidate_area_m2']:.1f} m2",
        f"- precision: {summary['precision']:.3f}",
        f"- recall: {summary['recall']:.3f}",
        f"- F1: {summary['f1']:.3f}",
        f"- 確認済み浸水域 delta 中央値: {summary['confirmed_delta_db_median']:.3f} dB",
        f"- 周辺比較領域 delta 中央値: {summary['background_delta_db_median']:.3f} dB",
        "",
        "## 出力",
        "",
        "- `confirmed_area_delta_db.tif`: SAR 差分 dB",
        "- `confirmed_flood_label.tif`: 確認済み浸水域ラベル",
        "- `sar_decrease_candidate.tif`: SAR 低下候補マスク",
        "- `sar_decrease_candidate.geojson`: SAR 低下候補ポリゴン",
        "- `summary.csv`: 全体集計",
        "- `per_polygon_stats.csv`: ポリゴン別集計",
    ]
    (args.output_dir / "report.md").write_text("\n".join(report), encoding="utf-8-sig")
    print(f"DONE: {args.output_dir}")


if __name__ == "__main__":
    main()
