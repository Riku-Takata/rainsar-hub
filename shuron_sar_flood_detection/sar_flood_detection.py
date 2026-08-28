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
DEFAULT_OUTPUT_DIR = Path(r"D:\shuron\flood_detection_sar\output")
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
        description="Detect candidate inundation areas from pre/post ALOS-2 SAR backscatter decrease."
    )
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-area-m2", type=float, default=100.0)
    parser.add_argument("--max-threshold-db", type=float, default=-0.5)
    parser.add_argument("--min-threshold-db", type=float, default=-10.0)
    return parser.parse_args()


def dn_to_db(dn: np.ndarray) -> np.ndarray:
    out = np.full(dn.shape, np.nan, dtype=np.float32)
    valid = dn > 0
    out[valid] = (20.0 * np.log10(dn[valid].astype(np.float32)) + CALIBRATION_FACTOR_DB).astype(
        np.float32
    )
    return out


def valid_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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
    frames = sorted({p.frame for p in products})
    for frame in frames:
        pres = sorted((p for p in products if p.frame == frame and p.role == "pre"), key=lambda p: p.date)
        posts = sorted((p for p in products if p.frame == frame and p.role == "post"), key=lambda p: p.date)
        if pres and posts:
            pairs.append(SarPair(frame=frame, pre=pres[-1], post=posts[0]))
    return pairs


def load_manual_areas(data_dir: Path) -> list[dict]:
    areas: list[dict] = []
    for shp_dir in sorted(data_dir.glob("wajima_*_shp")):
        inputarea_path = shp_dir / "inputarea.shp"
        floodarea_path = shp_dir / "floodarea.shp"
        if not inputarea_path.exists() or not floodarea_path.exists():
            continue
        inputarea = valid_geometry(gpd.read_file(inputarea_path))
        floodarea = valid_geometry(gpd.read_file(floodarea_path))
        if inputarea.empty or floodarea.empty:
            continue
        areas.append(
            {
                "name": shp_dir.name.replace("_shp", ""),
                "inputarea": inputarea,
                "floodarea": floodarea,
            }
        )
    return areas


def choose_pair(area: gpd.GeoDataFrame, pairs: list[SarPair]) -> SarPair | None:
    best_pair: SarPair | None = None
    best_overlap = 0.0
    for pair in pairs:
        with rasterio.open(pair.pre.path) as ds:
            projected = area.to_crs(ds.crs)
            overlap = projected.union_all().intersection(box(*ds.bounds)).area
        if overlap > best_overlap:
            best_overlap = overlap
            best_pair = pair
    return best_pair if best_overlap > 0 else None


def make_window(ds: rasterio.DatasetReader, geom) -> rasterio.windows.Window:
    bounds = geom.bounds
    window = from_bounds(*bounds, transform=ds.transform)
    window = window.round_offsets().round_lengths()
    col_off = max(0, int(window.col_off))
    row_off = max(0, int(window.row_off))
    width = min(ds.width - col_off, int(window.width))
    height = min(ds.height - row_off, int(window.height))
    if width <= 0 or height <= 0:
        raise ValueError("AOI is outside raster extent.")
    return rasterio.windows.Window(col_off, row_off, width, height)


def optimize_threshold(
    delta_db: np.ndarray,
    valid_mask: np.ndarray,
    aoi_mask: np.ndarray,
    flood_mask: np.ndarray,
    min_threshold_db: float,
    max_threshold_db: float,
) -> tuple[float, dict]:
    labels = flood_mask & aoi_mask & valid_mask
    candidates = np.linspace(min_threshold_db, max_threshold_db, 200)
    best = {"f1": -1.0, "precision": 0.0, "recall": 0.0, "tp": 0, "fp": 0, "fn": 0, "tn": 0}
    best_threshold = -2.0

    eval_mask = aoi_mask & valid_mask
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

    if best["f1"] <= 0:
        flood_values = delta_db[labels]
        if flood_values.size:
            best_threshold = float(np.nanpercentile(flood_values, 50))
            best_threshold = min(max(best_threshold, min_threshold_db), max_threshold_db)
        else:
            best_threshold = -2.0
    return best_threshold, best


def polygonize_mask(
    mask: np.ndarray,
    transform,
    crs,
    min_area_m2: float,
    attrs: dict,
) -> gpd.GeoDataFrame:
    records = []
    for geom, value in shapes(mask.astype("uint8"), mask=mask.astype(bool), transform=transform):
        if int(value) != 1:
            continue
        polygon = shape(geom)
        area_m2 = float(polygon.area)
        if area_m2 < min_area_m2:
            continue
        record = dict(attrs)
        record["area_m2"] = area_m2
        records.append({**record, "geometry": polygon})

    if not records:
        return gpd.GeoDataFrame(columns=[*attrs.keys(), "area_m2", "geometry"], geometry="geometry", crs=crs)
    return gpd.GeoDataFrame(records, geometry="geometry", crs=crs)


def process_area(
    area: dict,
    pair: SarPair,
    output_dir: Path,
    min_area_m2: float,
    min_threshold_db: float,
    max_threshold_db: float,
) -> dict:
    name = area["name"]
    with rasterio.open(pair.pre.path) as pre_ds, rasterio.open(pair.post.path) as post_ds:
        inputarea = area["inputarea"].to_crs(pre_ds.crs)
        floodarea = area["floodarea"].to_crs(pre_ds.crs)
        aoi_geom = unary_union(inputarea.geometry)
        flood_geom = unary_union(floodarea.geometry)
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

        aoi_mask = geometry_mask([aoi_geom], out_shape=delta_db.shape, transform=transform, invert=True)
        flood_mask = geometry_mask([flood_geom], out_shape=delta_db.shape, transform=transform, invert=True)
        valid_mask = np.isfinite(delta_db) & (np.asarray(pre_dn.filled(0)) > 0) & (post_dn > 0)

        threshold, metrics = optimize_threshold(
            delta_db,
            valid_mask,
            aoi_mask,
            flood_mask,
            min_threshold_db=min_threshold_db,
            max_threshold_db=max_threshold_db,
        )
        raw_detected = (delta_db <= threshold) & valid_mask & aoi_mask

        attrs = {
            "source": name,
            "frame": pair.frame,
            "pre_date": pair.pre.date,
            "post_date": pair.post.date,
            "threshold": threshold,
        }
        detected_gdf = polygonize_mask(raw_detected, transform, pre_ds.crs, min_area_m2, attrs)
        if not detected_gdf.empty:
            cleaned_mask = rasterize(
                detected_gdf.geometry,
                out_shape=raw_detected.shape,
                transform=transform,
                fill=0,
                default_value=1,
                dtype="uint8",
            ).astype(bool)
        else:
            cleaned_mask = np.zeros(raw_detected.shape, dtype=bool)

        profile = pre_ds.profile.copy()
        profile.update(
            driver="GTiff",
            height=delta_db.shape[0],
            width=delta_db.shape[1],
            transform=transform,
            count=1,
            compress="lzw",
        )

        delta_path = output_dir / f"{name}_delta_db.tif"
        delta_profile = profile.copy()
        delta_profile.update(dtype="float32", nodata=np.nan)
        with rasterio.open(delta_path, "w", **delta_profile) as dst:
            dst.write(delta_db.astype(np.float32), 1)

        mask_path = output_dir / f"{name}_detected_flood.tif"
        mask_profile = profile.copy()
        mask_profile.update(dtype="uint8", nodata=0)
        with rasterio.open(mask_path, "w", **mask_profile) as dst:
            dst.write(cleaned_mask.astype("uint8"), 1)

        vector_path = output_dir / f"{name}_detected_flood.geojson"
        if not detected_gdf.empty:
            detected_gdf.to_crs("EPSG:4326").to_file(vector_path, driver="GeoJSON")
        else:
            gpd.GeoDataFrame(
                columns=["source", "frame", "pre_date", "post_date", "threshold", "area_m2", "geometry"],
                geometry="geometry",
                crs="EPSG:4326",
            ).to_file(vector_path, driver="GeoJSON")

        pixel_area_m2 = abs(pre_ds.transform.a * pre_ds.transform.e)
        detected_area_m2 = float(np.count_nonzero(cleaned_mask) * pixel_area_m2)
        manual_area_m2 = float(floodarea.area.sum())
        valid_pixels = int(np.count_nonzero(valid_mask & aoi_mask))
        decrease_pixels = int(np.count_nonzero(raw_detected))

    return {
        "source": name,
        "frame": pair.frame,
        "pre_scene": pair.pre.path.name,
        "post_scene": pair.post.path.name,
        "threshold_db": threshold,
        "manual_area_m2": manual_area_m2,
        "detected_area_m2": detected_area_m2,
        "valid_pixels": valid_pixels,
        "raw_decrease_pixels": decrease_pixels,
        **metrics,
        "delta_tif": str(delta_path),
        "detected_tif": str(mask_path),
        "detected_geojson": str(vector_path),
    }


def write_report(output_dir: Path, rows: list[dict], pairs: list[SarPair]) -> None:
    report_path = output_dir / "report.md"
    lines = [
        "# ALOS-2 SAR 浸水判別レポート",
        "",
        "## 方法",
        "",
        "- 被災前（240811/240812）と被災後（240922/240923）の同一フレームをペア化した。",
        "- DN を `20*log10(DN) - 83` で後方散乱強度 dB に変換した。",
        "- 被災後画像を被災前画像のグリッドへ再投影し、`delta_db = post_db - pre_db` を計算した。",
        "- 各 `inputarea.shp` 内で `floodarea.shp` と最も整合する負のしきい値を探索し、`delta_db <= threshold` を浸水候補とした。",
        "- 100 m2 未満の小ポリゴンは GeoJSON と最終マスクから除外した。",
        "",
        "## 使用した SAR ペア",
        "",
    ]
    for pair in pairs:
        lines.append(f"- frame {pair.frame}: {pair.pre.path.name} -> {pair.post.path.name}")
    lines.extend(["", "## 結果", ""])
    if rows:
        lines.append("| source | frame | threshold dB | manual m2 | detected m2 | precision | recall | f1 |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for row in rows:
            lines.append(
                "| {source} | {frame} | {threshold_db:.3f} | {manual_area_m2:.1f} | "
                "{detected_area_m2:.1f} | {precision:.3f} | {recall:.3f} | {f1:.3f} |".format(**row)
            )
    else:
        lines.append("処理対象が見つかりませんでした。")
    lines.extend(
        [
            "",
            "## 出力",
            "",
            "- `*_delta_db.tif`: 後方散乱強度差分（被災後 - 被災前、dB）",
            "- `*_detected_flood.tif`: 浸水候補マスク",
            "- `*_detected_flood.geojson`: 浸水候補ポリゴン",
            "- `summary.csv`: しきい値、面積、評価指標の一覧",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    pairs = discover_sar_pairs(args.data_dir)
    areas = load_manual_areas(args.data_dir)
    if not pairs:
        raise RuntimeError(f"No SAR pre/post pairs found in {args.data_dir}")
    if not areas:
        raise RuntimeError(f"No wajima shapefile sets found in {args.data_dir}")

    rows: list[dict] = []
    detected_vectors: list[gpd.GeoDataFrame] = []
    for area in areas:
        pair = choose_pair(area["inputarea"], pairs)
        if pair is None:
            print(f"SKIP {area['name']}: no overlapping SAR pair")
            continue
        print(f"PROCESS {area['name']}: frame {pair.frame}")
        row = process_area(
            area,
            pair,
            args.output_dir,
            min_area_m2=args.min_area_m2,
            min_threshold_db=args.min_threshold_db,
            max_threshold_db=args.max_threshold_db,
        )
        rows.append(row)
        vector_path = Path(row["detected_geojson"])
        if vector_path.exists():
            gdf = gpd.read_file(vector_path)
            if not gdf.empty:
                detected_vectors.append(gdf)

    summary_path = args.output_dir / "summary.csv"
    if rows:
        fieldnames = list(rows[0].keys())
        with summary_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        summary_path.write_text("", encoding="utf-8")

    if detected_vectors:
        merged = gpd.GeoDataFrame(pd.concat([gdf.to_crs("EPSG:4326") for gdf in detected_vectors], ignore_index=True))
        merged = merged.set_geometry("geometry")
        merged.crs = "EPSG:4326"
        merged.to_file(args.output_dir / "detected_flood_merged.geojson", driver="GeoJSON")

    write_report(args.output_dir, rows, pairs)
    print(f"DONE: {args.output_dir}")


if __name__ == "__main__":
    main()
