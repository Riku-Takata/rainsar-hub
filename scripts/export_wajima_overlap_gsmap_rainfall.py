#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Export GSMaP rainfall records whose grid cells overlap confirmed flood polygons.

This script treats each GSMaP point as the center of a regular lon/lat grid cell
and exports all time-series rainfall records for cells intersecting the truth
polygons.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from datetime import timedelta, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import pymysql
from pymysql.cursors import SSCursor
from shapely.geometry import box


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
UTC_PLUS_9 = timezone(timedelta(hours=9))


DEFAULT_TRUTH_PATH = Path(r"D:\shuron\GT-data\sinsuiiki\shinsui.shp")
DEFAULT_OUTPUT_DIR = Path(r"E:\shuron")


@dataclass(frozen=True)
class OverlapGrid:
    lat: float
    lon: float
    db_grid_id: str | None
    export_grid_id: str
    cell_min_lon: float
    cell_min_lat: float
    cell_max_lon: float
    cell_max_lat: float
    overlap_area_m2: float
    cell_area_m2: float

    @property
    def overlap_ratio(self) -> float:
        if self.cell_area_m2 <= 0:
            return 0.0
        return self.overlap_area_m2 / self.cell_area_m2


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_args() -> argparse.Namespace:
    load_env_file(BACKEND_DIR / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--truth-path", type=Path, default=DEFAULT_TRUTH_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=3307)
    parser.add_argument("--db-user", default=os.environ.get("DB_USER", "rainsar"))
    parser.add_argument("--db-password", default=os.environ.get("DB_PASSWORD", "rainsar_pw"))
    parser.add_argument("--db-name", default=os.environ.get("DB_NAME", "rainsar_hub"))
    parser.add_argument(
        "--grid-size-deg",
        type=float,
        default=0.1,
        help="GSMaP lon/lat cell size in degrees. The default assumes 0.1-degree grid cells.",
    )
    parser.add_argument(
        "--assume-truth-crs",
        default="EPSG:4326",
        help="CRS to assign if the truth file has no CRS.",
    )
    parser.add_argument(
        "--output-prefix",
        default="wajima_inundation_overlap_gsmap",
        help="Prefix for output CSV and report files.",
    )
    return parser.parse_args()


def connect_db(args: argparse.Namespace) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        charset="utf8mb4",
        cursorclass=SSCursor,
        autocommit=True,
    )


def load_truth_geometry(truth_path: Path, assume_crs: str) -> tuple[Any, Any, Any, tuple[float, float, float, float]]:
    if not truth_path.exists():
        raise FileNotFoundError(f"Truth file not found: {truth_path}")

    truth = gpd.read_file(truth_path)
    truth = truth[truth.geometry.notna() & ~truth.geometry.is_empty].copy()
    if truth.empty:
        raise RuntimeError(f"Truth file has no valid geometry: {truth_path}")
    if truth.crs is None:
        truth = truth.set_crs(assume_crs)

    truth_wgs84 = truth.to_crs("EPSG:4326")
    truth_geom_wgs84 = truth_wgs84.geometry.union_all()
    projected_crs = truth_wgs84.estimate_utm_crs()
    truth_projected = truth_wgs84.to_crs(projected_crs)
    truth_geom_projected = truth_projected.geometry.union_all()
    return truth_geom_wgs84, truth_geom_projected, projected_crs, truth_geom_wgs84.bounds


def fetch_candidate_grids(
    conn: pymysql.connections.Connection,
    bounds: tuple[float, float, float, float],
    half_cell_deg: float,
) -> list[tuple[float, float, str | None]]:
    min_lon, min_lat, max_lon, max_lat = bounds
    query = """
        SELECT lat, lon, MIN(grid_id) AS grid_id
        FROM gsmap_points
        WHERE lat BETWEEN %s AND %s
          AND lon BETWEEN %s AND %s
        GROUP BY lat, lon
        ORDER BY lat, lon
    """
    params = (
        min_lat - half_cell_deg,
        max_lat + half_cell_deg,
        min_lon - half_cell_deg,
        max_lon + half_cell_deg,
    )

    with conn.cursor() as cur:
        cur.execute(query, params)
        return [(float(row[0]), float(row[1]), row[2]) for row in cur.fetchall()]


def find_overlapping_grids(
    candidates: list[tuple[float, float, str | None]],
    truth_geom_wgs84: Any,
    truth_geom_projected: Any,
    projected_crs: Any,
    grid_size_deg: float,
) -> list[OverlapGrid]:
    half = grid_size_deg / 2.0
    grid_rows: list[OverlapGrid] = []

    for lat, lon, db_grid_id in candidates:
        cell = box(lon - half, lat - half, lon + half, lat + half)
        if not cell.intersects(truth_geom_wgs84):
            continue

        cell_gdf = gpd.GeoSeries([cell], crs="EPSG:4326").to_crs(projected_crs)
        cell_projected = cell_gdf.iloc[0]
        overlap_area_m2 = float(cell_projected.intersection(truth_geom_projected).area)
        cell_area_m2 = float(cell_projected.area)
        if overlap_area_m2 <= 0:
            continue

        export_grid_id = db_grid_id or f"lat{lat:.2f}_lon{lon:.2f}"
        grid_rows.append(
            OverlapGrid(
                lat=lat,
                lon=lon,
                db_grid_id=db_grid_id,
                export_grid_id=export_grid_id,
                cell_min_lon=lon - half,
                cell_min_lat=lat - half,
                cell_max_lon=lon + half,
                cell_max_lat=lat + half,
                overlap_area_m2=overlap_area_m2,
                cell_area_m2=cell_area_m2,
            )
        )

    return sorted(grid_rows, key=lambda g: (g.lat, g.lon))


def write_grids_csv(path: Path, grids: list[OverlapGrid]) -> None:
    fieldnames = [
        "export_grid_id",
        "db_grid_id",
        "lat",
        "lon",
        "cell_min_lon",
        "cell_min_lat",
        "cell_max_lon",
        "cell_max_lat",
        "overlap_area_m2",
        "cell_area_m2",
        "overlap_ratio",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for grid in grids:
            writer.writerow(
                {
                    "export_grid_id": grid.export_grid_id,
                    "db_grid_id": grid.db_grid_id or "",
                    "lat": f"{grid.lat:.8f}",
                    "lon": f"{grid.lon:.8f}",
                    "cell_min_lon": f"{grid.cell_min_lon:.8f}",
                    "cell_min_lat": f"{grid.cell_min_lat:.8f}",
                    "cell_max_lon": f"{grid.cell_max_lon:.8f}",
                    "cell_max_lat": f"{grid.cell_max_lat:.8f}",
                    "overlap_area_m2": f"{grid.overlap_area_m2:.3f}",
                    "cell_area_m2": f"{grid.cell_area_m2:.3f}",
                    "overlap_ratio": f"{grid.overlap_ratio:.8f}",
                }
            )


def export_rainfall_csv(
    conn: pymysql.connections.Connection,
    path: Path,
    grids: list[OverlapGrid],
) -> int:
    query = """
        SELECT ts_utc, lat, lon, grid_id, gauge_mm_h, rain_mm_h, region, source_file
        FROM gsmap_points
        WHERE lat BETWEEN %s AND %s
          AND lon BETWEEN %s AND %s
        ORDER BY ts_utc
    """
    fieldnames = [
        "export_grid_id",
        "db_grid_id",
        "lat",
        "lon",
        "ts_utc",
        "ts_jst",
        "gauge_mm_h",
        "rain_mm_h",
        "region",
        "source_file",
        "cell_min_lon",
        "cell_min_lat",
        "cell_max_lon",
        "cell_max_lat",
        "overlap_area_m2",
        "overlap_ratio",
    ]

    total = 0
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for grid in grids:
            with conn.cursor() as cur:
                eps = 1.0e-4
                cur.execute(
                    query,
                    (grid.lat - eps, grid.lat + eps, grid.lon - eps, grid.lon + eps),
                )
                for row in cur:
                    ts_utc = row[0]
                    ts_jst = ""
                    if ts_utc is not None:
                        ts_jst = ts_utc.replace(tzinfo=timezone.utc).astimezone(UTC_PLUS_9).isoformat()
                    writer.writerow(
                        {
                            "export_grid_id": grid.export_grid_id,
                            "db_grid_id": row[3] or grid.db_grid_id or "",
                            "lat": f"{float(row[1]):.8f}",
                            "lon": f"{float(row[2]):.8f}",
                            "ts_utc": ts_utc.isoformat() if ts_utc is not None else "",
                            "ts_jst": ts_jst,
                            "gauge_mm_h": "" if row[4] is None else row[4],
                            "rain_mm_h": "" if row[5] is None else row[5],
                            "region": row[6] or "",
                            "source_file": row[7] or "",
                            "cell_min_lon": f"{grid.cell_min_lon:.8f}",
                            "cell_min_lat": f"{grid.cell_min_lat:.8f}",
                            "cell_max_lon": f"{grid.cell_max_lon:.8f}",
                            "cell_max_lat": f"{grid.cell_max_lat:.8f}",
                            "overlap_area_m2": f"{grid.overlap_area_m2:.3f}",
                            "overlap_ratio": f"{grid.overlap_ratio:.8f}",
                        }
                    )
                    total += 1
    return total


def write_report(
    path: Path,
    args: argparse.Namespace,
    bounds: tuple[float, float, float, float],
    candidate_count: int,
    grids: list[OverlapGrid],
    rainfall_count: int,
) -> None:
    min_lon, min_lat, max_lon, max_lat = bounds
    lines = [
        "# 浸水域重複GSMaP降水量エクスポート",
        "",
        f"- truth_path: `{args.truth_path}`",
        f"- output_dir: `{args.output_dir}`",
        f"- DB: `{args.db_host}:{args.db_port}/{args.db_name}`",
        f"- truth_bounds_epsg4326: `{min_lon:.8f}, {min_lat:.8f}, {max_lon:.8f}, {max_lat:.8f}`",
        f"- grid_size_deg: `{args.grid_size_deg}`",
        f"- candidate_grids: `{candidate_count}`",
        f"- overlapping_grids: `{len(grids)}`",
        f"- exported_rainfall_rows: `{rainfall_count}`",
        "",
        "## 出力ファイル",
        "",
        f"- `{args.output_prefix}_rainfall_points.csv`: 重複格子の全時系列降水量",
        f"- `{args.output_prefix}_grids.csv`: 浸水域と重なるGSMaP格子一覧",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    truth_geom_wgs84, truth_geom_projected, projected_crs, bounds = load_truth_geometry(
        args.truth_path,
        args.assume_truth_crs,
    )

    conn = connect_db(args)
    try:
        half_cell = args.grid_size_deg / 2.0
        candidates = fetch_candidate_grids(conn, bounds, half_cell)
        grids = find_overlapping_grids(
            candidates,
            truth_geom_wgs84,
            truth_geom_projected,
            projected_crs,
            args.grid_size_deg,
        )

        grids_csv = args.output_dir / f"{args.output_prefix}_grids.csv"
        rainfall_csv = args.output_dir / f"{args.output_prefix}_rainfall_points.csv"
        report_md = args.output_dir / f"{args.output_prefix}_report.md"

        write_grids_csv(grids_csv, grids)
        rainfall_count = export_rainfall_csv(conn, rainfall_csv, grids) if grids else 0
        write_report(report_md, args, bounds, len(candidates), grids, rainfall_count)
    finally:
        conn.close()

    print(f"candidate_grids={len(candidates)}")
    print(f"overlapping_grids={len(grids)}")
    print(f"exported_rainfall_rows={rainfall_count}")
    print(f"rainfall_csv={rainfall_csv}")
    print(f"grids_csv={grids_csv}")
    print(f"report={report_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
