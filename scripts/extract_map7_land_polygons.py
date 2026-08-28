#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Extract map7 road/paddy related polygon data availability."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import zipfile
from pathlib import Path
from typing import Any, Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from sqlalchemy import func, text  # noqa: E402
from app.db.session import SessionLocal  # noqa: E402
from app.db import models  # noqa: E402


DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_land_polygons"
DEFAULT_ROAD_DIR = Path(r"D:\sotsuron\road-polygon")


def map7_bbox() -> tuple[float, float, float, float]:
    path = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "geojson" / "map (7).geojson"
    data = json.loads(path.read_text(encoding="utf-8"))
    coords = data["features"][0]["geometry"]["coordinates"][0]
    lons = [float(point[0]) for point in coords]
    lats = [float(point[1]) for point in coords]
    return min(lons), min(lats), max(lons), max(lats)


def iter_positions(geometry: dict[str, Any]) -> Iterable[tuple[float, float]]:
    coords = geometry.get("coordinates")

    def walk(value: Any) -> Iterable[tuple[float, float]]:
        if (
            isinstance(value, list)
            and len(value) >= 2
            and isinstance(value[0], (int, float))
            and isinstance(value[1], (int, float))
        ):
            yield float(value[0]), float(value[1])
            return
        if isinstance(value, list):
            for item in value:
                yield from walk(item)

    yield from walk(coords)


def geometry_intersects_bbox(geometry: dict[str, Any], bbox: tuple[float, float, float, float]) -> bool:
    min_lon, min_lat, max_lon, max_lat = bbox
    for lon, lat in iter_positions(geometry):
        if min_lon <= lon <= max_lon and min_lat <= lat <= max_lat:
            return True
    return False


def road_mesh_codes_for_bbox(bbox: tuple[float, float, float, float]) -> set[str]:
    min_lon, min_lat, max_lon, max_lat = bbox
    codes: set[str] = set()
    for lat_int in range(int(min_lat), int(max_lat) + 2):
        for lon_int in range(int(min_lon), int(max_lon) + 2):
            p = int(lat_int * 1.5)
            u = lon_int - 100
            codes.add(f"{p:02d}{u:02d}")
    return codes


def extract_road_source_geojson(road_dir: Path, bbox: tuple[float, float, float, float]) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    mesh_codes = road_mesh_codes_for_bbox(bbox)
    for code in sorted(mesh_codes):
        zpath = road_dir / f"N13-24_{code}_GEOJSON.zip"
        if not zpath.exists():
            continue
        with zipfile.ZipFile(zpath) as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".geojson"):
                    continue
                data = json.loads(zf.read(name).decode("utf-8"))
                for feature in data.get("features", []):
                    geometry = feature.get("geometry")
                    if geometry and geometry_intersects_bbox(geometry, bbox):
                        features.append(feature)
    return features


def fude_rows_to_features(rows) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    for row in rows:
        geometry = row.geometry
        if isinstance(geometry, str):
            geometry = json.loads(geometry)
        properties = {
            "polygon_uuid": row.polygon_uuid,
            "land_type": row.land_type,
            "issue_year": row.issue_year,
            "edit_year": row.edit_year,
            "history": row.history,
            "last_polygon_uuid": row.last_polygon_uuid,
            "prev_last_polygon_uuid": row.prev_last_polygon_uuid,
            "local_government_cd": row.local_government_cd,
            "point_lng": row.point_lng,
            "point_lat": row.point_lat,
            "old_polygon_id": row.old_polygon_id,
            "pref_id": row.pref_id,
        }
        features.append({"type": "Feature", "geometry": geometry, "properties": properties})
    return features


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--road-dir", type=Path, default=DEFAULT_ROAD_DIR)
    args = parser.parse_args()

    # Allow overriding Docker service host from shell.
    os.environ.setdefault("DB_HOST", "127.0.0.1")
    os.environ.setdefault("DB_PORT", "3307")

    bbox = map7_bbox()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    db = SessionLocal()
    try:
        fude_count = int(db.query(func.count(models.FudePolygon.id)).scalar() or 0)
        road_count = int(db.query(func.count(models.RoadPolygon.id)).scalar() or 0)
        road_grids = db.execute(
            text(
                """
                SELECT grid_id, lat, lon
                FROM japan_road_grids
                WHERE lat BETWEEN :min_lat AND :max_lat
                  AND lon BETWEEN :min_lon AND :max_lon
                ORDER BY lat, lon
                """
            ),
            {
                "min_lon": bbox[0],
                "min_lat": bbox[1],
                "max_lon": bbox[2],
                "max_lat": bbox[3],
            },
        ).fetchall()
        fude_rows = db.execute(
            text(
                """
                SELECT polygon_uuid, land_type, issue_year, edit_year, history,
                       last_polygon_uuid, prev_last_polygon_uuid, local_government_cd,
                       point_lng, point_lat, old_polygon_id, pref_id, geometry
                FROM fude_polygons
                WHERE point_lat BETWEEN :min_lat AND :max_lat
                  AND point_lng BETWEEN :min_lon AND :max_lon
                ORDER BY pref_id, local_government_cd, polygon_uuid
                """
            ),
            {
                "min_lon": bbox[0],
                "min_lat": bbox[1],
                "max_lon": bbox[2],
                "max_lat": bbox[3],
            },
        ).fetchall()
        fude_land_type_counts = db.execute(
            text(
                """
                SELECT land_type, COUNT(*) AS count
                FROM fude_polygons
                WHERE point_lat BETWEEN :min_lat AND :max_lat
                  AND point_lng BETWEEN :min_lon AND :max_lon
                GROUP BY land_type
                ORDER BY land_type
                """
            ),
            {
                "min_lon": bbox[0],
                "min_lat": bbox[1],
                "max_lon": bbox[2],
                "max_lat": bbox[3],
            },
        ).fetchall()
    finally:
        db.close()

    road_grid_csv = args.output_dir / "map7_road_grids_from_db.csv"
    with road_grid_csv.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["grid_id", "lat", "lon"])
        writer.writerows(road_grids)

    road_features: list[dict[str, Any]] = []
    if args.road_dir.exists():
        road_features = extract_road_source_geojson(args.road_dir, bbox)

    road_geojson = args.output_dir / "map7_road_polygons.geojson"
    road_geojson.write_text(
        json.dumps({"type": "FeatureCollection", "features": road_features}, ensure_ascii=False),
        encoding="utf-8",
    )

    fude_features = fude_rows_to_features(fude_rows)
    fude_geojson = args.output_dir / "map7_fude_polygons_from_db.geojson"
    fude_geojson.write_text(
        json.dumps({"type": "FeatureCollection", "features": fude_features}, ensure_ascii=False),
        encoding="utf-8",
    )

    paddy_features = [
        feature for feature in fude_features if feature["properties"].get("land_type") == 100
    ]
    paddy_geojson = args.output_dir / "map7_fude_paddy_polygons_from_db.geojson"
    paddy_geojson.write_text(
        json.dumps({"type": "FeatureCollection", "features": paddy_features}, ensure_ascii=False),
        encoding="utf-8",
    )

    summary = {
        "fude_polygons_db_count": fude_count,
        "road_polygons_db_count": road_count,
        "map7_road_grids_from_db_count": len(road_grids),
        "map7_road_features_extracted_from_source_count": len(road_features),
        "map7_fude_polygons_from_db_count": len(fude_features),
        "map7_fude_paddy_polygons_from_db_count": len(paddy_features),
        "map7_fude_land_type_counts": {
            str(row.land_type): int(row.count) for row in fude_land_type_counts
        },
        "road_grid_csv": str(road_grid_csv),
        "road_geojson": str(road_geojson),
        "fude_geojson": str(fude_geojson),
        "fude_paddy_geojson": str(paddy_geojson),
    }
    summary_path = args.output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
