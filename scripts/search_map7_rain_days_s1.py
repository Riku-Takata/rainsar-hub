#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Search Sentinel-1 scenes on map7 rain days extracted from DB."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file(BACKEND_DIR / ".env")

from app.db.session import SessionLocal  # noqa: E402
from app.db import models  # noqa: E402
from app.services.s1_cdse_client import S1CDSEClient  # noqa: E402


JST = timezone(timedelta(hours=9))
UTC = timezone.utc


def map7_bbox() -> tuple[float, float, float, float]:
    path = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "geojson" / "map (7).geojson"
    data = json.loads(path.read_text(encoding="utf-8"))
    coords = data["features"][0]["geometry"]["coordinates"][0]
    lons = [float(point[0]) for point in coords]
    lats = [float(point[1]) for point in coords]
    return min(lon for lon in lons), min(lats), max(lons), max(lats)


def extract_rain_days(
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    threshold: float,
) -> list[dict[str, object]]:
    db = SessionLocal()
    try:
        points = (
            db.query(models.GsmapPoint)
            .filter(
                models.GsmapPoint.lat >= min_lat,
                models.GsmapPoint.lat <= max_lat,
                models.GsmapPoint.lon >= min_lon,
                models.GsmapPoint.lon <= max_lon,
                models.GsmapPoint.gauge_mm_h >= threshold,
            )
            .order_by(models.GsmapPoint.ts_utc)
            .all()
        )
    finally:
        db.close()

    days: dict[str, dict[str, object]] = {}
    for point in points:
        ts_utc = point.ts_utc.replace(tzinfo=UTC)
        day_jst = ts_utc.astimezone(JST).date().isoformat()
        item = days.setdefault(
            day_jst,
            {
                "rain_day_jst": day_jst,
                "point_records": 0,
                "grid_count": set(),
                "max_mm_h": 0.0,
                "first_ts_utc": ts_utc,
                "last_ts_utc": ts_utc,
            },
        )
        item["point_records"] = int(item["point_records"]) + 1
        item["grid_count"].add((float(point.lat), float(point.lon)))  # type: ignore[union-attr]
        item["max_mm_h"] = max(float(item["max_mm_h"]), float(point.gauge_mm_h))
        item["first_ts_utc"] = min(item["first_ts_utc"], ts_utc)  # type: ignore[arg-type]
        item["last_ts_utc"] = max(item["last_ts_utc"], ts_utc)  # type: ignore[arg-type]

    rows: list[dict[str, object]] = []
    for item in days.values():
        first_ts_utc = item["first_ts_utc"]
        last_ts_utc = item["last_ts_utc"]
        rows.append(
            {
                "rain_day_jst": item["rain_day_jst"],
                "point_records": item["point_records"],
                "grid_count": len(item["grid_count"]),  # type: ignore[arg-type]
                "max_mm_h": item["max_mm_h"],
                "first_ts_utc": first_ts_utc.isoformat(),  # type: ignore[union-attr]
                "last_ts_utc": last_ts_utc.isoformat(),  # type: ignore[union-attr]
                "first_ts_jst": first_ts_utc.astimezone(JST).isoformat(),  # type: ignore[union-attr]
                "last_ts_jst": last_ts_utc.astimezone(JST).isoformat(),  # type: ignore[union-attr]
            }
        )
    return sorted(rows, key=lambda row: str(row["rain_day_jst"]))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=float, default=10.0)
    parser.add_argument("--output-dir", type=Path, default=ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    min_lon, min_lat, max_lon, max_lat = map7_bbox()
    rain_days = extract_rain_days(min_lon, min_lat, max_lon, max_lat, args.threshold)

    rain_csv = args.output_dir / "map7_db_rain_days_ge10_all_jst.csv"
    with rain_csv.open("w", encoding="utf-8-sig", newline="") as f:
        fields = [
            "rain_day_jst",
            "point_records",
            "grid_count",
            "max_mm_h",
            "first_ts_utc",
            "last_ts_utc",
            "first_ts_jst",
            "last_ts_jst",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rain_days)

    client = S1CDSEClient()
    match_rows: list[dict[str, object]] = []
    for rain_day in rain_days:
        day = datetime.strptime(str(rain_day["rain_day_jst"]), "%Y-%m-%d").date()
        search_start_utc = datetime.combine(day, datetime.min.time(), JST).astimezone(UTC)
        search_end_utc = datetime.combine(day, datetime.max.time().replace(microsecond=0), JST).astimezone(UTC)
        scenes = client.search_grd_bbox_time(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            start=search_start_utc,
            end=search_end_utc,
            limit=100,
        )
        if not scenes:
            match_rows.append(
                {
                    **rain_day,
                    "search_start_utc": search_start_utc.isoformat(),
                    "search_end_utc": search_end_utc.isoformat(),
                    "has_s1": False,
                    "stac_id": "",
                    "acquisition_time_utc": "",
                    "acquisition_time_jst": "",
                    "platform": "",
                    "orbit_direction": "",
                    "relative_orbit": "",
                }
            )
            continue
        for scene in scenes:
            match_rows.append(
                {
                    **rain_day,
                    "search_start_utc": search_start_utc.isoformat(),
                    "search_end_utc": search_end_utc.isoformat(),
                    "has_s1": True,
                    "stac_id": scene.stac_id,
                    "acquisition_time_utc": scene.acquisition_time.isoformat(),
                    "acquisition_time_jst": scene.acquisition_time.astimezone(JST).isoformat(),
                    "platform": scene.platform or "",
                    "orbit_direction": scene.orbit_direction or "",
                    "relative_orbit": scene.relative_orbit or "",
                }
            )

    match_csv = args.output_dir / "map7_db_rain_days_s1_matches_all_jst.csv"
    with match_csv.open("w", encoding="utf-8-sig", newline="") as f:
        fields = [
            "rain_day_jst",
            "point_records",
            "grid_count",
            "max_mm_h",
            "first_ts_utc",
            "last_ts_utc",
            "first_ts_jst",
            "last_ts_jst",
            "search_start_utc",
            "search_end_utc",
            "has_s1",
            "stac_id",
            "acquisition_time_utc",
            "acquisition_time_jst",
            "platform",
            "orbit_direction",
            "relative_orbit",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(match_rows)

    hit_days = {str(row["rain_day_jst"]) for row in match_rows if row["has_s1"]}
    hit_scenes = {str(row["stac_id"]) for row in match_rows if row["stac_id"]}
    print(f"rain_days={len(rain_days)}")
    print(f"s1_hit_days={len(hit_days)}")
    print(f"s1_hit_scenes={len(hit_scenes)}")
    print(f"rain_csv={rain_csv}")
    print(f"match_csv={match_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
