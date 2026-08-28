#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Search and download Sentinel-1 GRD scenes 0-24h after heavy rain in Kanoya."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile


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
from app.services.s1_cdse_client import S1CDSEClient, S1Scene  # noqa: E402


LOGGER = logging.getLogger("kanoya_rain_s1")
JST = timezone(timedelta(hours=9))
UTC = timezone.utc
DEFAULT_GEOJSON = Path(r"D:\sotsuron\kanoya\kanoya.geojson")
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "kanoya_rain_s1"


def iter_positions(geometry: dict[str, Any]):
    coords = geometry.get("coordinates")

    def walk(value):
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


def geojson_bbox(path: Path) -> tuple[float, float, float, float]:
    data = json.loads(path.read_text(encoding="utf-8"))
    positions: list[tuple[float, float]] = []
    for feature in data.get("features", []):
        geometry = feature.get("geometry")
        if geometry:
            positions.extend(iter_positions(geometry))
    if not positions:
        raise ValueError(f"No coordinates found in {path}")
    lons = [lon for lon, _lat in positions]
    lats = [lat for _lon, lat in positions]
    return min(lons), min(lats), max(lons), max(lats)


def extract_rain_days(
    *,
    bbox: tuple[float, float, float, float],
    rain_buffer_deg: float,
    start_year: int,
    end_year: int,
    threshold: float,
) -> list[dict[str, Any]]:
    min_lon, min_lat, max_lon, max_lat = bbox
    min_lon -= rain_buffer_deg
    min_lat -= rain_buffer_deg
    max_lon += rain_buffer_deg
    max_lat += rain_buffer_deg
    start_utc = datetime(start_year, 1, 1, tzinfo=JST).astimezone(UTC).replace(tzinfo=None)
    end_utc = datetime(end_year + 1, 1, 1, tzinfo=JST).astimezone(UTC).replace(tzinfo=None)

    db = SessionLocal()
    try:
        points = (
            db.query(models.GsmapPoint)
            .filter(
                models.GsmapPoint.ts_utc >= start_utc,
                models.GsmapPoint.ts_utc < end_utc,
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

    days: dict[str, dict[str, Any]] = {}
    for point in points:
        ts_utc = point.ts_utc.replace(tzinfo=UTC)
        day_jst = ts_utc.astimezone(JST).date().isoformat()
        item = days.setdefault(
            day_jst,
            {
                "rain_day_jst": day_jst,
                "point_records": 0,
                "grid_points": set(),
                "max_mm_h": 0.0,
                "first_ts_utc": ts_utc,
                "last_ts_utc": ts_utc,
            },
        )
        item["point_records"] += 1
        item["grid_points"].add((float(point.lat), float(point.lon)))
        item["max_mm_h"] = max(float(item["max_mm_h"]), float(point.gauge_mm_h))
        item["first_ts_utc"] = min(item["first_ts_utc"], ts_utc)
        item["last_ts_utc"] = max(item["last_ts_utc"], ts_utc)

    rows: list[dict[str, Any]] = []
    for item in days.values():
        first_ts_utc = item["first_ts_utc"]
        last_ts_utc = item["last_ts_utc"]
        rows.append(
            {
                "rain_day_jst": item["rain_day_jst"],
                "point_records": item["point_records"],
                "grid_count": len(item["grid_points"]),
                "max_mm_h": item["max_mm_h"],
                "first_ts_utc": first_ts_utc.isoformat(),
                "last_ts_utc": last_ts_utc.isoformat(),
                "first_ts_jst": first_ts_utc.astimezone(JST).isoformat(),
                "last_ts_jst": last_ts_utc.astimezone(JST).isoformat(),
            }
        )
    return sorted(rows, key=lambda row: row["rain_day_jst"])


def search_s1_grd_bbox_time_any(
    client: S1CDSEClient,
    *,
    bbox: tuple[float, float, float, float],
    start: datetime,
    end: datetime,
    limit: int,
) -> list[S1Scene]:
    min_lon, min_lat, max_lon, max_lat = bbox
    params = {
        "collections": "sentinel-1-grd",
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "datetime": f"{start.isoformat().replace('+00:00', 'Z')}/{end.isoformat().replace('+00:00', 'Z')}",
        "limit": limit,
    }
    data = client._stac_search(params)
    return client._features_to_scenes(data)


def normalize_product_name(name: str) -> str:
    return name.removesuffix("_COG").removesuffix(".SAFE")


def scene_product_name(scene: S1Scene) -> str:
    return scene.product_identifier or scene.stac_id


def valid_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        with ZipFile(path) as archive:
            return archive.testzip() is None
    except BadZipFile:
        return False


def download_once(
    client: S1CDSEClient,
    product_name: str,
    download_dir: Path,
    cache: dict[str, Path],
    dry_run: bool,
) -> tuple[bool, str]:
    normalized = normalize_product_name(product_name)
    expected = download_dir / f"{normalized}.zip"
    if valid_zip(expected):
        cache[normalized] = expected
        return True, str(expected)
    if expected.exists():
        LOGGER.warning("Removing invalid zip: %s", expected)
        expected.unlink()
    if normalized in cache and valid_zip(cache[normalized]):
        return True, str(cache[normalized])
    if dry_run:
        return False, ""
    saved = client.download_product(product_name, download_dir)
    if saved is not None and valid_zip(saved):
        cache[normalized] = saved
        return True, str(saved)
    return False, str(saved) if saved else ""


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--threshold", type=float, default=10.0)
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2021)
    parser.add_argument("--after-hours", type=float, default=24.0)
    parser.add_argument(
        "--rain-buffer-deg",
        type=float,
        default=0.05,
        help="Buffer for GSMaP grid-center query. 0.05 deg roughly includes 0.1 deg cells overlapping the AOI.",
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    download_dir = args.output_dir / "downloads"
    bbox = geojson_bbox(args.geojson)
    LOGGER.info("bbox lon %.6f..%.6f lat %.6f..%.6f", bbox[0], bbox[2], bbox[1], bbox[3])

    rain_days = extract_rain_days(
        bbox=bbox,
        rain_buffer_deg=args.rain_buffer_deg,
        start_year=args.start_year,
        end_year=args.end_year,
        threshold=args.threshold,
    )
    rain_csv = args.output_dir / "kanoya_db_rain_days_ge10_2015_2021_jst.csv"
    rain_fields = [
        "rain_day_jst",
        "point_records",
        "grid_count",
        "max_mm_h",
        "first_ts_utc",
        "last_ts_utc",
        "first_ts_jst",
        "last_ts_jst",
    ]
    write_csv(rain_csv, rain_days, rain_fields)
    LOGGER.info("rain days: %d", len(rain_days))

    client = S1CDSEClient()
    result_csv = args.output_dir / "kanoya_s1_after_rain_0_24h_download.csv"
    result_fields = [
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
        "product_identifier",
        "acquisition_time_utc",
        "acquisition_time_jst",
        "delay_from_rain_start_h",
        "delay_from_rain_end_h",
        "timing",
        "platform",
        "product_type",
        "orbit_direction",
        "relative_orbit",
        "downloaded",
        "download_path",
    ]

    rows: list[dict[str, Any]] = []
    download_cache: dict[str, Path] = {}
    for index, rain_day in enumerate(rain_days, start=1):
        first_ts_utc = datetime.fromisoformat(rain_day["first_ts_utc"])
        last_ts_utc = datetime.fromisoformat(rain_day["last_ts_utc"])
        search_start_utc = first_ts_utc
        search_end_utc = first_ts_utc + timedelta(hours=args.after_hours)
        LOGGER.info("[%d/%d] %s search %s to %s", index, len(rain_days), rain_day["rain_day_jst"], search_start_utc, search_end_utc)
        scenes = search_s1_grd_bbox_time_any(
            client,
            bbox=bbox,
            start=search_start_utc,
            end=search_end_utc,
            limit=args.limit,
        )
        candidates = [
            scene
            for scene in scenes
            if first_ts_utc <= scene.acquisition_time <= search_end_utc
        ]
        if not candidates:
            rows.append(
                {
                    **rain_day,
                    "search_start_utc": search_start_utc.isoformat(),
                    "search_end_utc": search_end_utc.isoformat(),
                    "has_s1": False,
                    "stac_id": "",
                    "product_identifier": "",
                    "acquisition_time_utc": "",
                    "acquisition_time_jst": "",
                    "delay_from_rain_start_h": "",
                    "delay_from_rain_end_h": "",
                    "timing": "",
                    "platform": "",
                    "product_type": "",
                    "orbit_direction": "",
                    "relative_orbit": "",
                    "downloaded": False,
                    "download_path": "",
                }
            )
            write_csv(result_csv, rows, result_fields)
            continue

        for scene in candidates:
            delay_start_h = (scene.acquisition_time - first_ts_utc).total_seconds() / 3600.0
            delay_end_h = (scene.acquisition_time - last_ts_utc).total_seconds() / 3600.0
            timing = "during_rain_window" if scene.acquisition_time <= last_ts_utc else "after_rain_end"
            product_name = scene_product_name(scene)
            downloaded, download_path = download_once(client, product_name, download_dir, download_cache, args.dry_run)
            rows.append(
                {
                    **rain_day,
                    "search_start_utc": search_start_utc.isoformat(),
                    "search_end_utc": search_end_utc.isoformat(),
                    "has_s1": True,
                    "stac_id": scene.stac_id,
                    "product_identifier": scene.product_identifier or "",
                    "acquisition_time_utc": scene.acquisition_time.isoformat(),
                    "acquisition_time_jst": scene.acquisition_time.astimezone(JST).isoformat(),
                    "delay_from_rain_start_h": round(delay_start_h, 3),
                    "delay_from_rain_end_h": round(delay_end_h, 3),
                    "timing": timing,
                    "platform": scene.platform or "",
                    "product_type": scene.product_type or "",
                    "orbit_direction": scene.orbit_direction or "",
                    "relative_orbit": scene.relative_orbit or "",
                    "downloaded": downloaded,
                    "download_path": download_path,
                }
            )
            write_csv(result_csv, rows, result_fields)
            if not args.dry_run:
                time.sleep(1)

    write_csv(result_csv, rows, result_fields)
    hit_days = {row["rain_day_jst"] for row in rows if row["has_s1"]}
    hit_scenes = {row["stac_id"] for row in rows if row["stac_id"]}
    downloaded_scenes = {row["stac_id"] for row in rows if row["downloaded"] and row["stac_id"]}
    LOGGER.info("result csv: %s", result_csv)
    LOGGER.info("hit days: %d", len(hit_days))
    LOGGER.info("hit scenes: %d", len(hit_scenes))
    LOGGER.info("downloaded scenes: %d", len(downloaded_scenes))
    print(f"rain_days={len(rain_days)}")
    print(f"hit_days={len(hit_days)}")
    print(f"hit_scenes={len(hit_scenes)}")
    print(f"downloaded_scenes={len(downloaded_scenes)}")
    print(f"rain_csv={rain_csv}")
    print(f"result_csv={result_csv}")
    print(f"download_dir={download_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
