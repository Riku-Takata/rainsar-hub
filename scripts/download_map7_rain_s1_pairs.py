#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Build and download map7 rain/no-rain Sentinel-1 pairs."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
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

from sqlalchemy import func  # noqa: E402
from app.db.session import SessionLocal  # noqa: E402
from app.db import models  # noqa: E402
from app.services.s1_cdse_client import S1CDSEClient, S1Scene  # noqa: E402


LOGGER = logging.getLogger("map7_s1_pairs")
JST = timezone(timedelta(hours=9))
UTC = timezone.utc
DEFAULT_INPUT_CSV = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "map7_db_rain_days_s1_delay_all_jst.csv"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"


def map7_bbox() -> tuple[float, float, float, float]:
    path = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "geojson" / "map (7).geojson"
    data = json.loads(path.read_text(encoding="utf-8"))
    coords = data["features"][0]["geometry"]["coordinates"][0]
    lons = [float(point[0]) for point in coords]
    lats = [float(point[1]) for point in coords]
    return min(lons), min(lats), max(lons), max(lats)


def parse_scene_parts(stac_id: str) -> dict[str, str]:
    parts = stac_id.removesuffix("_COG").split("_")
    if len(parts) < 4:
        return {"mission": "", "mode": "", "product": "", "resolution": "", "pol_token": ""}
    return {
        "mission": parts[0],
        "mode": parts[1],
        "product": parts[2],
        "resolution": parts[2][-1:] if parts[2] else "",
        "pol_token": parts[3],
    }


def scene_product_name(scene: S1Scene) -> str:
    return scene.product_identifier or scene.stac_id


def normalize_product_name(name: str) -> str:
    return name.removesuffix("_COG").removesuffix(".SAFE")


def scene_key_from_row(row: dict[str, str]) -> tuple[str, str, str, str, str, str]:
    parts = parse_scene_parts(row["stac_id"])
    return (
        row.get("platform", ""),
        row.get("orbit_direction", ""),
        str(row.get("relative_orbit", "")),
        parts["mode"],
        parts["product"],
        parts["pol_token"],
    )


def scene_key(scene: S1Scene) -> tuple[str, str, str, str, str, str]:
    parts = parse_scene_parts(scene.stac_id)
    return (
        scene.platform or "",
        scene.orbit_direction or "",
        str(scene.relative_orbit or ""),
        parts["mode"],
        parts["product"],
        parts["pol_token"],
    )


def is_no_heavy_rain_day(
    *,
    day_jst: str,
    bbox: tuple[float, float, float, float],
    threshold: float,
) -> bool:
    min_lon, min_lat, max_lon, max_lat = bbox
    day = datetime.strptime(day_jst, "%Y-%m-%d").date()
    start_utc = datetime.combine(day, datetime.min.time(), JST).astimezone(UTC).replace(tzinfo=None)
    end_utc = datetime.combine(day + timedelta(days=1), datetime.min.time(), JST).astimezone(UTC).replace(tzinfo=None)
    db = SessionLocal()
    try:
        count = (
            db.query(func.count(models.GsmapPoint.id))
            .filter(
                models.GsmapPoint.ts_utc >= start_utc,
                models.GsmapPoint.ts_utc < end_utc,
                models.GsmapPoint.lat >= min_lat,
                models.GsmapPoint.lat <= max_lat,
                models.GsmapPoint.lon >= min_lon,
                models.GsmapPoint.lon <= max_lon,
                models.GsmapPoint.gauge_mm_h >= threshold,
            )
            .scalar()
        )
        return int(count or 0) == 0
    finally:
        db.close()


def search_s1_grd_bbox_time_any(
    client: S1CDSEClient,
    *,
    bbox: tuple[float, float, float, float],
    start: datetime,
    end: datetime,
    limit: int,
) -> list[S1Scene]:
    min_lon, min_lat, max_lon, max_lat = bbox
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    params = {
        "collections": "sentinel-1-grd",
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "datetime": f"{start.isoformat().replace('+00:00', 'Z')}/{end.isoformat().replace('+00:00', 'Z')}",
        "limit": limit,
    }
    data = client._stac_search(params)
    return client._features_to_scenes(data)


def find_pair_scene(
    client: S1CDSEClient,
    *,
    target: dict[str, str],
    bbox: tuple[float, float, float, float],
    threshold: float,
    lookback_days: int,
    limit: int,
) -> S1Scene | None:
    target_dt = datetime.fromisoformat(target["acquisition_time_utc"])
    target_key = scene_key_from_row(target)
    scenes = search_s1_grd_bbox_time_any(
        client,
        bbox=bbox,
        start=target_dt - timedelta(days=lookback_days),
        end=target_dt - timedelta(seconds=1),
        limit=limit,
    )
    candidates = [
        scene
        for scene in scenes
        if scene.acquisition_time < target_dt and scene_key(scene) == target_key
    ]
    candidates.sort(key=lambda scene: scene.acquisition_time, reverse=True)
    for scene in candidates:
        day_jst = scene.acquisition_time.astimezone(JST).date().isoformat()
        if is_no_heavy_rain_day(day_jst=day_jst, bbox=bbox, threshold=threshold):
            return scene
    return None


def download_once(
    client: S1CDSEClient,
    product_name: str,
    download_dir: Path,
    cache: dict[str, Path],
    dry_run: bool,
) -> tuple[bool, str]:
    normalized = normalize_product_name(product_name)
    expected = download_dir / f"{normalized}.zip"
    if expected.exists() and expected.stat().st_size > 0:
        try:
            with ZipFile(expected) as zf:
                bad_member = zf.testzip()
            if bad_member is None:
                cache[normalized] = expected
                return True, str(expected)
            LOGGER.warning("Removing corrupt zip %s (bad member: %s)", expected, bad_member)
        except BadZipFile:
            LOGGER.warning("Removing corrupt zip %s", expected)
        expected.unlink()
    if dry_run:
        return False, ""
    if normalized in cache and cache[normalized].exists():
        return True, str(cache[normalized])
    saved = client.download_product(product_name, download_dir)
    if saved is not None:
        cache[normalized] = saved
        return True, str(saved)
    return False, ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--threshold", type=float, default=10.0)
    parser.add_argument("--lookback-days", type=int, default=180)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    download_dir = args.output_dir / "downloads"
    result_csv = args.output_dir / "map7_s1_rain_no_rain_pairs_download.csv"

    bbox = map7_bbox()
    rows = list(csv.DictReader(args.input_csv.open(encoding="utf-8-sig")))
    targets = [
        row
        for row in rows
        if row.get("has_s1") == "True" and row.get("stac_id") and row.get("timing") != "before_rain_start"
    ]

    client = S1CDSEClient()
    download_cache: dict[str, Path] = {}
    results: list[dict[str, Any]] = []

    LOGGER.info("target scenes: %d", len(targets))
    for index, target in enumerate(targets, 1):
        LOGGER.info("[%d/%d] target: %s", index, len(targets), target["stac_id"])
        pair = find_pair_scene(
            client,
            target=target,
            bbox=bbox,
            threshold=args.threshold,
            lookback_days=args.lookback_days,
            limit=args.limit,
        )
        if pair is None:
            LOGGER.warning("No no-rain matching pair for %s", target["stac_id"])
            continue

        target_name = normalize_product_name(target["stac_id"])
        pair_name = normalize_product_name(scene_product_name(pair))
        target_downloaded, target_path = download_once(client, target_name, download_dir, download_cache, args.dry_run)
        time.sleep(1)
        pair_downloaded, pair_path = download_once(client, pair_name, download_dir, download_cache, args.dry_run)
        time.sleep(1)

        results.append(
            {
                "rain_day_jst": target["rain_day_jst"],
                "timing": target["timing"],
                "rain_first_ts_jst": target["first_ts_jst"],
                "rain_last_ts_jst": target["last_ts_jst"],
                "rain_max_mm_h": target["max_mm_h"],
                "target_stac_id": target["stac_id"],
                "target_acquisition_time_utc": target["acquisition_time_utc"],
                "target_acquisition_time_jst": target["acquisition_time_jst"],
                "target_downloaded": target_downloaded,
                "target_download_path": target_path,
                "pair_stac_id": pair.stac_id,
                "pair_acquisition_time_utc": pair.acquisition_time.isoformat(),
                "pair_acquisition_time_jst": pair.acquisition_time.astimezone(JST).isoformat(),
                "pair_no_heavy_rain_day_jst": pair.acquisition_time.astimezone(JST).date().isoformat(),
                "pair_downloaded": pair_downloaded,
                "pair_download_path": pair_path,
                "platform": target["platform"],
                "orbit_direction": target["orbit_direction"],
                "relative_orbit": target["relative_orbit"],
            }
        )
        with result_csv.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)

    if results:
        with result_csv.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
    LOGGER.info("pairs: %d", len(results))
    LOGGER.info("target downloaded rows: %d", sum(1 for row in results if row["target_downloaded"]))
    LOGGER.info("pair downloaded rows: %d", sum(1 for row in results if row["pair_downloaded"]))
    LOGGER.info("result csv: %s", result_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
