#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Build and download Kanoya rain/no-rain Sentinel-1 GRD pairs."""

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

from sqlalchemy import func  # noqa: E402
from app.db.session import SessionLocal  # noqa: E402
from app.db import models  # noqa: E402
from app.services.s1_cdse_client import S1CDSEClient, S1Scene  # noqa: E402


LOGGER = logging.getLogger("kanoya_s1_pairs")
JST = timezone(timedelta(hours=9))
UTC = timezone.utc
DEFAULT_GEOJSON = Path(r"D:\sotsuron\kanoya\kanoya.geojson")
DEFAULT_INPUT_CSV = ROOT_DIR / "output" / "kanoya_rain_s1" / "kanoya_s1_after_rain_0_24h_download.csv"
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


def buffered_bbox(bbox: tuple[float, float, float, float], buffer_deg: float) -> tuple[float, float, float, float]:
    min_lon, min_lat, max_lon, max_lat = bbox
    return min_lon - buffer_deg, min_lat - buffer_deg, max_lon + buffer_deg, max_lat + buffer_deg


def parse_scene_parts(stac_id: str) -> dict[str, str]:
    parts = stac_id.removesuffix("_COG").split("_")
    if len(parts) < 4:
        return {"mode": "", "product": "", "pol_token": ""}
    return {"mode": parts[1], "product": parts[2], "pol_token": parts[3]}


def normalize_product_name(name: str) -> str:
    return name.removesuffix("_COG").removesuffix(".SAFE")


def scene_product_name(scene: S1Scene) -> str:
    return scene.product_identifier or scene.stac_id


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


def valid_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        with ZipFile(path) as archive:
            return archive.testzip() is None
    except BadZipFile:
        return False


def is_no_heavy_rain_day(
    *,
    day_jst: str,
    rain_bbox: tuple[float, float, float, float],
    threshold: float,
) -> bool:
    min_lon, min_lat, max_lon, max_lat = rain_bbox
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
    search_bbox: tuple[float, float, float, float],
    rain_bbox: tuple[float, float, float, float],
    threshold: float,
    lookback_days: int,
    limit: int,
) -> S1Scene | None:
    target_dt = datetime.fromisoformat(target["acquisition_time_utc"])
    target_key = scene_key_from_row(target)
    scenes = search_s1_grd_bbox_time_any(
        client,
        bbox=search_bbox,
        start=target_dt - timedelta(days=lookback_days),
        end=target_dt - timedelta(seconds=1),
        limit=limit,
    )
    candidates = [scene for scene in scenes if scene.acquisition_time < target_dt and scene_key(scene) == target_key]
    candidates.sort(key=lambda scene: scene.acquisition_time, reverse=True)
    for scene in candidates:
        day_jst = scene.acquisition_time.astimezone(JST).date().isoformat()
        if is_no_heavy_rain_day(day_jst=day_jst, rain_bbox=rain_bbox, threshold=threshold):
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
    if valid_zip(expected):
        cache[normalized] = expected
        return True, str(expected)
    if expected.exists():
        LOGGER.warning("Removing invalid zip: %s", expected)
        expected.unlink()
    if dry_run:
        return False, ""
    if normalized in cache and valid_zip(cache[normalized]):
        return True, str(cache[normalized])
    saved = client.download_product(product_name, download_dir)
    if saved is not None and valid_zip(saved):
        cache[normalized] = saved
        return True, str(saved)
    return False, str(saved) if saved else ""


def write_results(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON)
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--threshold", type=float, default=10.0)
    parser.add_argument("--rain-buffer-deg", type=float, default=0.05)
    parser.add_argument("--lookback-days", type=int, default=180)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    download_dir = args.output_dir / "downloads"
    result_csv = args.output_dir / "kanoya_s1_rain_no_rain_pairs_download.csv"
    bbox = geojson_bbox(args.geojson)
    rain_bbox = buffered_bbox(bbox, args.rain_buffer_deg)

    rows = list(csv.DictReader(args.input_csv.open(encoding="utf-8-sig")))
    targets = [
        row
        for row in rows
        if row.get("has_s1") == "True"
        and row.get("stac_id")
        and row.get("timing") != "before_rain_start"
    ]

    client = S1CDSEClient()
    download_cache: dict[str, Path] = {}
    results: list[dict[str, Any]] = []

    LOGGER.info("target rows: %d", len(targets))
    for index, target in enumerate(targets, 1):
        LOGGER.info("[%d/%d] target: %s", index, len(targets), target["stac_id"])
        pair = find_pair_scene(
            client,
            target=target,
            search_bbox=bbox,
            rain_bbox=rain_bbox,
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

        pair_day_jst = pair.acquisition_time.astimezone(JST).date().isoformat()
        results.append(
            {
                "rain_day_jst": target["rain_day_jst"],
                "timing": target["timing"],
                "rain_first_ts_jst": target["first_ts_jst"],
                "rain_last_ts_jst": target["last_ts_jst"],
                "rain_max_mm_h": target["max_mm_h"],
                "delay_from_rain_start_h": target["delay_from_rain_start_h"],
                "delay_from_rain_end_h": target["delay_from_rain_end_h"],
                "target_stac_id": target["stac_id"],
                "target_acquisition_time_utc": target["acquisition_time_utc"],
                "target_acquisition_time_jst": target["acquisition_time_jst"],
                "target_downloaded": target_downloaded,
                "target_download_path": target_path,
                "pair_stac_id": pair.stac_id,
                "pair_acquisition_time_utc": pair.acquisition_time.isoformat(),
                "pair_acquisition_time_jst": pair.acquisition_time.astimezone(JST).isoformat(),
                "pair_no_heavy_rain_day_jst": pair_day_jst,
                "pair_downloaded": pair_downloaded,
                "pair_download_path": pair_path,
                "platform": target["platform"],
                "orbit_direction": target["orbit_direction"],
                "relative_orbit": target["relative_orbit"],
            }
        )
        write_results(result_csv, results)

    write_results(result_csv, results)
    LOGGER.info("pairs: %d", len(results))
    LOGGER.info("target downloaded rows: %d", sum(1 for row in results if row["target_downloaded"]))
    LOGGER.info("pair downloaded rows: %d", sum(1 for row in results if row["pair_downloaded"]))
    LOGGER.info("result csv: %s", result_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
