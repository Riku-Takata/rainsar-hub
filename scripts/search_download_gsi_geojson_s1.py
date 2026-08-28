#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Search and download Sentinel-1 GRD scenes for GSI inundation GeoJSON masks.

This script uses backend/app/services/s1_cdse_client.py for STAC search and
OData download. All GeoJSON files in the mask directory are used as search
targets.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


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

from app.services.s1_cdse_client import S1CDSEClient, S1Scene  # noqa: E402


LOGGER = logging.getLogger(__name__)

BEFORE_START_UTC = datetime(2018, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
BEFORE_END_UTC = datetime(2018, 6, 30, 23, 59, 59, tzinfo=timezone.utc)
EVENT_START_UTC = datetime(2018, 7, 1, 0, 0, 0, tzinfo=timezone.utc)
EVENT_END_UTC = datetime(2018, 7, 8, 23, 59, 59, tzinfo=timezone.utc)
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1"
DEFAULT_GEOJSON_DIR = DEFAULT_OUTPUT_DIR / "geojson"


def iter_positions(geometry: dict[str, Any]) -> Iterable[tuple[float, float]]:
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    geometries = geometry.get("geometries")

    if geom_type == "GeometryCollection":
        for geom in geometries or []:
            yield from iter_positions(geom)
        return

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


def geojson_bbox(path: Path) -> tuple[float, float, float, float]:
    data = json.loads(path.read_text(encoding="utf-8"))
    positions: list[tuple[float, float]] = []

    if data.get("type") == "FeatureCollection":
        for feature in data.get("features", []):
            geom = feature.get("geometry")
            if geom:
                positions.extend(iter_positions(geom))
    elif data.get("type") == "Feature":
        positions.extend(iter_positions(data.get("geometry") or {}))
    else:
        positions.extend(iter_positions(data))

    if not positions:
        raise ValueError(f"No coordinates found in {path}")

    lons = [lon for lon, _lat in positions]
    lats = [lat for _lon, lat in positions]
    return min(lons), min(lats), max(lons), max(lats)


def find_target_geojsons(geojson_dir: Path) -> list[tuple[str, Path]]:
    targets: list[tuple[str, Path]] = []
    if not geojson_dir.exists():
        raise FileNotFoundError(f"GeoJSON directory not found: {geojson_dir}")

    for geojson_path in sorted(geojson_dir.glob("*.geojson")):
        targets.append((geojson_path.stem, geojson_path))

    if not targets:
        raise FileNotFoundError(f"No GeoJSON files found in: {geojson_dir}")
    return targets


def scene_product_name(scene: S1Scene) -> str:
    return scene.product_identifier or scene.stac_id


def scene_match_key(scene: S1Scene) -> tuple[str, str, str]:
    return (
        scene.platform or "",
        scene.orbit_direction or "",
        str(scene.relative_orbit or ""),
    )


def find_matching_before_scene(event_scene: S1Scene, before_scenes: list[S1Scene]) -> S1Scene | None:
    event_key = scene_match_key(event_scene)
    candidates = [
        scene
        for scene in before_scenes
        if scene_match_key(scene) == event_key and scene.acquisition_time < event_scene.acquisition_time
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda scene: scene.acquisition_time)


def write_search_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "area",
        "period",
        "matched_event_stac_id",
        "geojson",
        "min_lon",
        "min_lat",
        "max_lon",
        "max_lat",
        "stac_id",
        "product_identifier",
        "acquisition_time_utc",
        "platform",
        "product_type",
        "orbit_direction",
        "relative_orbit",
        "downloaded",
        "download_path",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--geojson-dir", type=Path, default=DEFAULT_GEOJSON_DIR)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true", help="Search only; do not download products.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    download_dir = args.output_dir / "downloads"
    result_csv = args.output_dir / "search_download_results.csv"

    client = S1CDSEClient()
    rows: list[dict[str, Any]] = []
    downloaded_products: dict[str, Path] = {}

    for area, geojson_path in find_target_geojsons(args.geojson_dir):
        min_lon, min_lat, max_lon, max_lat = geojson_bbox(geojson_path)
        LOGGER.info(
            "%s bbox: lon %.6f..%.6f, lat %.6f..%.6f",
            area,
            min_lon,
            max_lon,
            min_lat,
            max_lat,
        )

        event_scenes = client.search_grd_bbox_time(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            start=EVENT_START_UTC,
            end=EVENT_END_UTC,
            limit=args.limit,
        )
        LOGGER.info("%s event scenes: %d", area, len(event_scenes))

        if not event_scenes:
            LOGGER.info("%s skipped: no scenes from 2018-07-01 to 2018-07-08", area)
            continue

        before_scenes = client.search_grd_bbox_time(
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            start=BEFORE_START_UTC,
            end=BEFORE_END_UTC,
            limit=args.limit,
        )
        LOGGER.info("%s before scenes: %d", area, len(before_scenes))

        paired_scenes: list[tuple[str, S1Scene, str]] = []
        for event_scene in event_scenes:
            before_scene = find_matching_before_scene(event_scene, before_scenes)
            if before_scene is None:
                LOGGER.warning(
                    "%s no matching before scene for %s (%s/%s/relative_orbit=%s)",
                    area,
                    event_scene.stac_id,
                    event_scene.platform,
                    event_scene.orbit_direction,
                    event_scene.relative_orbit,
                )
                continue
            paired_scenes.append(("event", event_scene, ""))
            paired_scenes.append(("before", before_scene, event_scene.stac_id))

        LOGGER.info("%s matched event-before pairs: %d", area, len(paired_scenes) // 2)

        for period, scene, matched_event_stac_id in paired_scenes:
            product_name = scene_product_name(scene)
            downloaded = False
            download_path = ""
            if not args.dry_run and product_name not in downloaded_products:
                saved = client.download_product(product_name, download_dir)
                downloaded = saved is not None
                download_path = str(saved) if saved else ""
                if saved is not None:
                    downloaded_products[product_name] = saved
                time.sleep(1)
            elif product_name in downloaded_products:
                expected = downloaded_products[product_name]
                downloaded = expected.exists() and expected.stat().st_size > 0
                download_path = str(expected) if downloaded else ""

            rows.append(
                {
                    "area": area,
                    "period": period,
                    "matched_event_stac_id": matched_event_stac_id,
                    "geojson": str(geojson_path),
                    "min_lon": min_lon,
                    "min_lat": min_lat,
                    "max_lon": max_lon,
                    "max_lat": max_lat,
                    "stac_id": scene.stac_id,
                    "product_identifier": scene.product_identifier or "",
                    "acquisition_time_utc": scene.acquisition_time.isoformat(),
                    "platform": scene.platform or "",
                    "product_type": scene.product_type or "",
                    "orbit_direction": scene.orbit_direction or "",
                    "relative_orbit": scene.relative_orbit or "",
                    "downloaded": downloaded,
                    "download_path": download_path,
                }
            )
            write_search_csv(rows, result_csv)

    write_search_csv(rows, result_csv)
    LOGGER.info("result csv: %s", result_csv)
    LOGGER.info("matched scenes: %d", len(rows))
    LOGGER.info("downloaded scenes: %d", sum(1 for row in rows if row["downloaded"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
