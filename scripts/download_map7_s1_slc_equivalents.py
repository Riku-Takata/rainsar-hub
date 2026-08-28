#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Search and download SLC equivalents for already downloaded map7 GRD pairs."""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import zipfile


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


LOGGER = logging.getLogger("map7_slc_equivalents")
UTC = timezone.utc
DEFAULT_INPUT_CSV = (
    ROOT_DIR
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "map7_s1_rain_no_rain_pairs_download.csv"
)
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"


def normalize_product_name(name: str) -> str:
    return name.removesuffix("_COG").removesuffix(".SAFE")


def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_scene_parts(stac_id: str) -> dict[str, str]:
    parts = normalize_product_name(stac_id).split("_")
    return {
        "mission": parts[0] if len(parts) > 0 else "",
        "mode": parts[1] if len(parts) > 1 else "",
        "product": parts[2] if len(parts) > 2 else "",
        "pol_token": parts[3] if len(parts) > 3 else "",
    }


def map7_bbox() -> tuple[float, float, float, float]:
    import json

    path = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "geojson" / "map (7).geojson"
    data = json.loads(path.read_text(encoding="utf-8"))
    coords = data["features"][0]["geometry"]["coordinates"][0]
    lons = [float(point[0]) for point in coords]
    lats = [float(point[1]) for point in coords]
    return min(lons), min(lats), max(lons), max(lats)


def unique_grd_scenes(input_csv: Path) -> list[dict[str, str]]:
    rows = list(csv.DictReader(input_csv.open(encoding="utf-8-sig")))
    by_id: dict[str, dict[str, str]] = {}
    for row in rows:
        for prefix, role in [("target", "target"), ("pair", "before")]:
            stac_id = row.get(f"{prefix}_stac_id", "")
            if not stac_id:
                continue
            by_id.setdefault(
                stac_id,
                {
                    "grd_stac_id": stac_id,
                    "role_examples": role,
                    "acquisition_time_utc": row.get(f"{prefix}_acquisition_time_utc", ""),
                    "acquisition_time_jst": row.get(f"{prefix}_acquisition_time_jst", ""),
                    "platform": row.get("platform", ""),
                    "orbit_direction": row.get("orbit_direction", ""),
                    "relative_orbit": row.get("relative_orbit", ""),
                },
            )
    return sorted(by_id.values(), key=lambda row: row["acquisition_time_utc"])


def search_slc_bbox_time(
    client: S1CDSEClient,
    *,
    bbox: tuple[float, float, float, float],
    start: datetime,
    end: datetime,
    limit: int,
) -> list[S1Scene]:
    min_lon, min_lat, max_lon, max_lat = bbox
    params = {
        "collections": "sentinel-1-slc",
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "datetime": f"{start.isoformat().replace('+00:00', 'Z')}/{end.isoformat().replace('+00:00', 'Z')}",
        "limit": limit,
    }
    data = client._stac_search(params)
    return client._features_to_scenes(data)


def scene_matches_grd(scene: S1Scene, grd: dict[str, str], tolerance_seconds: int) -> bool:
    grd_parts = parse_scene_parts(grd["grd_stac_id"])
    slc_parts = parse_scene_parts(scene.stac_id)
    grd_dt = parse_dt(grd["acquisition_time_utc"])
    if abs((scene.acquisition_time - grd_dt).total_seconds()) > tolerance_seconds:
        return False
    if grd_parts["mission"] and slc_parts["mission"] and grd_parts["mission"] != slc_parts["mission"]:
        return False
    if grd_parts["mode"] and slc_parts["mode"] and grd_parts["mode"] != slc_parts["mode"]:
        return False
    if grd_parts["pol_token"] and slc_parts["pol_token"] and grd_parts["pol_token"] != slc_parts["pol_token"]:
        return False
    if grd.get("platform") and scene.platform:
        if grd["platform"].lower().replace("-", "") != scene.platform.lower().replace("-", ""):
            return False
    if grd.get("orbit_direction") and scene.orbit_direction:
        if grd["orbit_direction"].lower() != scene.orbit_direction.lower():
            return False
    if grd.get("relative_orbit") and scene.relative_orbit is not None:
        if str(grd["relative_orbit"]) != str(scene.relative_orbit):
            return False
    return True


def find_slc_equivalent(
    client: S1CDSEClient,
    *,
    grd: dict[str, str],
    bbox: tuple[float, float, float, float],
    window_minutes: int,
    tolerance_seconds: int,
    limit: int,
) -> S1Scene | None:
    grd_dt = parse_dt(grd["acquisition_time_utc"])
    scenes = search_slc_bbox_time(
        client,
        bbox=bbox,
        start=grd_dt - timedelta(minutes=window_minutes),
        end=grd_dt + timedelta(minutes=window_minutes),
        limit=limit,
    )
    candidates = [scene for scene in scenes if scene_matches_grd(scene, grd, tolerance_seconds)]
    if not candidates:
        return None
    candidates.sort(key=lambda scene: abs((scene.acquisition_time - grd_dt).total_seconds()))
    return candidates[0]


def product_name(scene: S1Scene) -> str:
    return scene.product_identifier or scene.stac_id


def valid_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    return zipfile.is_zipfile(path)


def download_once(client: S1CDSEClient, name: str, download_dir: Path, dry_run: bool) -> tuple[bool, str]:
    normalized = normalize_product_name(name)
    expected = download_dir / f"{normalized}.zip"
    if valid_zip(expected):
        return True, str(expected)
    if expected.exists():
        expected.unlink()
    if dry_run:
        return False, ""
    saved = client.download_product(name, download_dir)
    if saved is not None and valid_zip(saved):
        return True, str(saved)
    return False, str(saved) if saved else ""


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--download-dir", type=Path, default=None)
    parser.add_argument("--window-minutes", type=int, default=3)
    parser.add_argument("--tolerance-seconds", type=int, default=180)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-scenes", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    download_dir = args.download_dir or (args.output_dir / "slc_downloads")
    result_csv = args.output_dir / "map7_s1_slc_equivalents_download.csv"

    bbox = map7_bbox()
    grds = unique_grd_scenes(args.input_csv)
    if args.max_scenes:
        grds = grds[: args.max_scenes]
    LOGGER.info("unique GRD scenes: %d", len(grds))
    LOGGER.info("SLC download dir: %s", download_dir)

    client = S1CDSEClient()
    rows: list[dict[str, Any]] = []
    for index, grd in enumerate(grds, start=1):
        LOGGER.info("[%d/%d] search SLC for %s", index, len(grds), grd["grd_stac_id"])
        slc = find_slc_equivalent(
            client,
            grd=grd,
            bbox=bbox,
            window_minutes=args.window_minutes,
            tolerance_seconds=args.tolerance_seconds,
            limit=args.limit,
        )
        if slc is None:
            row = {
                **grd,
                "slc_found": False,
                "slc_stac_id": "",
                "slc_product_identifier": "",
                "slc_acquisition_time_utc": "",
                "slc_product_type": "",
                "downloaded": False,
                "download_path": "",
            }
            rows.append(row)
            write_csv(rows, result_csv)
            LOGGER.warning("No SLC found for %s", grd["grd_stac_id"])
            continue

        name = product_name(slc)
        downloaded, download_path = download_once(client, name, download_dir, args.dry_run)
        rows.append(
            {
                **grd,
                "slc_found": True,
                "slc_stac_id": slc.stac_id,
                "slc_product_identifier": slc.product_identifier or "",
                "slc_acquisition_time_utc": slc.acquisition_time.isoformat(),
                "slc_product_type": slc.product_type or "",
                "downloaded": downloaded,
                "download_path": download_path,
            }
        )
        write_csv(rows, result_csv)
        if not args.dry_run:
            time.sleep(1)

    LOGGER.info("result csv: %s", result_csv)
    LOGGER.info("SLC found: %d/%d", sum(1 for row in rows if row["slc_found"]), len(rows))
    LOGGER.info("downloaded: %d/%d", sum(1 for row in rows if row["downloaded"]), len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
