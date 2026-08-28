#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import MAFF fude-polygon GeoJSON ZIPs into fude_polygons.

The source files are prefecture ZIPs such as 2021_34.zip. Each JSON member is
a GeoJSON FeatureCollection formatted with one feature per line, so this script
streams features instead of loading a whole prefecture file into memory.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import zipfile
from pathlib import Path
from typing import Iterable

from sqlalchemy import func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert

from app.db import models
from app.db.session import SessionLocal


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path(r"D:\sotsuron\fude-polygon"),
        help="Directory containing 2021_XX.zip files.",
    )
    parser.add_argument(
        "--pref-id",
        action="append",
        help="Prefecture id to import, e.g. 34. Can be specified multiple times. Defaults to all ZIPs.",
    )
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument(
        "--skip-existing-pref",
        action="store_true",
        help="Skip a prefecture when fude_polygons already has rows for that pref_id.",
    )
    return parser.parse_args()


def normalize_pref_id(pref_id: str) -> str:
    return f"{int(pref_id):02d}"


def iter_zip_paths(source_dir: Path, pref_ids: list[str] | None) -> list[Path]:
    if pref_ids:
        paths = [source_dir / f"2021_{normalize_pref_id(pref_id)}.zip" for pref_id in pref_ids]
    else:
        paths = sorted(source_dir.glob("2021_*.zip"))

    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing fude ZIP files: " + ", ".join(missing))
    return paths


def iter_features_from_member(handle: Iterable[bytes]):
    prefix = '{"type":"FeatureCollection","features":['
    suffix = "]}"

    for raw_line in handle:
        line = raw_line.decode("utf-8").strip()
        if not line:
            continue
        if line.startswith(prefix):
            line = line[len(prefix) :]
        if line.endswith(suffix):
            line = line[: -len(suffix)]
        if line.endswith(","):
            line = line[:-1]
        if not line:
            continue
        feature = json.loads(line)
        if feature.get("type") == "Feature":
            yield feature


def feature_to_record(feature: dict, pref_id: str) -> dict | None:
    props = feature.get("properties") or {}
    geometry = feature.get("geometry")
    polygon_uuid = props.get("polygon_uuid")
    if not polygon_uuid or not geometry:
        return None

    return {
        "polygon_uuid": polygon_uuid,
        "land_type": props.get("land_type"),
        "issue_year": props.get("issue_year"),
        "edit_year": props.get("edit_year"),
        "history": props.get("history"),
        "last_polygon_uuid": props.get("last_polygon_uuid"),
        "prev_last_polygon_uuid": props.get("prev_last_polygon_uuid"),
        "local_government_cd": props.get("local_government_cd"),
        "point_lng": props.get("point_lng"),
        "point_lat": props.get("point_lat"),
        "old_polygon_id": props.get("old_polygon_id"),
        "pref_id": pref_id,
        "geometry": geometry,
    }


def flush_batch(session, stmt, batch: list[dict]) -> int:
    if not batch:
        return 0
    result = session.execute(stmt, batch)
    session.commit()
    inserted = result.rowcount or 0
    batch.clear()
    return inserted


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()

    if not args.source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {args.source_dir}")

    zip_paths = iter_zip_paths(args.source_dir, args.pref_id)
    stmt = mysql_insert(models.FudePolygon).prefix_with("IGNORE")

    session = SessionLocal()
    total_seen = 0
    total_inserted = 0
    started = time.time()

    try:
        for zip_index, zip_path in enumerate(zip_paths, start=1):
            pref_id = zip_path.stem.split("_")[-1]
            if args.skip_existing_pref:
                existing = session.scalar(
                    select(func.count()).select_from(models.FudePolygon).where(models.FudePolygon.pref_id == pref_id)
                )
                if existing:
                    logger.info("[%s/%s] %s already has %s rows; skipping.", zip_index, len(zip_paths), pref_id, existing)
                    continue

            logger.info("[%s/%s] Importing %s", zip_index, len(zip_paths), zip_path.name)
            pref_seen = 0
            pref_inserted = 0
            batch: list[dict] = []

            with zipfile.ZipFile(zip_path, "r") as archive:
                json_members = [name for name in archive.namelist() if name.lower().endswith(".json")]
                for member_index, member in enumerate(json_members, start=1):
                    with archive.open(member) as handle:
                        for feature in iter_features_from_member(handle):
                            pref_seen += 1
                            record = feature_to_record(feature, pref_id)
                            if record is None:
                                continue
                            batch.append(record)
                            if len(batch) >= args.batch_size:
                                pref_inserted += flush_batch(session, stmt, batch)

                    if member_index % 25 == 0:
                        logger.info(
                            "  %s: %s/%s members, seen=%s inserted=%s",
                            zip_path.name,
                            member_index,
                            len(json_members),
                            pref_seen,
                            pref_inserted,
                        )

            pref_inserted += flush_batch(session, stmt, batch)
            total_seen += pref_seen
            total_inserted += pref_inserted
            logger.info("Finished %s: seen=%s inserted=%s", zip_path.name, pref_seen, pref_inserted)

    finally:
        session.close()

    logger.info(
        "Done. seen=%s inserted=%s elapsed=%.1fs",
        total_seen,
        total_inserted,
        time.time() - started,
    )


if __name__ == "__main__":
    main()
