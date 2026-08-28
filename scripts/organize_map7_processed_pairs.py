#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Organize processed map7 S1 pair GeoTIFFs by rain date."""

from __future__ import annotations

import argparse
import csv
import os
import shutil
from collections import defaultdict
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PAIR_CSV = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "map7_s1_rain_no_rain_pairs_download.csv"
DEFAULT_PREPROCESS_CSV = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed" / "preprocess_results_VV.csv"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed_by_date"


def normalize_scene_id(stac_id: str) -> str:
    return stac_id.removesuffix("_COG").removesuffix(".SAFE")


def link_or_copy(src: Path, dst: Path) -> str:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return "exists"
    try:
        os.link(src, dst)
        return "hardlink"
    except OSError:
        shutil.copy2(src, dst)
        return "copy"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-csv", type=Path, default=DEFAULT_PAIR_CSV)
    parser.add_argument("--preprocess-csv", type=Path, default=DEFAULT_PREPROCESS_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    pair_rows = list(csv.DictReader(args.pair_csv.open(encoding="utf-8-sig")))
    proc_rows = list(csv.DictReader(args.preprocess_csv.open(encoding="utf-8-sig")))
    proc_by_scene = {
        normalize_scene_id(row["stac_id"]): Path(row["output_path"])
        for row in proc_rows
        if row.get("status") in {"success", "skipped"} and row.get("output_path")
    }

    date_counts: defaultdict[str, int] = defaultdict(int)
    manifest_rows: list[dict[str, str]] = []

    for row in pair_rows:
        date = row["rain_day_jst"]
        date_counts[date] += 1
        pair_no = f"pair_{date_counts[date]:02d}"
        pair_dir = args.output_dir / date / pair_no

        for role in ("target", "pair"):
            stac_id = row[f"{role}_stac_id"]
            scene = normalize_scene_id(stac_id)
            src = proc_by_scene.get(scene)
            if src is None or not src.exists():
                status = "missing_processed_tif"
                dst = pair_dir / f"{role}.tif"
            else:
                dst = pair_dir / f"{role}.tif"
                status = link_or_copy(src, dst)

            manifest_rows.append(
                {
                    "rain_day_jst": date,
                    "pair_no": pair_no,
                    "role": role,
                    "stac_id": stac_id,
                    "source_path": str(src or ""),
                    "organized_path": str(dst),
                    "status": status,
                    "rain_first_ts_jst": row.get("rain_first_ts_jst", ""),
                    "rain_last_ts_jst": row.get("rain_last_ts_jst", ""),
                    "pair_no_heavy_rain_day_jst": row.get("pair_no_heavy_rain_day_jst", ""),
                }
            )

    manifest_path = args.output_dir / "manifest.csv"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(manifest_rows[0].keys()))
        writer.writeheader()
        writer.writerows(manifest_rows)

    print(f"date_dirs={len(date_counts)}")
    print(f"pairs={len(pair_rows)}")
    print(f"linked_files={len(manifest_rows)}")
    print(f"manifest={manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
