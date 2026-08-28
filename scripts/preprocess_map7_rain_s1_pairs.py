#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Preprocess downloaded map7 rain/no-rain Sentinel-1 pairs with SNAP GPT."""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import tempfile
from pathlib import Path
from xml.sax.saxutils import escape


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.preprocess_gsi_geojson_s1 import GRAPH_TEMPLATE, geojson_wkt, run_gpt  # noqa: E402


DEFAULT_PAIR_CSV = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "map7_s1_rain_no_rain_pairs_download.csv"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed"
DEFAULT_GEOJSON = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "geojson" / "map (7).geojson"
DEFAULT_GPT_EXE = Path(r"C:\Program Files\esa-snap\bin\gpt.exe")

LOGGER = logging.getLogger("map7_s1_preprocess")


def normalize_scene_id(stac_id: str) -> str:
    return stac_id.removesuffix("_COG").removesuffix(".SAFE")


def collect_tasks(rows: list[dict[str, str]], output_dir: Path, pol: str) -> list[dict[str, str]]:
    tasks_by_scene: dict[str, dict[str, str]] = {}
    for row in rows:
        for role in ("target", "pair"):
            stac_id = row[f"{role}_stac_id"]
            zip_path = row[f"{role}_download_path"]
            if not stac_id or not zip_path:
                continue
            scene_stem = normalize_scene_id(stac_id)
            out_tif = output_dir / role / f"{scene_stem}__{pol}_proc.tif"
            tasks_by_scene[scene_stem] = {
                "role": role,
                "scene_stem": scene_stem,
                "stac_id": stac_id,
                "zip_path": zip_path,
                "out_tif": str(out_tif),
            }
    return sorted(tasks_by_scene.values(), key=lambda task: (task["role"], task["scene_stem"]))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-csv", type=Path, default=DEFAULT_PAIR_CSV)
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--gpt-exe", type=Path, default=DEFAULT_GPT_EXE)
    parser.add_argument("--pol", choices=["VV", "VH"], default="VV")
    parser.add_argument("--pixel-spacing", type=float, default=10.0)
    parser.add_argument("--memory", default="16G")
    parser.add_argument("--tile-cache", default="8G")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if not args.gpt_exe.exists():
        LOGGER.error("GPT not found: %s", args.gpt_exe)
        return 1
    if not args.pair_csv.exists():
        LOGGER.error("Pair CSV not found: %s", args.pair_csv)
        return 1
    if not args.geojson.exists():
        LOGGER.error("GeoJSON not found: %s", args.geojson)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    result_csv = args.output_dir / f"preprocess_results_{args.pol}.csv"
    log_path = args.output_dir / f"preprocess_{args.pol}.log"
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    LOGGER.addHandler(file_handler)

    rows = list(csv.DictReader(args.pair_csv.open(encoding="utf-8-sig")))
    tasks = collect_tasks(rows, args.output_dir, args.pol)
    if args.limit > 0:
        tasks = tasks[: args.limit]
    LOGGER.info("Tasks: %d", len(tasks))

    aoi_wkt = geojson_wkt(args.geojson)
    with tempfile.NamedTemporaryFile("w", suffix=".xml", delete=False, encoding="utf-8") as graph_file:
        graph_path = Path(graph_file.name)
        graph_file.write(
            GRAPH_TEMPLATE.format(
                pol=escape(args.pol),
                pixel_spacing=escape(str(args.pixel_spacing)),
            )
        )

    results: list[dict[str, str]] = []
    try:
        for index, task in enumerate(tasks, 1):
            zip_path = Path(task["zip_path"])
            out_tif = Path(task["out_tif"])
            if not zip_path.exists():
                status = "missing_zip"
                LOGGER.error("[%d/%d] Missing ZIP: %s", index, len(tasks), zip_path)
            elif out_tif.exists() and out_tif.stat().st_size > 0 and not args.force:
                status = "skipped"
                LOGGER.info("[%d/%d] SKIP: %s", index, len(tasks), out_tif.name)
            else:
                LOGGER.info("[%d/%d] START: %s %s", index, len(tasks), task["role"], zip_path.name)
                ok = run_gpt(
                    gpt_exe=args.gpt_exe,
                    graph_xml=graph_path,
                    zip_path=zip_path,
                    out_tif=out_tif,
                    aoi_wkt=aoi_wkt,
                    memory=args.memory,
                    tile_cache=args.tile_cache,
                    threads=args.threads,
                )
                status = "success" if ok else "failed"

            results.append(
                {
                    "role": task["role"],
                    "stac_id": task["stac_id"],
                    "pol": args.pol,
                    "status": status,
                    "zip_path": task["zip_path"],
                    "output_path": str(out_tif if out_tif.exists() else ""),
                }
            )
            with result_csv.open("w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
                writer.writeheader()
                writer.writerows(results)
    finally:
        graph_path.unlink(missing_ok=True)

    success = sum(1 for row in results if row["status"] == "success")
    skipped = sum(1 for row in results if row["status"] == "skipped")
    failed = sum(1 for row in results if row["status"] in {"failed", "missing_zip"})
    LOGGER.info("Done: success=%d skipped=%d failed=%d total=%d", success, skipped, failed, len(results))
    LOGGER.info("Results CSV: %s", result_csv)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
