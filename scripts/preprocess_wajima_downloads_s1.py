#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Preprocess Wajima Sentinel-1 downloads with SNAP GPT.

Output is written under D:\\shuron\\downloads\\processed by default.
The processing graph follows the existing GSI/Map7 scripts:
Read -> Apply-Orbit-File -> Subset -> ThermalNoiseRemoval -> Calibration
-> Terrain-Correction -> LinearToFromdB -> GeoTIFF-BigTIFF.
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import tempfile
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

import geopandas as gpd


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.preprocess_gsi_geojson_s1 import GRAPH_TEMPLATE, run_gpt  # noqa: E402


DEFAULT_DOWNLOAD_DIR = Path(r"D:\shuron\downloads")
DEFAULT_PAIR_CSV = DEFAULT_DOWNLOAD_DIR / "wajima_s1_rain_pairs.csv"
DEFAULT_TRUTH_PATH = Path(r"D:\shuron\GT-data\sinsuiiki\shinsui.shp")
DEFAULT_OUTPUT_DIR = DEFAULT_DOWNLOAD_DIR / "processed"
DEFAULT_GPT_EXE = Path(r"C:\Program Files\esa-snap\bin\gpt.exe")

LOGGER = logging.getLogger("wajima_s1_preprocess")


def normalize_scene_id(value: str) -> str:
    return value.removesuffix("_COG").removesuffix(".SAFE").removesuffix(".zip")


def scene_polarizations(scene_stem: str) -> set[str]:
    parts = scene_stem.split("_")
    if len(parts) >= 4:
        pol_token = parts[3]
        if pol_token.endswith("DV"):
            return {"VV", "VH"}
        if pol_token.endswith("SV"):
            return {"VV"}
        if pol_token.endswith("SH"):
            return {"HH"}
        if pol_token.endswith("DH"):
            return {"HH", "HV"}
    return {"VV"}


def is_iw_grd(scene_stem: str) -> bool:
    parts = scene_stem.split("_")
    return len(parts) >= 3 and parts[1] == "IW" and parts[2].startswith("GRD")


def is_valid_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    return zipfile.is_zipfile(path)


def aoi_bbox_wkt(truth_path: Path, buffer_deg: float) -> str:
    truth = gpd.read_file(truth_path)
    if truth.crs is None:
        raise RuntimeError(f"Truth data has no CRS: {truth_path}")
    truth = truth[truth.geometry.notna() & ~truth.geometry.is_empty].to_crs("EPSG:4326")
    min_lon, min_lat, max_lon, max_lat = truth.total_bounds
    min_lon -= buffer_deg
    min_lat -= buffer_deg
    max_lon += buffer_deg
    max_lat += buffer_deg
    return (
        "POLYGON (("
        f"{min_lon} {min_lat}, "
        f"{min_lon} {max_lat}, "
        f"{max_lon} {max_lat}, "
        f"{max_lon} {min_lat}, "
        f"{min_lon} {min_lat}"
        "))"
    )


def collect_zip_paths(pair_csv: Path, download_dir: Path) -> list[Path]:
    paths: dict[str, Path] = {}
    if pair_csv.exists():
        rows = list(csv.DictReader(pair_csv.open(encoding="utf-8-sig")))
        for row in rows:
            if row.get("status") != "matched":
                continue
            for key in ("after_path", "before_path"):
                raw = row.get(key, "")
                if not raw:
                    continue
                path = Path(raw)
                if path.exists():
                    paths[normalize_scene_id(path.name)] = path

    for path in sorted(download_dir.glob("S1*.zip")):
        paths.setdefault(normalize_scene_id(path.name), path)

    return sorted(paths.values(), key=lambda p: p.name)


def build_tasks(zip_paths: list[Path], output_dir: Path, pols: list[str]) -> list[dict[str, str]]:
    tasks: list[dict[str, str]] = []
    for zip_path in zip_paths:
        scene_stem = normalize_scene_id(zip_path.name)
        available = scene_polarizations(scene_stem)
        if not is_iw_grd(scene_stem):
            tasks.append(
                {
                    "scene_stem": scene_stem,
                    "zip_path": str(zip_path),
                    "pol": "",
                    "status": "skipped_non_iw_grd",
                    "output_path": "",
                }
            )
            continue
        if not is_valid_zip(zip_path):
            tasks.append(
                {
                    "scene_stem": scene_stem,
                    "zip_path": str(zip_path),
                    "pol": "",
                    "status": "skipped_invalid_zip",
                    "output_path": "",
                }
            )
            continue
        for pol in pols:
            if pol not in available:
                tasks.append(
                    {
                        "scene_stem": scene_stem,
                        "zip_path": str(zip_path),
                        "pol": pol,
                        "status": "skipped_unavailable_pol",
                        "output_path": "",
                    }
                )
                continue
            out_tif = output_dir / pol / f"{scene_stem}__{pol}_proc.tif"
            tasks.append(
                {
                    "scene_stem": scene_stem,
                    "zip_path": str(zip_path),
                    "pol": pol,
                    "status": "pending",
                    "output_path": str(out_tif),
                }
            )
    return tasks


def write_results(results: list[dict[str, str]], result_csv: Path) -> None:
    result_csv.parent.mkdir(parents=True, exist_ok=True)
    if not results:
        result_csv.write_text("", encoding="utf-8-sig")
        return
    with result_csv.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-dir", type=Path, default=DEFAULT_DOWNLOAD_DIR)
    parser.add_argument("--pair-csv", type=Path, default=DEFAULT_PAIR_CSV)
    parser.add_argument("--truth-path", type=Path, default=DEFAULT_TRUTH_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--gpt-exe", type=Path, default=DEFAULT_GPT_EXE)
    parser.add_argument("--pol", choices=["VV", "VH", "all"], default="all")
    parser.add_argument("--aoi-buffer-deg", type=float, default=0.08)
    parser.add_argument("--pixel-spacing", type=float, default=10.0)
    parser.add_argument("--memory", default="16G")
    parser.add_argument("--tile-cache", default="8G")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.output_dir / "preprocess.log"
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    LOGGER.addHandler(file_handler)

    if not args.gpt_exe.exists() and not args.plan_only:
        LOGGER.error("GPT not found: %s", args.gpt_exe)
        return 1
    if not args.truth_path.exists():
        LOGGER.error("Truth shapefile not found: %s", args.truth_path)
        return 1

    pols = ["VV", "VH"] if args.pol == "all" else [args.pol]
    zip_paths = collect_zip_paths(args.pair_csv, args.download_dir)
    tasks = build_tasks(zip_paths, args.output_dir, pols)
    pending_tasks = [task for task in tasks if task["status"] == "pending"]
    if args.limit > 0:
        process_ids = {id(task) for task in pending_tasks[: args.limit]}
    else:
        process_ids = {id(task) for task in pending_tasks}

    result_csv = args.output_dir / "preprocess_results.csv"
    results: list[dict[str, str]] = []

    aoi_wkt = aoi_bbox_wkt(args.truth_path, args.aoi_buffer_deg)
    LOGGER.info("ZIP refs: %d", len(zip_paths))
    LOGGER.info("Tasks total: %d, processable: %d", len(tasks), len(pending_tasks))
    LOGGER.info("Output dir: %s", args.output_dir)

    if args.plan_only:
        for task in tasks:
            row = dict(task)
            if row["status"] == "pending":
                row["status"] = "planned"
            results.append(row)
        write_results(results, result_csv)
        LOGGER.info("Plan written: %s", result_csv)
        return 0

    graph_paths: dict[str, Path] = {}
    try:
        for pol in pols:
            with tempfile.NamedTemporaryFile("w", suffix=f"_{pol}.xml", delete=False, encoding="utf-8") as graph_file:
                graph_path = Path(graph_file.name)
                graph_file.write(
                    GRAPH_TEMPLATE.format(
                        pol=escape(pol),
                        pixel_spacing=escape(str(args.pixel_spacing)),
                    )
                )
                graph_paths[pol] = graph_path

        for index, task in enumerate(tasks, 1):
            row = dict(task)
            if task["status"] != "pending":
                results.append(row)
                write_results(results, result_csv)
                continue
            if id(task) not in process_ids:
                row["status"] = "not_run_limit"
                results.append(row)
                write_results(results, result_csv)
                continue

            zip_path = Path(task["zip_path"])
            out_tif = Path(task["output_path"])
            if out_tif.exists() and out_tif.stat().st_size > 0 and not args.force:
                row["status"] = "skipped_existing"
                LOGGER.info("[%d/%d] SKIP existing: %s", index, len(tasks), out_tif.name)
            else:
                LOGGER.info("[%d/%d] START %s %s", index, len(tasks), task["pol"], zip_path.name)
                ok = run_gpt(
                    gpt_exe=args.gpt_exe,
                    graph_xml=graph_paths[task["pol"]],
                    zip_path=zip_path,
                    out_tif=out_tif,
                    aoi_wkt=aoi_wkt,
                    memory=args.memory,
                    tile_cache=args.tile_cache,
                    threads=args.threads,
                )
                row["status"] = "success" if ok else "failed"
            results.append(row)
            write_results(results, result_csv)
    finally:
        for graph_path in graph_paths.values():
            graph_path.unlink(missing_ok=True)

    success = sum(1 for row in results if row["status"] == "success")
    skipped = sum(1 for row in results if row["status"].startswith("skipped"))
    failed = sum(1 for row in results if row["status"] == "failed")
    LOGGER.info("Done: success=%d skipped=%d failed=%d total=%d", success, skipped, failed, len(results))
    LOGGER.info("Results CSV: %s", result_csv)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
