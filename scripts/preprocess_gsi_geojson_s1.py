#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Preprocess downloaded Sentinel-1 GRD scenes for GSI GeoJSON masks.

The processing chain follows scripts/archive/preprocess_s1_cog.py:
Read -> Apply-Orbit-File -> Subset -> ThermalNoiseRemoval -> Calibration
-> Terrain-Correction -> LinearToFromdB -> Write.

SNAP is executed through gpt.exe because esa_snappy is not available in the
default Python environment.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_CSV = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "search_download_results.csv"
DEFAULT_DOWNLOAD_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "downloads"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "processed_geojson"
DEFAULT_GPT_EXE = Path(r"C:\Program Files\esa-snap\bin\gpt.exe")

LOGGER = logging.getLogger("gsi_s1_preprocess")


GRAPH_TEMPLATE = """<graph id="S1_GRD_GSI_GEOJSON_Preprocess">
  <version>1.0</version>
  <node id="Read">
    <operator>Read</operator>
    <sources/>
    <parameters>
      <file>${{sourceFile}}</file>
    </parameters>
  </node>
  <node id="Apply-Orbit-File">
    <operator>Apply-Orbit-File</operator>
    <sources>
      <sourceProduct refid="Read"/>
    </sources>
    <parameters>
      <orbitType>Sentinel Precise (Auto Download)</orbitType>
      <polyDegree>3</polyDegree>
      <continueOnFail>true</continueOnFail>
    </parameters>
  </node>
  <node id="Subset">
    <operator>Subset</operator>
    <sources>
      <sourceProduct refid="Apply-Orbit-File"/>
    </sources>
    <parameters>
      <geoRegion>${{aoiWkt}}</geoRegion>
      <copyMetadata>true</copyMetadata>
    </parameters>
  </node>
  <node id="ThermalNoiseRemoval">
    <operator>ThermalNoiseRemoval</operator>
    <sources>
      <sourceProduct refid="Subset"/>
    </sources>
    <parameters>
      <selectedPolarisations>{pol}</selectedPolarisations>
      <removeThermalNoise>true</removeThermalNoise>
    </parameters>
  </node>
  <node id="Calibration">
    <operator>Calibration</operator>
    <sources>
      <sourceProduct refid="ThermalNoiseRemoval"/>
    </sources>
    <parameters>
      <sourceBands>Intensity_{pol}</sourceBands>
      <selectedPolarisations>{pol}</selectedPolarisations>
      <outputSigmaBand>true</outputSigmaBand>
      <outputBetaBand>false</outputBetaBand>
      <outputGammaBand>false</outputGammaBand>
      <outputImageScaleInDb>false</outputImageScaleInDb>
    </parameters>
  </node>
  <node id="Terrain-Correction">
    <operator>Terrain-Correction</operator>
    <sources>
      <sourceProduct refid="Calibration"/>
    </sources>
    <parameters>
      <sourceBands>Sigma0_{pol}</sourceBands>
      <demName>Copernicus 30m Global DEM</demName>
      <demResamplingMethod>BILINEAR_INTERPOLATION</demResamplingMethod>
      <imgResamplingMethod>BILINEAR_INTERPOLATION</imgResamplingMethod>
      <pixelSpacingInMeter>{pixel_spacing}</pixelSpacingInMeter>
      <mapProjection>WGS84(DD)</mapProjection>
      <alignToStandardGrid>true</alignToStandardGrid>
      <standardGridOriginX>0.0</standardGridOriginX>
      <standardGridOriginY>0.0</standardGridOriginY>
      <nodataValueAtSea>false</nodataValueAtSea>
      <saveSelectedSourceBand>true</saveSelectedSourceBand>
    </parameters>
  </node>
  <node id="LinearToFromdB">
    <operator>LinearToFromdB</operator>
    <sources>
      <sourceProduct refid="Terrain-Correction"/>
    </sources>
    <parameters>
      <sourceBands>Sigma0_{pol}</sourceBands>
    </parameters>
  </node>
  <node id="Write">
    <operator>Write</operator>
    <sources>
      <sourceProduct refid="LinearToFromdB"/>
    </sources>
    <parameters>
      <file>${{targetFile}}</file>
      <formatName>GeoTIFF-BigTIFF</formatName>
    </parameters>
  </node>
</graph>
"""


def normalize_scene_id(stac_id: str) -> str:
    return stac_id.removesuffix("_COG").removesuffix(".SAFE")


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip()).strip("_")


def coord_pair(coord: list[Any]) -> str:
    if len(coord) < 2:
        raise ValueError(f"Invalid coordinate: {coord}")
    return f"{float(coord[0])} {float(coord[1])}"


def ring_wkt(ring: list[list[Any]]) -> str:
    if not ring:
        raise ValueError("Polygon ring is empty")
    points = [coord_pair(coord) for coord in ring]
    if points[0] != points[-1]:
        points.append(points[0])
    return f"({', '.join(points)})"


def polygon_wkt(coordinates: list[Any]) -> str:
    return f"POLYGON ({', '.join(ring_wkt(ring) for ring in coordinates)})"


def multipolygon_wkt(coordinates: list[Any]) -> str:
    polygons = []
    for polygon in coordinates:
        polygons.append(f"({', '.join(ring_wkt(ring) for ring in polygon)})")
    return f"MULTIPOLYGON ({', '.join(polygons)})"


def geometry_wkt(geometry: dict[str, Any]) -> str:
    geom_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geom_type == "Polygon":
        return polygon_wkt(coordinates)
    if geom_type == "MultiPolygon":
        return multipolygon_wkt(coordinates)
    if geom_type == "GeometryCollection":
        geometries = geometry.get("geometries") or []
        polygons = []
        for geom in geometries:
            wkt = geometry_wkt(geom)
            if wkt.startswith("POLYGON "):
                polygons.append(wkt.removeprefix("POLYGON "))
            elif wkt.startswith("MULTIPOLYGON "):
                polygons.extend(wkt.removeprefix("MULTIPOLYGON ").strip()[1:-1].split("), ("))
        if not polygons:
            raise ValueError("GeometryCollection has no polygon geometry")
        return f"MULTIPOLYGON ({', '.join(polygons)})"
    raise ValueError(f"Unsupported GeoJSON geometry type: {geom_type}")


def geojson_wkt(path: Path) -> str:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("type") == "FeatureCollection":
        wkts = [geometry_wkt(feature["geometry"]) for feature in data.get("features", []) if feature.get("geometry")]
        if not wkts:
            raise ValueError(f"No geometry found in {path}")
        if len(wkts) == 1:
            return wkts[0]
        polygons = []
        for wkt in wkts:
            if wkt.startswith("POLYGON "):
                polygons.append(wkt.removeprefix("POLYGON "))
            elif wkt.startswith("MULTIPOLYGON "):
                polygons.extend(wkt.removeprefix("MULTIPOLYGON ").strip()[1:-1].split("), ("))
            else:
                raise ValueError(f"Unsupported WKT in {path}: {wkt[:32]}")
        return f"MULTIPOLYGON ({', '.join(polygons)})"
    if data.get("type") == "Feature":
        return geometry_wkt(data["geometry"])
    return geometry_wkt(data)


def expected_tif(target_stem: Path) -> Path:
    return target_stem.with_suffix(".tif")


def run_gpt(
    *,
    gpt_exe: Path,
    graph_xml: Path,
    zip_path: Path,
    out_tif: Path,
    aoi_wkt: str,
    memory: str,
    tile_cache: str,
    threads: int,
) -> bool:
    out_tif.parent.mkdir(parents=True, exist_ok=True)
    target_stem = out_tif.with_suffix("")
    cmd = [
        str(gpt_exe),
        str(graph_xml),
        f"-PsourceFile={zip_path}",
        f"-PtargetFile={target_stem}",
        f"-PaoiWkt={aoi_wkt}",
        f"-J-Xmx{memory}",
        "-c",
        tile_cache,
        "-e",
        "-q",
        str(threads),
    ]
    LOGGER.info("GPT: %s -> %s", zip_path.name, out_tif)
    result = subprocess.run(cmd, cwd=str(ROOT_DIR))
    return result.returncode == 0 and out_tif.exists() and out_tif.stat().st_size > 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--download-dir", type=Path, default=DEFAULT_DOWNLOAD_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--gpt-exe", type=Path, default=DEFAULT_GPT_EXE)
    parser.add_argument("--pol", choices=["VV", "VH"], default="VV")
    parser.add_argument("--pixel-spacing", type=float, default=10.0)
    parser.add_argument("--memory", default="16G")
    parser.add_argument("--tile-cache", default="8G")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0, help="Process only first N tasks.")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not args.gpt_exe.exists():
        LOGGER.error("GPT not found: %s", args.gpt_exe)
        return 1
    if not args.input_csv.exists():
        LOGGER.error("CSV not found: %s", args.input_csv)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    result_csv = args.output_dir / f"preprocess_results_{args.pol}.csv"
    log_path = args.output_dir / f"preprocess_{args.pol}.log"
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    LOGGER.addHandler(file_handler)

    with tempfile.NamedTemporaryFile("w", suffix=".xml", delete=False, encoding="utf-8") as graph_file:
        graph_path = Path(graph_file.name)
        graph_file.write(
            GRAPH_TEMPLATE.format(
                pol=escape(args.pol),
                pixel_spacing=escape(str(args.pixel_spacing)),
            )
        )

    rows = list(csv.DictReader(args.input_csv.open(encoding="utf-8-sig")))
    aoi_cache: dict[Path, str] = {}
    tasks: list[dict[str, str]] = []
    for row in rows:
        if row.get("downloaded") != "True":
            continue
        scene_stem = normalize_scene_id(row["stac_id"])
        zip_path = args.download_dir / f"{scene_stem}.zip"
        if not zip_path.exists():
            LOGGER.warning("Missing ZIP: %s", zip_path)
            continue
        tasks.append(row)

    if args.limit > 0:
        tasks = tasks[: args.limit]

    LOGGER.info("Tasks: %d", len(tasks))
    results: list[dict[str, str]] = []

    try:
        for index, row in enumerate(tasks, 1):
            area = safe_name(row["area"])
            period = safe_name(row["period"])
            scene_stem = normalize_scene_id(row["stac_id"])
            zip_path = args.download_dir / f"{scene_stem}.zip"
            out_tif = args.output_dir / area / f"{period}__{scene_stem}__{args.pol}_proc.tif"

            if out_tif.exists() and out_tif.stat().st_size > 0 and not args.force:
                status = "skipped"
                LOGGER.info("[%d/%d] SKIP: %s", index, len(tasks), out_tif.name)
            else:
                LOGGER.info("[%d/%d] START: %s %s", index, len(tasks), row["area"], row["period"])
                geojson_path = Path(row["geojson"])
                if geojson_path not in aoi_cache:
                    aoi_cache[geojson_path] = geojson_wkt(geojson_path)
                ok = run_gpt(
                    gpt_exe=args.gpt_exe,
                    graph_xml=graph_path,
                    zip_path=zip_path,
                    out_tif=out_tif,
                    aoi_wkt=aoi_cache[geojson_path],
                    memory=args.memory,
                    tile_cache=args.tile_cache,
                    threads=args.threads,
                )
                status = "success" if ok else "failed"

            results.append(
                {
                    "area": row["area"],
                    "period": row["period"],
                    "matched_event_stac_id": row.get("matched_event_stac_id", ""),
                    "stac_id": row["stac_id"],
                    "pol": args.pol,
                    "status": status,
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
    failed = sum(1 for row in results if row["status"] == "failed")
    LOGGER.info("Done: success=%d skipped=%d failed=%d total=%d", success, skipped, failed, len(results))
    LOGGER.info("Results CSV: %s", result_csv)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
