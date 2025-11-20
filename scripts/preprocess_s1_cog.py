#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
preprocess_s1_cog.py

Pipeline:
  1) Read Product
  2) Apply-Orbit-File
  3) Subset (Grid ID範囲)
  4) ThermalNoiseRemoval
  5) Calibration (Sigma0, Linear, Single Pol)
  6) Terrain-Correction
  7) Write (GeoTIFF)
"""

import argparse
import logging
import sys
import re
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import esa_snappy
    from esa_snappy import GPF, ProductIO, HashMap, jpy
except ImportError:
    print("Error: 'esa_snappy' module not found.")
    sys.exit(1)

JInt = jpy.get_type('java.lang.Integer')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("s1_proc")

GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()


def decode_grid_id(grid_id: str) -> Tuple[float, float]:
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, grid_id)
    if not m:
        raise ValueError(f"Invalid Grid ID format: {grid_id}")
    ns, lat_str, ew, lon_str = m.groups()
    lat = float(lat_str) / 100.0
    if ns == 'S': lat = -lat
    lon = float(lon_str) / 100.0
    if ew == 'W': lon = -lon
    return lat, lon


def create_wkt_polygon(center_lat: float, center_lon: float, size: float = 0.1) -> str:
    half = size / 2
    min_lon, max_lon = center_lon - half, center_lon + half
    min_lat, max_lat = center_lat - half, center_lat + half
    return f"POLYGON (({min_lon} {min_lat}, {min_lon} {max_lat}, {max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"


def process_one(
    in_file: Path, 
    out_file: Path,
    aoi_wkt: str,
    pixel_spacing: float = 10.0,
    pol: str = "VH"
):
    if out_file.exists():
        logger.info(f"[SKIP] Already exists: {out_file.name}")
        return

    logger.info(f"[READ] {in_file.name}")
    source = None
    current_product = None
    
    try:
        source = ProductIO.readProduct(str(in_file))
        if source is None:
            logger.error(f"Failed to read product: {in_file}")
            return
        current_product = source

        logger.info("  - Apply Orbit File...")
        params_orb = HashMap()
        params_orb.put("orbitType", "Sentinel Precise (Auto Download)")
        params_orb.put("polyDegree", JInt(3))
        params_orb.put("continueOnFail", True)
        current_product = GPF.createProduct("Apply-Orbit-File", params_orb, current_product)

        logger.info("  - Subset (Clipping by Grid ID)...")
        params_sub = HashMap()
        params_sub.put("geoRegion", aoi_wkt)
        params_sub.put("copyMetadata", True)
        try:
            current_product = GPF.createProduct("Subset", params_sub, current_product)
        except Exception as e:
            logger.warning(f"Subset failed: {e}")
            return

        logger.info("  - Thermal Noise Removal...")
        params_tnr = HashMap()
        params_tnr.put("removeThermalNoise", True)
        current_product = GPF.createProduct("ThermalNoiseRemoval", params_tnr, current_product)

        logger.info(f"  - Calibration (Sigma0, {pol})...")
        params_cal = HashMap()
        params_cal.put("outputSigmaBand", True)
        params_cal.put("sourceBands", f"Intensity_{pol}") 
        params_cal.put("selectedPolarisations", pol)
        params_cal.put("outputImageScaleInDb", False)
        current_product = GPF.createProduct("Calibration", params_cal, current_product)

        logger.info(f"  - Terrain Correction (Spacing: {pixel_spacing}m)...")
        params_tc = HashMap()
        params_tc.put("demName", "Copernicus 30m Global DEM")
        params_tc.put("demResamplingMethod", "BILINEAR_INTERPOLATION")
        params_tc.put("imgResamplingMethod", "BILINEAR_INTERPOLATION")
        params_tc.put("pixelSpacingInMeter", float(pixel_spacing))
        params_tc.put("mapProjection", "WGS84(DD)")
        params_tc.put("saveSelectedSourceBand", True)
        params_tc.put("nodataValueAtSea", False) 
        current_product = GPF.createProduct("Terrain-Correction", params_tc, current_product)

        logger.info(f"[WRITE] {out_file.name}")
        out_file.parent.mkdir(parents=True, exist_ok=True)
        write_path_stem = str(out_file.with_suffix(""))
        ProductIO.writeProduct(current_product, write_path_stem, "GeoTIFF-BigTIFF")
        logger.info(f"[DONE] Saved to {out_file}")

    except Exception as e:
        logger.error(f"Processing failed for {in_file.name}: {e}", exc_info=True)
    finally:
        if 'source' in locals() and source is not None: source.dispose()
        if 'current_product' in locals() and current_product is not None: current_product.dispose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--grid-id", type=str, required=True)
    parser.add_argument("--target-file", type=str, required=True)
    parser.add_argument("--in-root", type=str, default=r"D:\sotsuron\s1_safe")
    parser.add_argument("--out-root", type=str, default=r"D:\sotsuron\s1_samples")
    parser.add_argument("--pixel-spacing", type=float, default=10.0)
    parser.add_argument("--grid-size", type=float, default=0.1)
    parser.add_argument("--pol", type=str, default="VH", choices=["VV", "VH"])

    args = parser.parse_args()
    in_path = Path(args.in_root)
    out_path = Path(args.out_root)
    target_zip = in_path / args.target_file
    
    if not target_zip.exists():
        logger.error(f"Target file not found: {target_zip}")
        sys.exit(1)

    try:
        lat, lon = decode_grid_id(args.grid_id)
        logger.info(f"Grid ID: {args.grid_id} -> Center Lat: {lat}, Lon: {lon}")
    except ValueError as e:
        logger.error(e)
        sys.exit(1)

    aoi_wkt = create_wkt_polygon(lat, lon, args.grid_size)
    stem = target_zip.name.replace(".zip", "").replace(".SAFE", "")
    out_file = out_path / args.grid_id / f"{stem}_proc.tif"

    process_one(
        in_file=target_zip,
        out_file=out_file,
        aoi_wkt=aoi_wkt,
        pixel_spacing=args.pixel_spacing,
        pol=args.pol
    )

if __name__ == "__main__":
    main()