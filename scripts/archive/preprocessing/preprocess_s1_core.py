# -*- coding: utf-8 -*-
"""
preprocess_s1_core.py

Core logic for S1 preprocessing using SNAP API (snappy).
"""

import sys
import re
import logging
from pathlib import Path
from typing import Tuple, Optional

# Setup Logging
logger = logging.getLogger("s1_proc")

# Try importing snappy
try:
    import esa_snappy
    from esa_snappy import GPF, ProductIO, HashMap, jpy
except ImportError:
    # Fallback or error
    try:
        import snappy
        from snappy import GPF, ProductIO, HashMap, jpy
    except ImportError:
        logger.error("Error: 'esa_snappy' or 'snappy' module not found.")
        # We don't exit here to allow import, but functions will fail
        GPF = None

def init_snappy():
    if GPF is None:
        raise ImportError("SNAP API (snappy) not available.")
    # Initialize L2 cache or operators if needed
    GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
    logger.info("SNAP GPF operators loaded.")

def decode_grid_id(grid_id: str) -> Tuple[float, float]:
    """
    例: N03675E13685 -> (36.75, 136.85)
    """
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, grid_id)
    if not m:
        raise ValueError(f"Invalid Grid ID format: {grid_id}")
    ns, lat_str, ew, lon_str = m.groups()
    lat = float(lat_str) / 100.0
    if ns == 'S':
        lat = -lat
    lon = float(lon_str) / 100.0
    if ew == 'W':
        lon = -lon
    return lat, lon

def create_wkt_polygon(center_lat: float, center_lon: float, size: float = 0.1) -> str:
    half = size / 2
    min_lon, max_lon = center_lon - half, center_lon + half
    min_lat, max_lat = center_lat - half, center_lat + half
    return (
        f"POLYGON (({min_lon} {min_lat}, {min_lon} {max_lat}, "
        f"{max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"
    )

def process_one_scene_to_grid(
    in_file: Path,
    out_file: Path,
    grid_id: str,
    pixel_spacing: float = 10.0,
    pol: str = "VV"
):
    """
    Process one S1 scene (zip) -> Crop to Grid -> Preprocess -> Save TIF
    """
    if out_file.exists():
        logger.info(f"[SKIP] Already exists: {out_file.name}")
        return

    # Decode Grid
    try:
        lat, lon = decode_grid_id(grid_id)
        aoi_wkt = create_wkt_polygon(lat, lon, size=0.1) # 0.1 deg ~ 10km
    except Exception as e:
        logger.error(f"Failed to decode grid {grid_id}: {e}")
        return

    init_snappy()
    JInt = jpy.get_type('java.lang.Integer')

    logger.info(f"[READ] {in_file.name} -> Target: {grid_id}")
    source = None
    current_product = None

    try:
        # 1. Read
        source = ProductIO.readProduct(str(in_file))
        if source is None:
            logger.error(f"Failed to read product: {in_file}")
            return
        current_product = source

        # 2. Apply Orbit File
        logger.info("  - Apply Orbit File...")
        params_orb = HashMap()
        params_orb.put("orbitType", "Sentinel Precise (Auto Download)")
        params_orb.put("polyDegree", JInt(3))
        params_orb.put("continueOnFail", True)
        current_product = GPF.createProduct("Apply-Orbit-File", params_orb, current_product)

        # 3. Subset (Grid Clipping)
        logger.info(f"  - Subset (Clipping to {grid_id})...")
        params_sub = HashMap()
        params_sub.put("geoRegion", aoi_wkt)
        params_sub.put("copyMetadata", True)
        try:
            current_product = GPF.createProduct("Subset", params_sub, current_product)
        except Exception as e:
            logger.warning(f"Subset failed (possibly out of bounds): {e}")
            return

        if current_product.getSceneRasterWidth() == 0 or current_product.getSceneRasterHeight() == 0:
            logger.warning(f"[SKIP] Subset empty. Scene might not cover this grid.")
            return

        # 4. Thermal Noise Removal
        logger.info("  - Thermal Noise Removal...")
        params_tnr = HashMap()
        params_tnr.put("removeThermalNoise", True)
        current_product = GPF.createProduct("ThermalNoiseRemoval", params_tnr, current_product)

        # 5. Calibration
        logger.info(f"  - Calibration (Sigma0, pol={pol})...")
        params_cal = HashMap()
        params_cal.put("outputSigmaBand", True)
        params_cal.put("sourceBands", f"Intensity_{pol}")
        params_cal.put("selectedPolarisations", pol)
        params_cal.put("outputImageScaleInDb", False) 
        current_product = GPF.createProduct("Calibration", params_cal, current_product)

        # 6. Terrain Correction
        # Settings matched to preprocess_s1_cog.py for consistency
        logger.info(f"  - Terrain Correction (Spacing: {pixel_spacing} m)...")
        params_tc = HashMap()
        params_tc.put("demName", "Copernicus 30m Global DEM")
        params_tc.put("demResamplingMethod", "BILINEAR_INTERPOLATION")
        params_tc.put("imgResamplingMethod", "BILINEAR_INTERPOLATION")
        params_tc.put("pixelSpacingInMeter", float(pixel_spacing))
        params_tc.put("mapProjection", "WGS84(DD)")
        params_tc.put("saveSelectedSourceBand", True)
        params_tc.put("alignToStandardGrid", True)
        params_tc.put("standardGridOriginX", 0.0)
        params_tc.put("standardGridOriginY", 0.0)
        params_tc.put("nodataValueAtSea", False)
        
        current_product = GPF.createProduct("Terrain-Correction", params_tc, current_product)

        # 7. Linear -> dB
        logger.info("  - Convert to dB...")
        params_db = HashMap()
        current_product = GPF.createProduct("LinearToFromdB", params_db, current_product)

        # 8. Write
        out_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"[WRITE] {out_file}")
        write_path_stem = str(out_file.with_suffix(""))
        ProductIO.writeProduct(current_product, write_path_stem, "GeoTIFF-BigTIFF")
        logger.info(f"[DONE] Saved {out_file.name}")

    except Exception as e:
        logger.error(f"Processing failed for {in_file.name}: {e}", exc_info=True)
    finally:
        if current_product:
            current_product.dispose()
        if source:
            source.dispose()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Process one S1 scene to Grid (SNAP)")
    parser.add_argument("--in-file", required=True, type=str, help="Input ZIP file path")
    parser.add_argument("--out-file", required=True, type=str, help="Output TIF file path")
    parser.add_argument("--grid-id", required=True, type=str, help="Target Grid ID")
    parser.add_argument("--pol", default="VV", type=str, help="Polarization (VV or VH)")
    
    args = parser.parse_args()
    
    in_path = Path(args.in_file)
    out_path = Path(args.out_file)
    
    if not in_path.exists():
        logger.error(f"Input file not found: {in_path}")
        sys.exit(1)
        
    process_one_scene_to_grid(
        in_file=in_path,
        out_file=out_path,
        grid_id=args.grid_id,
        pol=args.pol
    )

if __name__ == "__main__":
    # Configure basic logging for CLI run
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
