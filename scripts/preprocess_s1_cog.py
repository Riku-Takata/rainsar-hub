#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
preprocess_s1_cog.py

解析用前処理パイプライン (rev. coreg-simple)
- Speckle Filter を外す
- Terrain Correction で標準グリッドに揃え、同じ grid-id / pixel-spacing で処理した2枚が
  同じ GeoTIFF グリッドになるようにする
"""

import argparse
import logging
import sys
import re
from pathlib import Path
from typing import Tuple

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

# SNAP オペレータをロード
GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()


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
    """
    grid-id から中心座標とサイズ(度)を使って矩形 AOI を WKT POLYGON で生成
    """
    half = size / 2
    min_lon, max_lon = center_lon - half, center_lon + half
    min_lat, max_lat = center_lat - half, center_lat + half
    return (
        f"POLYGON (({min_lon} {min_lat}, {min_lon} {max_lat}, "
        f"{max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"
    )


def process_one(
    in_file: Path,
    out_file: Path,
    aoi_wkt: str,
    pixel_spacing: float = 10.0,
    pol: str = "VH",
):
    """
    1シーン分を前処理して、AOI 部分のみの dB GeoTIFF を出力。
    - Speckle Filter はかけない
    - Terrain Correction で alignToStandardGrid=True とし、
      同じ AOI / pixel_spacing なら同一グリッドに揃うようにする
    """
    if out_file.exists():
        logger.info(f"[SKIP] Already exists: {out_file.name}")
        return

    logger.info(f"[READ] {in_file.name}")
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

        # 3. Subset (Grid切り出し)
        logger.info("  - Subset (Clipping by Grid ID AOI)...")
        params_sub = HashMap()
        params_sub.put("geoRegion", aoi_wkt)
        params_sub.put("copyMetadata", True)
        try:
            current_product = GPF.createProduct("Subset", params_sub, current_product)
        except Exception as e:
            logger.warning(f"Subset failed (possibly out of bounds): {e}")
            return

        # Subset 結果の空チェック
        if current_product.getSceneRasterWidth() == 0 or current_product.getSceneRasterHeight() == 0:
            logger.warning(f"[SKIP] Subset resulted in empty product (No intersection). File: {in_file.name}")
            return

        # 4. Thermal Noise Removal
        logger.info("  - Thermal Noise Removal...")
        params_tnr = HashMap()
        params_tnr.put("removeThermalNoise", True)
        current_product = GPF.createProduct("ThermalNoiseRemoval", params_tnr, current_product)

        # 5. Calibration (Sigma0, 線形スケール)
        logger.info(f"  - Calibration (Sigma0, pol={pol})...")
        params_cal = HashMap()
        params_cal.put("outputSigmaBand", True)
        params_cal.put("sourceBands", f"Intensity_{pol}")
        params_cal.put("selectedPolarisations", pol)
        params_cal.put("outputImageScaleInDb", False)  # ここでは線形のまま
        current_product = GPF.createProduct("Calibration", params_cal, current_product)

        # 6. Terrain Correction (標準グリッドに揃える)
        logger.info(f"  - Terrain Correction (Spacing: {pixel_spacing} m, alignToStandardGrid=True)...")
        params_tc = HashMap()
        params_tc.put("demName", "Copernicus 30m Global DEM")
        params_tc.put("demResamplingMethod", "BILINEAR_INTERPOLATION")
        params_tc.put("imgResamplingMethod", "BILINEAR_INTERPOLATION")
        params_tc.put("pixelSpacingInMeter", float(pixel_spacing))
        # ここが重要：同じ AOI / pixelSpacing / standardGridOrigin で処理すれば、
        # 異なるシーンでも同一グリッドに揃う
        params_tc.put("mapProjection", "WGS84(DD)")
        params_tc.put("saveSelectedSourceBand", True)
        params_tc.put("nodataValueAtSea", False)
        params_tc.put("alignToStandardGrid", True)
        # Origin は 0,0 に揃えておく（WGS84(DD) なので経度緯度単位）
        # ※ pixelSpacingInMeter との組み合わせで内部的に最寄りのグリッドにスナップされる
        params_tc.put("standardGridOriginX", 0.0)
        params_tc.put("standardGridOriginY", 0.0)

        current_product = GPF.createProduct("Terrain-Correction", params_tc, current_product)

        # 7. 線形 -> dB 変換
        logger.info("  - Convert to dB...")
        params_db = HashMap()
        current_product = GPF.createProduct("LinearToFromdB", params_db, current_product)

        # 8. Write (GeoTIFF-BigTIFF)
        logger.info(f"[WRITE] {out_file.name}")
        out_file.parent.mkdir(parents=True, exist_ok=True)
        write_path_stem = str(out_file.with_suffix(""))
        ProductIO.writeProduct(current_product, write_path_stem, "GeoTIFF-BigTIFF")
        logger.info(f"[DONE] Saved to {out_file}")

    except Exception as e:
        logger.error(f"Processing failed for {in_file.name}: {e}", exc_info=True)
    finally:
        # Java オブジェクトの解放
        try:
            if source is not None:
                source.dispose()
        except Exception:
            pass
        try:
            if current_product is not None:
                current_product.dispose()
        except Exception:
            pass


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

    # grid-id -> AOI ポリゴン
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
        pol=args.pol,
    )


if __name__ == "__main__":
    main()
