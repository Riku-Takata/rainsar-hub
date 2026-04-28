#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
preprocess_s1_tokyo_batch.py

E:\s1_tokyo_2025 に格納された全 S1 ZIP ファイルを一括前処理する。

処理パイプライン (SNAPベース):
  1. Apply Orbit File
  2. Subset (東京都バウンディングボックスにクリップ)
  3. Thermal Noise Removal
  4. Calibration (Sigma0, 線形スケール)
  5. Terrain Correction (10m, WGS84, 標準グリッド揃え)
  6. Linear → dB 変換
  7. GeoTIFF-BigTIFF 出力

使い方:
  cd D:\sotsuron\rainsar-hub\backend
  .\.venv\Scripts\python.exe ..\scripts\preprocess_s1_tokyo_batch.py
"""

import argparse
import logging
import sys
from pathlib import Path

# --- SNAP (esa_snappy) ---
try:
    import esa_snappy
    from esa_snappy import GPF, ProductIO, HashMap, jpy
except ImportError:
    try:
        import snappy
        from snappy import GPF, ProductIO, HashMap, jpy
    except ImportError:
        print("Error: 'esa_snappy' or 'snappy' module not found.")
        sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("s1_tokyo_batch")

# SNAP operators の初期化
GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
JInt = jpy.get_type("java.lang.Integer")

# 東京都を広めにカバーするバウンディングボックス (島嶼除く本土部分)
TOKYO_BBOX = {
    "min_lat": 35.50,
    "max_lat": 35.90,
    "min_lon": 139.40,
    "max_lon": 140.00,
}


def create_wkt_from_bbox(min_lat, max_lat, min_lon, max_lon):
    """バウンディングボックスからWKT POLYGONを生成"""
    return (
        f"POLYGON (({min_lon} {min_lat}, {min_lon} {max_lat}, "
        f"{max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"
    )


def process_one_scene(
    in_file: Path,
    out_dir: Path,
    aoi_wkt: str,
    pixel_spacing: float = 10.0,
    polarizations: list = None,
):
    """
    1シーン分を前処理。指定偏波ごとに個別にキャリブレーション→出力する。
    """
    if polarizations is None:
        polarizations = ["VV", "VH"]

    scene_stem = in_file.stem  # ZIP のファイル名(拡張子なし)

    for pol in polarizations:
        out_file = out_dir / f"{scene_stem}_{pol}_sigma0_dB.tif"

        if out_file.exists():
            logger.info(f"[SKIP] Already exists: {out_file.name}")
            continue

        logger.info(f"[START] {in_file.name} | Pol: {pol}")

        source = None
        current_product = None

        try:
            # 1. Read
            logger.info("  1/7  Read product...")
            source = ProductIO.readProduct(str(in_file))
            if source is None:
                logger.error(f"Failed to read product: {in_file}")
                return
            current_product = source

            # 2. Apply Orbit File
            logger.info("  2/7  Apply Orbit File...")
            params_orb = HashMap()
            params_orb.put("orbitType", "Sentinel Precise (Auto Download)")
            params_orb.put("polyDegree", JInt(3))
            params_orb.put("continueOnFail", True)
            current_product = GPF.createProduct(
                "Apply-Orbit-File", params_orb, current_product
            )

            # 3. Subset (東京都の範囲にクリップ)
            logger.info("  3/7  Subset (Tokyo AOI)...")
            params_sub = HashMap()
            params_sub.put("geoRegion", aoi_wkt)
            params_sub.put("copyMetadata", True)
            try:
                current_product = GPF.createProduct(
                    "Subset", params_sub, current_product
                )
            except Exception as e:
                logger.warning(f"Subset failed (possibly out of bounds): {e}")
                return

            w = current_product.getSceneRasterWidth()
            h = current_product.getSceneRasterHeight()
            if w == 0 or h == 0:
                logger.warning(
                    f"[SKIP] Subset empty — scene does not cover Tokyo AOI."
                )
                return
            logger.info(f"         Subset size: {w} x {h} pixels")

            # 4. Thermal Noise Removal
            logger.info("  4/7  Thermal Noise Removal...")
            params_tnr = HashMap()
            params_tnr.put("removeThermalNoise", True)
            current_product = GPF.createProduct(
                "ThermalNoiseRemoval", params_tnr, current_product
            )

            # 5. Calibration (Sigma0, 線形スケール)
            logger.info(f"  5/7  Calibration (Sigma0, pol={pol})...")
            params_cal = HashMap()
            params_cal.put("outputSigmaBand", True)
            params_cal.put("sourceBands", f"Intensity_{pol}")
            params_cal.put("selectedPolarisations", pol)
            params_cal.put("outputImageScaleInDb", False)
            current_product = GPF.createProduct(
                "Calibration", params_cal, current_product
            )

            # 6. Terrain Correction
            logger.info(
                f"  6/7  Terrain Correction ({pixel_spacing}m, alignToStandardGrid)..."
            )
            params_tc = HashMap()
            params_tc.put("demName", "Copernicus 30m Global DEM")
            params_tc.put("demResamplingMethod", "BILINEAR_INTERPOLATION")
            params_tc.put("imgResamplingMethod", "BILINEAR_INTERPOLATION")
            params_tc.put("pixelSpacingInMeter", float(pixel_spacing))
            params_tc.put("mapProjection", "WGS84(DD)")
            params_tc.put("saveSelectedSourceBand", True)
            params_tc.put("nodataValueAtSea", False)
            params_tc.put("alignToStandardGrid", True)
            params_tc.put("standardGridOriginX", 0.0)
            params_tc.put("standardGridOriginY", 0.0)
            current_product = GPF.createProduct(
                "Terrain-Correction", params_tc, current_product
            )

            # 7. Linear → dB
            logger.info("  7/7  Convert to dB...")
            params_db = HashMap()
            current_product = GPF.createProduct(
                "LinearToFromdB", params_db, current_product
            )

            # Write
            out_dir.mkdir(parents=True, exist_ok=True)
            write_stem = str(out_file.with_suffix(""))
            logger.info(f"[WRITE] {out_file.name}")
            ProductIO.writeProduct(current_product, write_stem, "GeoTIFF-BigTIFF")
            logger.info(f"[DONE]  {out_file.name}")

        except Exception as e:
            logger.error(f"Processing failed: {e}", exc_info=True)
        finally:
            try:
                if current_product is not None:
                    current_product.dispose()
            except Exception:
                pass
            try:
                if source is not None:
                    source.dispose()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(
        description="Batch preprocess S1 scenes for Tokyo area"
    )
    parser.add_argument(
        "--in-dir",
        type=str,
        default=r"E:\s1_tokyo_2025",
        help="Input directory containing S1 ZIP files",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=r"E:\s1_tokyo_2025\processed",
        help="Output directory for processed GeoTIFFs",
    )
    parser.add_argument(
        "--pixel-spacing",
        type=float,
        default=10.0,
        help="Terrain correction pixel spacing in meters (default: 10.0)",
    )
    parser.add_argument(
        "--pol",
        type=str,
        nargs="+",
        default=["VV", "VH"],
        choices=["VV", "VH"],
        help="Polarizations to process (default: VV VH)",
    )
    args = parser.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)

    zip_files = sorted(in_dir.glob("S1*_IW_GRDH_*.zip"))
    if not zip_files:
        logger.error(f"No S1 ZIP files found in {in_dir}")
        sys.exit(1)

    aoi_wkt = create_wkt_from_bbox(**TOKYO_BBOX)

    logger.info("=" * 60)
    logger.info(f"S1 Tokyo Batch Preprocessing")
    logger.info(f"  Input dir  : {in_dir}")
    logger.info(f"  Output dir : {out_dir}")
    logger.info(f"  Scenes     : {len(zip_files)}")
    logger.info(f"  Polarizations: {args.pol}")
    logger.info(f"  Pixel spacing: {args.pixel_spacing} m")
    logger.info(f"  AOI (WKT)  : {aoi_wkt}")
    logger.info("=" * 60)

    for i, zf in enumerate(zip_files, 1):
        logger.info(f"\n[{i}/{len(zip_files)}] {zf.name}")
        process_one_scene(
            in_file=zf,
            out_dir=out_dir,
            aoi_wkt=aoi_wkt,
            pixel_spacing=args.pixel_spacing,
            polarizations=args.pol,
        )

    logger.info("\n=== All scenes processed. ===")


if __name__ == "__main__":
    main()
