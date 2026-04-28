import sys
import os
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as MplPolygon

# --- Configuration ---
# BASE_DIR: d:\sotsuron\rainsar-hub
BASE_DIR = Path(__file__).resolve().parent.parent 
S1_SAFE_DIR = BASE_DIR.parent / "s1_safe"
OUTPUT_ROOT = BASE_DIR / "result" / "pre_processing_vis"

# Add backend .venv to path for snappy
venv_site_packages = BASE_DIR / "backend" / ".venv" / "Lib" / "site-packages"
if venv_site_packages.exists():
    sys.path.append(str(venv_site_packages))

from common_utils import setup_logger, TARGET_GRIDS, decode_grid_id

logger = setup_logger("preprocessing_viz")

# 日本語フォント設定 (Windows)
plt.rcParams['font.family'] = 'MS Gothic'

# Try importing esa_snappy and jpy
try:
    import esa_snappy
    from esa_snappy import ProductIO, GPF, HashMap, WKTReader, GeoPos, PixelPos
    import jpy
    Integer = jpy.get_type('java.lang.Integer')
except ImportError:
    logger.error("Error: 'esa_snappy' or 'jpy' module not found.")
    logger.error(f"Tried adding: {venv_site_packages}")
    logger.error("Please run this script using the Python environment where ESA SNAP/snappy is installed.")
    sys.exit(1)

# Initialize SNAP Operators
GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()

def create_wkt_polygon(center_lat: float, center_lon: float, size: float = 0.02) -> str:
    """中心座標とサイズ(度)を使って矩形(対象領域)をWKT POLYGONで生成"""
    half = size / 2
    min_lon, max_lon = center_lon - half, center_lon + half
    min_lat, max_lat = center_lat - half, center_lat + half
    return (
        f"POLYGON (({min_lon} {min_lat}, {min_lon} {max_lat}, "
        f"{max_lon} {max_lat}, {max_lon} {min_lat}, {min_lon} {min_lat}))"
    )

def get_roi_polygon_pixels(product, center_lat, center_lon, size=0.02, subsample=1):
    """
    対象領域(ROI)の4隅の座標を、Product上のピクセル座標(x, y)に変換してリストで返す。
    subsampleが指定されている場合、ピクセル座標を縮小する。
    """
    half = size / 2
    # 4隅の緯度経度 (反時計回り等で定義)
    corners_geo = [
        (center_lat - half, center_lon - half), # MinLat, MinLon
        (center_lat - half, center_lon + half), # MinLat, MaxLon
        (center_lat + half, center_lon + half), # MaxLat, MaxLon
        (center_lat + half, center_lon - half), # MaxLat, MinLon
    ]
    
    gc = product.getSceneGeoCoding()
    pixel_coords = []
    
    for lat, lon in corners_geo:
        gp = GeoPos(lat, lon)
        pp = PixelPos()
        gc.getPixelPos(gp, pp)
        
        # PixelPosは有効範囲外でも値を返すことがあるが、NaNの場合は無効
        if str(pp.x) == 'NaN' or str(pp.y) == 'NaN':
            continue
            
        # サブサンプリング補正
        x = pp.x / subsample
        y = pp.y / subsample
        pixel_coords.append((x, y))
        
    return pixel_coords

def get_band_data(product, band_name):
    """Productからバンドデータをnumpy配列として取得する"""
    band = product.getBand(band_name)
    if band is None:
        band_names = list(product.getBandNames())
        for bn in band_names:
            if band_name in bn:
                band = product.getBand(bn)
                break
        if band is None:
            raise ValueError(f"Band {band_name} not found in product.")

    w = band.getRasterWidth()
    h = band.getRasterHeight()
    data = np.zeros(w * h, np.float32)
    band.readPixels(0, 0, w, h, data)
    return data.reshape(h, w)

def save_comparison_image(img_before, img_after, step_name, output_path, label_before="適用前", label_after="適用後", roi_polygon=None):
    """
    img_before, img_after: 画像データ
    roi_polygon: [(x1,y1), (x2,y2), ...] Step1のBefore画像に描画する赤枠の座標リスト
    """
    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    
    def plot_ax(ax, img, title, roi_coords=None):
        valid_mask = ~np.isnan(img)
        if not np.any(valid_mask):
            ax.text(0.5, 0.5, "有効データなし", ha='center')
            return
            
        valid_data = img[valid_mask]
        with np.errstate(invalid='ignore'):
            vmin, vmax = np.percentile(valid_data, [2, 98])
        
        im = ax.imshow(img, cmap='gray', vmin=vmin, vmax=vmax, interpolation='nearest')
        ax.set_title(title)
        ax.axis('off')
        
        # ROI描画
        if roi_coords and len(roi_coords) >= 3:
            poly = MplPolygon(roi_coords, closed=True, edgecolor='red', facecolor='none', linewidth=2)
            ax.add_patch(poly)
            
        return im

    plot_ax(axes[0], img_before, label_before, roi_coords=roi_polygon)
    plot_ax(axes[1], img_after, label_after)
    
    plt.suptitle(f"工程: {step_name}", fontsize=16)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info(f"Saved comparison: {output_path}")

def save_single_image(img, step_name, output_path):
    plt.figure(figsize=(8, 8))
    valid_mask = ~np.isnan(img)
    if np.any(valid_mask):
        valid_data = img[valid_mask]
        with np.errstate(invalid='ignore'):
            vmin, vmax = np.percentile(valid_data, [2, 98])
        plt.imshow(img, cmap='gray', vmin=vmin, vmax=vmax, interpolation='nearest')
    else:
        plt.text(0.5, 0.5, "有効データなし", ha='center')
    plt.title(step_name)
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    logger.info(f"Saved image: {output_path}")

def process_grid(grid_id):
    logger.info(f"Processing Grid: {grid_id}")
    
    grid_dir = S1_SAFE_DIR / grid_id
    if not grid_dir.exists():
        logger.warning(f"Directory not found: {grid_dir}")
        return

    # Find first zip file
    zip_files = list(grid_dir.glob("*.zip"))
    if not zip_files:
        logger.warning(f"No zip files found in {grid_dir}")
        return
    
    input_file = zip_files[0]
    logger.info(f"Using input: {input_file.name}")
    
    # Setup Output
    output_dir = OUTPUT_ROOT / grid_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Calculate Target Region
    lat, lon = decode_grid_id(grid_id)
    target_region_size = 0.02
    target_region_wkt = create_wkt_polygon(lat, lon, target_region_size)
    
    # --- Processing ---
    product = None
    step0_subset = None
    step1_product = None
    step2_product = None
    step3_product = None
    step4_product = None
    step5_product = None

    try:
        product = ProductIO.readProduct(str(input_file))
        if product is None:
            logger.error(f"Failed to read product: {input_file}")
            return

        # Prepare for Step 1 Before (Full Scene)
        logger.info("  Reading Full Scene (Quicklook)...")
        # 間引き読み込み (Scale 1/10)
        subsample_factor = 10
        params = HashMap()
        params.put('subSamplingX', Integer(subsample_factor))
        params.put('subSamplingY', Integer(subsample_factor))
        step0_subset = GPF.createProduct('Subset', params, product)
        
        band_name_raw = 'Amplitude_VV'
        if 'Intensity_VV' in list(product.getBandNames()):
            band_name_raw = 'Intensity_VV'
            
        data_step0 = get_band_data(step0_subset, band_name_raw)

        # Get ROI Polygon Pixels for Step 0 (accounting for subsample)
        roi_pixels = get_roi_polygon_pixels(product, lat, lon, target_region_size, subsample=subsample_factor)

        # Step 1: Subset (Real Clipping of Target Region)
        logger.info("  Step 1: 領域切り出し (Subset)...")
        params = HashMap()
        params.put('copyMetadata', True)
        params.put('geoRegion', target_region_wkt)
        step1_product = GPF.createProduct('Subset', params, product)

        if step1_product.getSceneRasterWidth() == 0:
            logger.warning(f"  Subset Empty. Skipping {grid_id}")
            return

        if 'Intensity_VV' in list(step1_product.getBandNames()):
            band_name_step1 = 'Intensity_VV'
        else:
            band_name_step1 = 'Amplitude_VV'
        
        data_step1 = get_band_data(step1_product, band_name_step1)
        
        # Save Comparison Step 1 with ROI Box
        save_comparison_image(
            data_step0, 
            data_step1, 
            "領域切り出し (Subset)", 
            output_dir / "comparison_step1_subset.png",
            label_before="適用前 (全景・間引き)", 
            label_after="適用後 (対象領域)",
            roi_polygon=roi_pixels
        )
        save_single_image(data_step1, "ステップ1: 切り出し後 (Linear)", output_dir / "step1_subset.png")

        # Step 2: Thermal Noise Removal
        logger.info("  Step 2: 熱雑音除去 (Thermal Noise Removal)...")
        params = HashMap()
        params.put('removeThermalNoise', True)
        step2_product = GPF.createProduct('ThermalNoiseRemoval', params, step1_product)
        data_step2 = get_band_data(step2_product, 'Intensity_VV')
        
        save_comparison_image(
            data_step1**2 if 'Amplitude' in band_name_step1 else data_step1, 
            data_step2, 
            "熱雑音除去", 
            output_dir / "comparison_step2_noise.png",
            label_before="適用前 (生データ)", label_after="適用後 (除去済)"
        )

        # Step 3: Calibration
        logger.info("  Step 3: 放射量補正 (Calibration)...")
        params = HashMap()
        params.put('outputSigmaBand', True)
        params.put('selectedPolarisations', 'VV')
        step3_product = GPF.createProduct('Calibration', params, step2_product)
        data_step3 = get_band_data(step3_product, 'Sigma0_VV')
        
        save_comparison_image(
            data_step2, 
            data_step3, 
            "放射量補正 (Calibration)", 
            output_dir / "comparison_step3_calibration.png",
            label_before="適用前 (Intensity)", label_after="適用後 (Sigma0)"
        )

        # Step 4: Terrain Correction
        logger.info("  Step 4: 地形補正 (Terrain Correction)...")
        params = HashMap()
        params.put('demName', 'SRTM 3Sec')
        params.put('demResamplingMethod', 'BILINEAR_INTERPOLATION')
        params.put('imgResamplingMethod', 'BILINEAR_INTERPOLATION')
        params.put('pixelSpacingInMeter', 10.0)
        params.put('mapProjection', 'WGS84(DD)')
        params.put('nodataValueAtSea', False)
        step4_product = GPF.createProduct('Terrain-Correction', params, step3_product)
        data_step4 = get_band_data(step4_product, 'Sigma0_VV')
        
        save_comparison_image(
            data_step3, 
            data_step4, 
            "地形補正 (Terrain Correction)", 
            output_dir / "comparison_step4_tc.png",
            label_before="適用前 (斜距離画像)", label_after="適用後 (地図座標画像)"
        )
        save_single_image(data_step4, "ステップ4: 地形補正後", output_dir / "step4_terrain_corrected.png")

        # Step 5: dB Conversion
        logger.info("  Step 5: dB変換 (dB Conversion)...")
        params = HashMap()
        step5_product = GPF.createProduct('LinearToFromdB', params, step4_product)
        data_step5 = get_band_data(step5_product, 'Sigma0_VV_db')
        
        save_comparison_image(
            data_step4, 
            data_step5, 
            "dB変換 (dB Conversion)", 
            output_dir / "comparison_step5_db.png",
            label_before="適用前 (リニア)", label_after="適用後 (dB)"
        )
        save_single_image(data_step5, "ステップ5: 最終画像 (dB)", output_dir / "step5_db_final.png")
        
        logger.info(f"  Finished {grid_id}")

    except Exception as e:
        logger.error(f"Error processing {grid_id}: {e}")
    finally:
        # Cleanup
        try:
            if 'step0_subset' in locals() and step0_subset: step0_subset.dispose()
            if 'product' in locals() and product: product.dispose()
            if 'step1_product' in locals() and step1_product: step1_product.dispose()
            if 'step2_product' in locals() and step2_product: step2_product.dispose()
            if 'step3_product' in locals() and step3_product: step3_product.dispose()
            if 'step4_product' in locals() and step4_product: step4_product.dispose()
            if 'step5_product' in locals() and step5_product: step5_product.dispose()
        except Exception:
            pass

def main():
    logger.info(f"対象グリッド数: {len(TARGET_GRIDS)}")
    for grid_id in TARGET_GRIDS:
        process_grid(grid_id)

if __name__ == "__main__":
    main()
