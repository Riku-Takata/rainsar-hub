import os
import sys
import re
import math
import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
import rasterio.features
import rasterio.mask
from rasterio.warp import reproject, Resampling
from shapely.geometry import shape

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("paddy_mask")

# ==========================================
# 設定
# ==========================================
BASE_DIR = Path(r"D:\sotsuron")
S1_SAMPLES_DIR = BASE_DIR / "s1_samples"
JAXA_DATA_DIR = BASE_DIR / "jaxa-data"

# 対象Grid ID
TARGET_GRIDS = [
    "N03145E13095", "N03285E13005", "N03285E13075", "N03285E13085",
    "N03285E13115", "N03285E13165", "N03295E12995", "N03295E13075",
    "N03295E13185", "N03325E13125", "N03335E13095", "N03355E13085",
    "N03355E13125", "N03375E13065", "N03375E13075", "N03385E13065",
    "N03375E13095"
]

# 内側に削るサイズ（メートル）
# 境界付近の混合ピクセルを除外するためマイナス値を設定
BUFFER_DISTANCE_METERS = -5.0 

# JAXAデータの定義 (田んぼのカテゴリID)
PADDY_CATEGORY_ID = 3

# ==========================================
# 関数
# ==========================================

def decode_grid_id(grid_id):
    """Grid IDから中心緯度経度を取得"""
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, grid_id)
    if not m: return None
    ns, lat_str, ew, lon_str = m.groups()
    lat = float(lat_str) / 100.0
    if ns == 'S': lat = -lat
    lon = float(lon_str) / 100.0
    if ew == 'W': lon = -lon
    return lat, lon

def get_year_from_filename(filename):
    """ファイル名から年を抽出"""
    match = re.search(r"(20\d{2})", filename)
    if match: return int(match.group(1))
    return None

def get_jaxa_lulc_path(year, lat, lon):
    """対象年と座標に基づいて適切なJAXA土地利用図のパスを返す"""
    lat_int = math.floor(lat)
    lon_int = math.floor(lon)
    filename = f"LC_N{lat_int:02d}E{lon_int:03d}.tif"
    
    # フォルダ構成に合わせてパスを選択
    if year >= 2022:
        version_dir = "2024JPN_v25.04"
    elif year >= 2020:
        version_dir = "2020JPN_v25.04"
    else:
        version_dir = "2018-2020JPN_v21.11_10m"
        
    path = JAXA_DATA_DIR / version_dir / filename
    
    # 見つからない場合のフォールバック（古いバージョンを確認）
    if not path.exists():
        fallback_path = JAXA_DATA_DIR / "2018-2020JPN_v21.11_10m" / filename
        if fallback_path.exists():
            return fallback_path
            
    return path

def create_eroded_paddy_mask(tif_path):
    """
    1つの衛星画像に対して、5m縮小した田んぼマスク画像を作成する
    """
    # 1. 画像情報の取得
    grid_id = tif_path.parent.name
    coords = decode_grid_id(grid_id)
    if not coords: return
    lat, lon = coords
    
    img_year = get_year_from_filename(tif_path.name)
    if img_year is None: return

    # 2. JAXAデータの特定
    jaxa_path = get_jaxa_lulc_path(img_year, lat, lon)
    if not jaxa_path.exists():
        logger.warning(f"  JAXA data not found for {img_year} around {lat},{lon}")
        return

    # 出力パス
    out_path = tif_path.with_name(tif_path.stem + "_paddy_mask.tif")
    # if out_path.exists(): return # 既に存在する場合はスキップ

    try:
        with rasterio.open(tif_path) as src:
            # 衛星画像のプロファイル
            meta = src.meta.copy()
            height, width = src.shape
            transform = src.transform
            crs = src.crs
            
            # --- A. JAXAデータを衛星画像のグリッドに合わせてリプロジェクト ---
            # メモリ上で処理（一時配列）
            jaxa_reprojected = np.zeros((height, width), dtype=rasterio.uint8)
            
            with rasterio.open(jaxa_path) as jaxa_src:
                reproject(
                    source=rasterio.band(jaxa_src, 1),
                    destination=jaxa_reprojected,
                    src_transform=jaxa_src.transform,
                    src_crs=jaxa_src.crs,
                    dst_transform=transform,
                    dst_crs=crs,
                    resampling=Resampling.nearest
                )

            # --- B. 田んぼピクセル(3)を抽出してベクトル化（ポリゴン化） ---
            # マスク作成: 田んぼ=1, その他=0
            paddy_binary = (jaxa_reprojected == PADDY_CATEGORY_ID).astype('uint8')
            
            # ベクトル化: (geometry, value) のジェネレータ
            shapes_gen = rasterio.features.shapes(paddy_binary, transform=transform)
            
            # 値が1（田んぼ）のジオメトリだけを取り出す
            polygons = [shape(geom) for geom, val in shapes_gen if val == 1]
            
            if not polygons:
                # logger.info("  No paddy fields found in this image.")
                return

            # GeoDataFrame化
            gdf = gpd.GeoDataFrame({'geometry': polygons}, crs=crs)

            # --- C. 内側に5m削る (Buffer -5m) ---
            # メートル座標系(UTM)に変換
            utm_crs = gdf.estimate_utm_crs()
            gdf_utm = gdf.to_crs(utm_crs)
            
            # バッファ処理 (マイナス値で縮小)
            # resolution=2 程度で角を少し丸めつつ軽量化
            gdf_utm['geometry'] = gdf_utm.geometry.buffer(BUFFER_DISTANCE_METERS, resolution=2)
            
            # 空になったポリゴン（幅が10m未満で消滅したもの）を除去
            gdf_utm = gdf_utm[~gdf_utm.is_empty]
            
            if gdf_utm.empty:
                logger.info("  All paddy fields removed after 5m shrinking.")
                return

            # WGS84に戻す
            gdf_shrunk = gdf_utm.to_crs(crs)
            
            # マスク用シェイプ形式に変換
            mask_shapes = [geom for geom in gdf_shrunk.geometry]

            # --- D. ラスタにマスク適用 ---
            # QGIS表示用メタデータ設定
            meta.update({
                "driver": "GTiff",
                "dtype": "float32",
                "nodata": np.nan,
                "compress": "lzw"
            })
            
            # マスク実行 (invert=False: ポリゴン内を残す)
            out_image, out_transform = rasterio.mask.mask(
                src, 
                mask_shapes, 
                crop=False, 
                invert=False, 
                nodata=np.nan
            )
            
            # 有効データの確認
            valid_px = np.sum(~np.isnan(out_image))
            
            if valid_px > 0:
                with rasterio.open(out_path, "w", **meta) as dest:
                    dest.write(out_image)
                logger.info(f"  [OK] Saved paddy mask (shrunk 5m): {out_path.name}")
            else:
                pass 
                # logger.info("  Mask created but empty.")

    except Exception as e:
        logger.error(f"  Error processing {tif_path.name}: {e}")


def main():
    logger.info("Starting Eroded Paddy Mask Generation (-5m)...")
    
    if not JAXA_DATA_DIR.exists():
        logger.error(f"JAXA Data dir not found: {JAXA_DATA_DIR}")
        return

    processed_count = 0
    
    for grid_id in TARGET_GRIDS:
        logger.info(f"Processing Grid: {grid_id}")
        grid_dir = S1_SAMPLES_DIR / grid_id
        
        tif_files = list(grid_dir.glob("*_proc.tif"))
        
        for tif_path in tif_files:
            # 他のマスクファイルはスキップ
            if "_mask" in tif_path.name or "_road" in tif_path.name or "_paddy" in tif_path.name:
                continue
            
            create_eroded_paddy_mask(tif_path)
            processed_count += 1

    logger.info("Processing completed.")

if __name__ == "__main__":
    main()