import os
import sys
import re
import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.geometry import mapping, box

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("highway_mask")

# ==========================================
# 設定
# ==========================================
BASE_DIR = Path(r"D:\sotsuron")
S1_SAMPLES_DIR = BASE_DIR / "s1_samples"
ROAD_POLYGON_DIR = BASE_DIR / "road-polygon"

# 対象Grid ID
TARGET_GRIDS = [
    "N03145E13095", "N03285E13005", "N03285E13075", "N03285E13085",
    "N03285E13115", "N03285E13165", "N03295E12995", "N03295E13075",
    "N03295E13185", "N03325E13125", "N03335E13095", "N03355E13085",
    "N03355E13125", "N03375E13065", "N03375E13075", "N03385E13065",
    "N03375E13095"
]

# 道路バッファサイズ（メートル）
# 中心線から片側何メートル広げるか (15m = 幅30m)
ROAD_BUFFER_METER = 5.0 

# ==========================================
# 関数
# ==========================================

def get_year_from_filename(filename):
    """衛星画像ファイル名から年を抽出"""
    match = re.search(r"(20\d{2})", filename)
    if match:
        return int(match.group(1))
    return None

def find_available_shapefiles(root_dir):
    """利用可能なシェープファイルを検索 (Shift-JIS優先)"""
    if not root_dir.exists():
        return {}

    shapefiles = {}
    candidates = list(root_dir.glob("**/*HighwaySection.shp"))
    
    for shp_path in candidates:
        if "UTF-8" in str(shp_path): continue

        match = re.search(r"N06-(\d{2})", shp_path.name)
        if match:
            yy = int(match.group(1))
            year = 2000 + yy
            
            if year in shapefiles:
                if "Shift-JIS" in str(shp_path) and "Shift-JIS" not in str(shapefiles[year]):
                     shapefiles[year] = shp_path
            else:
                shapefiles[year] = shp_path
            continue
            
        match_full = re.search(r"N06-(20\d{2})", shp_path.name)
        if match_full:
            year = int(match_full.group(1))
            if year in shapefiles:
                if "Shift-JIS" in str(shp_path) and "Shift-JIS" not in str(shapefiles[year]):
                     shapefiles[year] = shp_path
            else:
                shapefiles[year] = shp_path
    
    return shapefiles

def get_best_match_shapefile(target_year, available_files):
    """target_yearに最も近い年のファイルパスを返す"""
    if not available_files: return None, None
    years = np.array(list(available_files.keys()))
    idx = (np.abs(years - target_year)).argmin()
    nearest_year = years[idx]
    return available_files[nearest_year], nearest_year

def create_highway_mask(tif_path, available_shps):
    """
    1つの衛星画像に対して高速道路マスクを作成
    参考にしたコードをベースに、QGIS表示対策と座標変換を追加
    """
    
    # 1. 画像の年代特定とシェープファイル選択
    img_year = get_year_from_filename(tif_path.name)
    if img_year is None: return 

    shp_path, map_year = get_best_match_shapefile(img_year, available_shps)
    if shp_path is None: return

    # 出力パス
    out_path = tif_path.with_name(tif_path.stem + "_highway_mask.tif")
    # if out_path.exists(): return # 上書きしたい場合はコメントアウト

    try:
        with rasterio.open(tif_path) as src:
            raster_crs = src.crs
            # QGIS対策: データ型をfloat32、NoDataをNaNに設定するためのメタデータ準備
            out_meta = src.meta.copy()
            out_meta.update({
                "driver": "GTiff",
                "dtype": "float32",
                "nodata": np.nan,  # QGISで透明にするために重要
                "compress": "lzw"  # ファイルサイズ圧縮
            })

            # --- A. Shapefile読み込みとフィルタ ---
            try:
                # 範囲を絞って読み込むために画像のBoundingBoxを取得
                bounds = src.bounds
                img_box = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
                
                # 読み込み (cp932)
                gdf = gpd.read_file(shp_path, encoding='cp932')
                
                if gdf.crs is None:
                    gdf.set_crs("EPSG:6668", inplace=True) # JGD2011

                # WGS84 (画像と同じ) に変換
                if gdf.crs != raster_crs:
                    gdf = gdf.to_crs(raster_crs)

                # クリップ (画像範囲外のデータを除外)
                gdf_clipped = gpd.clip(gdf, img_box)
                
                if gdf_clipped.empty:
                    return # 高速道路がない場合は終了

            except Exception as e:
                logger.warning(f"  Shapefile read error: {e}")
                return

            # --- B. バッファ処理 (メートル単位で行う) ---
            # ここが重要: WGS84のままbuffer(30)すると30度になってしまう
            utm_crs = gdf_clipped.estimate_utm_crs()
            gdf_utm = gdf_clipped.to_crs(utm_crs)
            
            # バッファ (30m幅なら半径15m)
            # 既にポリゴンになっているデータでも、中心線データでも、bufferをかければ安全に太る
            buffered_utm = gdf_utm.geometry.buffer(ROAD_BUFFER_METER)
            
            # 座標系を画像に戻す
            roads_buffer = buffered_utm.to_crs(raster_crs)

            # --- C. マスク用ジオメトリ作成 ---
            # rasterio.maskにはGeoJSONライクな辞書リストを渡す
            shapes = [mapping(geom) for geom in roads_buffer]

            # --- D. ラスタにマスクをかける ---
            # invert=False: シェイプの「内側」を残す (高速道路を残す)
            # invert=True: シェイプの「内側」を消す (高速道路を除去する)
            # 今回は「高速道路マスク画像」を作りたいので False (道路を残す)
            
            out_image, out_transform = mask(
                src,
                shapes,
                invert=False, 
                crop=False,      # False推奨: 元画像と同じサイズ・位置を維持（QGISで重ねやすい）
                nodata=np.nan,   # マスク外をNaNにする
                filled=True      # fill_value (nodata) で埋める
            )

            # データ有効性の確認
            valid_px = np.sum(~np.isnan(out_image))
            
            if valid_px > 0:
                # 書き出し
                with rasterio.open(out_path, "w", **out_meta) as dst:
                    dst.write(out_image) # データ型はmetaに合わせて自動キャストされる
                logger.info(f"  [OK] Saved mask for {img_year} (Map: {map_year}): {out_path.name}")
            else:
                logger.info(f"  [Info] Mask empty for {img_year} (No overlap)")

    except Exception as e:
        logger.error(f"  Error processing {tif_path.name}: {e}")


def main():
    logger.info("Starting Corrected Highway Mask Generation...")
    
    if not ROAD_POLYGON_DIR.exists():
        logger.error(f"Directory not found: {ROAD_POLYGON_DIR}")
        return

    # 1. マップ検索
    available_shps = find_available_shapefiles(ROAD_POLYGON_DIR)
    if not available_shps:
        logger.error("No Shapefiles found.")
        return
    
    logger.info(f"Maps found: {sorted(available_shps.keys())}")

    # 2. 処理実行
    processed_count = 0
    for grid_id in TARGET_GRIDS:
        logger.info(f"Processing Grid: {grid_id}")
        grid_dir = S1_SAMPLES_DIR / grid_id
        
        tif_files = list(grid_dir.glob("*_proc.tif"))
        for tif_path in tif_files:
            if "_mask" in tif_path.name or "_road" in tif_path.name or "_paddy" in tif_path.name:
                continue
            
            create_highway_mask(tif_path, available_shps)
            processed_count += 1

    logger.info("Processing completed.")

if __name__ == "__main__":
    main()