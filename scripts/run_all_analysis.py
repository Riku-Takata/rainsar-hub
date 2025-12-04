import os
import re
import glob
import zipfile
import tempfile
import shutil
import logging
import datetime
import json
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask
from rasterio.warp import reproject, Resampling
import geopandas as gpd
from shapely.geometry import box
import osmnx as ox

# --- 設定項目 ---
BASE_DIR = r"D:\sotsuron"
SAMPLES_ROOT = os.path.join(BASE_DIR, "s1_samples")
SAFE_ROOT = os.path.join(BASE_DIR, "s1_safe")
RESULT_ROOT = os.path.join(BASE_DIR, "result")
FUDE_POLYGON_DIR = os.path.join(BASE_DIR, "fude-polygon")

DELAY_THRESHOLD = 7.0
PADDY_EROSION_METER = -2.0
ROAD_BUFFER_METER = 2.5

# 都道府県庁座標
PREF_CENTERS = {
    1: (43.06, 141.35), 2: (40.82, 140.74), 3: (39.70, 141.15), 4: (38.27, 140.87), 5: (39.72, 140.10),
    6: (38.24, 140.36), 7: (37.75, 140.47), 8: (36.34, 140.45), 9: (36.57, 139.88), 10: (36.39, 139.06),
    11: (35.86, 139.65), 12: (35.61, 140.12), 13: (35.69, 139.69), 14: (35.45, 139.64), 15: (37.90, 139.02),
    16: (36.69, 137.21), 17: (36.59, 136.63), 18: (36.06, 136.22), 19: (35.66, 138.57), 20: (36.65, 138.18),
    21: (35.39, 136.72), 22: (34.98, 138.38), 23: (35.18, 136.91), 24: (34.73, 136.51), 25: (35.00, 135.87),
    26: (35.02, 135.76), 27: (34.69, 135.52), 28: (34.69, 135.18), 29: (34.69, 135.83), 30: (34.23, 135.17),
    31: (35.50, 134.24), 32: (35.47, 133.05), 33: (34.66, 133.93), 34: (34.39, 132.46), 35: (34.19, 131.47),
    36: (34.07, 134.56), 37: (34.34, 134.04), 38: (33.84, 132.77), 39: (33.56, 133.53), 40: (33.61, 130.42),
    41: (33.25, 130.30), 42: (32.74, 129.87), 43: (32.79, 130.74), 44: (33.24, 131.61), 45: (31.91, 131.42),
    46: (31.56, 130.56), 47: (26.21, 127.68)
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(BASE_DIR, "analysis_v4_log.txt"), encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

def find_proc_file(grid_dir, scene_id):
    exact_path = os.path.join(grid_dir, f"{scene_id}_proc.tif")
    if os.path.exists(exact_path): return exact_path
    match = re.search(r'\d{8}T\d{6}', scene_id)
    if match:
        cands = glob.glob(os.path.join(grid_dir, f"*{match.group(0)}*_proc.tif"))
        if cands: return cands[0]
    return None

def align_and_mask(src_master, src_slave, shapes):
    """
    1. Master画像をShapeでマスク（切り抜き）する。
    2. Slave画像を、Master画像の切り抜き後のグリッド（Transform）に合わせて再投影（Reproject）する。
    3. これにより、地理座標レベルで完全に一致した2つの配列を得る。
    """
    # 1. Masterをマスク (nodata=NaN, all_touched=Trueで細い線も拾う)
    # これにより、ポリゴン外はNaNが入った矩形配列が得られる
    master_img, master_transform = mask(src_master, shapes, crop=True, nodata=np.nan, all_touched=True)
    master_arr = master_img[0] # Band 1

    # 2. SlaveをMasterのグリッドに合わせて再投影
    # Masterと同じ形状の空配列を用意
    slave_arr = np.empty_like(master_arr)
    
    reproject(
        source=rasterio.band(src_slave, 1),
        destination=slave_arr,
        src_transform=src_slave.transform,
        src_crs=src_slave.crs,
        dst_transform=master_transform,
        dst_crs=src_master.crs,
        resampling=Resampling.nearest # 値の変質を防ぐならnearest、滑らかにするならbilinear
    )

    # 3. MasterでNaNだった場所（ポリゴン外）をSlave側もNaNにする
    # reprojectは矩形全体を埋めるため、ポリゴンマスクを再適用するイメージ
    slave_arr[np.isnan(master_arr)] = np.nan

    return master_arr, slave_arr

class FudeContentScanner:
    def __init__(self, fude_dir):
        self.fude_dir = fude_dir
        self.all_zips = glob.glob(os.path.join(fude_dir, "*.zip"))
        logger.info(f"[FudeLoader] Found {len(self.all_zips)} ZIP files.")

    def _get_candidate_zips(self, lat, lon):
        dists = []
        for code, (plat, plon) in PREF_CENTERS.items():
            d = (lat - plat)**2 + (lon - plon)**2
            dists.append((d, code))
        dists.sort(key=lambda x: x[0])
        top_codes = [code for _, code in dists[:3]]
        
        candidate_files = []
        for zpath in self.all_zips:
            fname = os.path.basename(zpath)
            for code in top_codes:
                if re.search(r'(^|[^0-9])' + str(code) + r'([^0-9]|$)', fname):
                    candidate_files.append(zpath)
                    break
        return candidate_files

    def get_paddy_gdf(self, bounds):
        if not self.all_zips: return None
        west, south, east, north = bounds
        center_lat = (south + north) / 2
        center_lon = (west + east) / 2
        
        target_zips = self._get_candidate_zips(center_lat, center_lon)
        if not target_zips: target_zips = self.all_zips
        
        logger.info(f"  > Grid Center ({center_lat:.4f}, {center_lon:.4f}) -> Scanning {len(target_zips)} ZIPs")

        all_paddy_features = []
        for zip_path in target_zips:
            with tempfile.TemporaryDirectory() as temp_dir:
                try:
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        file_list = [f for f in z.namelist() if f.lower().endswith(('.json', '.geojson'))]
                        if not file_list: continue
                        z.extractall(temp_dir, members=file_list)
                    
                    json_files = []
                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            json_files.append(os.path.join(root, file))
                    
                    for jf in json_files:
                        try:
                            with open(jf, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            features = data.get('features', [])
                            if not features: continue
                            
                            props = features[0].get('properties', {})
                            p_lat, p_lon = props.get('point_lat'), props.get('point_lng')
                            if p_lat is None or p_lon is None: continue
                            
                            if (south - 0.02 <= p_lat <= north + 0.02) and (west - 0.02 <= p_lon <= east + 0.02):
                                for feat in features:
                                    p = feat.get('properties', {})
                                    val = p.get('land_type') or p.get('land_cat') or p.get('chi_moku') or p.get('code') or p.get('fude_type')
                                    if val == 100: all_paddy_features.append(feat)
                        except Exception: continue
                except Exception: continue

        if not all_paddy_features: return None
        
        gdf = gpd.GeoDataFrame.from_features(all_paddy_features)
        if gdf.crs is None: gdf.set_crs("EPSG:6668", inplace=True)
        # 画像とのCRS合わせは解析時に行うため、ここではEPSG:4326にして返す
        if gdf.crs != "EPSG:4326": gdf = gdf.to_crs("EPSG:4326")
        return gdf

def get_road_mask(bounds):
    logger.info("  > Fetching OSM Road network...")
    west, south, east, north = bounds
    try:
        G = ox.graph_from_bbox(bbox=(west, south, east, north), network_type='drive')
        edges = ox.graph_to_gdfs(G, nodes=False)
        if edges.empty: return None
        utm_crs = edges.estimate_utm_crs()
        edges_proj = edges.to_crs(utm_crs)
        buffered = edges_proj.geometry.buffer(ROAD_BUFFER_METER)
        try: road_poly = buffered.union_all()
        except AttributeError: road_poly = buffered.unary_union
        return gpd.GeoDataFrame(geometry=[road_poly], crs=utm_crs).to_crs("EPSG:4326")
    except Exception: return None

def parse_summary(filepath):
    entries = []
    current_entry = {}
    re_delay = re.compile(r"Delay \(Hours\)\s*:\s*([\d\.]+)")
    re_scene = re.compile(r"(After|Before) Scene\s*:\s*([A-Z0-9_]+)")
    if not os.path.exists(filepath): return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines:
            line = line.strip()
            if line.startswith("----"):
                if 'delay' in current_entry and 'after' in current_entry:
                    entries.append(current_entry)
                current_entry = {}
                continue
            d_match = re_delay.search(line)
            if d_match: current_entry['delay'] = float(d_match.group(1))
            s_match = re_scene.search(line)
            if s_match:
                key = 'after' if s_match.group(1) == 'After' else 'before'
                current_entry[key] = s_match.group(2)
        if 'delay' in current_entry and 'after' in current_entry: entries.append(current_entry)
    except Exception: pass
    return entries

def process_grid(grid_id, fude_loader):
    grid_sample_dir = os.path.join(SAMPLES_ROOT, grid_id)
    grid_summary_path = os.path.join(SAFE_ROOT, grid_id, "summary_delay.txt")
    out_dir = os.path.join(RESULT_ROOT, grid_id)
    os.makedirs(out_dir, exist_ok=True)

    entries = parse_summary(grid_summary_path)
    valid_entries = [e for e in entries if e.get('delay', 999) < DELAY_THRESHOLD]
    
    verified_entries = []
    for entry in valid_entries:
        pb = find_proc_file(grid_sample_dir, entry['before'])
        pa = find_proc_file(grid_sample_dir, entry['after'])
        if pb and pa:
            entry['path_before'] = pb
            entry['path_after'] = pa
            verified_entries.append(entry)

    if not verified_entries:
        logger.info(f"[{grid_id}] No valid pairs found. Skipped.")
        return None

    logger.info(f"[{grid_id}] Analyzing {len(verified_entries)} pairs...")
    
    results = []
    road_mask_base = None
    paddy_mask_base = None
    
    # 1. Prepare Base Masks (EPSG:4326)
    try:
        first_entry = verified_entries[0]
        with rasterio.open(first_entry['path_before']) as src:
            img_bounds = src.bounds
            road_mask_base = get_road_mask(img_bounds)
            
            raw_paddy = fude_loader.get_paddy_gdf(img_bounds)
            if raw_paddy is not None:
                utm = raw_paddy.estimate_utm_crs()
                logger.info(f"  > Eroding {len(raw_paddy)} polygons...")
                buffered = raw_paddy.to_crs(utm).geometry.buffer(PADDY_EROSION_METER)
                valid_polys = buffered[~buffered.is_empty]
                if len(valid_polys) > 0:
                    try: paddy_poly = valid_polys.union_all()
                    except AttributeError: paddy_poly = valid_polys.unary_union
                    if not paddy_poly.is_empty:
                        paddy_mask_base = gpd.GeoDataFrame(geometry=[paddy_poly], crs=utm).to_crs("EPSG:4326")
                        logger.info(f"  > Paddy Mask created ({len(valid_polys)} polys merged).")
    except Exception as e:
        logger.error(f"[{grid_id}] Mask prep failed: {e}")
        return None

    # 2. Analysis Loop with Alignment
    for entry in verified_entries:
        try:
            res_row = {
                'grid_id': grid_id,
                'date_before': os.path.basename(entry['path_before']),
                'date_after': os.path.basename(entry['path_after']),
                'delay_hours': entry['delay'],
                'road_ratio': np.nan, 'road_px': 0,
                'paddy_ratio': np.nan, 'paddy_px': 0
            }
            with rasterio.open(entry['path_before']) as src_pre, rasterio.open(entry['path_after']) as src_post:
                
                # Road
                if road_mask_base is not None:
                    try:
                        # 画像のCRSへ強制変換
                        curr_road_mask = road_mask_base.to_crs(src_pre.crs)
                        
                        # ★ 位置合わせとマスク抽出を同時に行う関数を使用
                        arr_pre, arr_post = align_and_mask(src_pre, src_post, curr_road_mask.geometry)
                        
                        diff = arr_post - arr_pre
                        valid = ~np.isnan(diff)
                        total = np.sum(valid)
                        if total > 0:
                            decreased = np.sum((diff < 0) & valid)
                            res_row['road_ratio'] = decreased / total
                            res_row['road_px'] = total
                    except Exception as e:
                        logger.error(f"  [Road Error] {e}")

                # Paddy
                if paddy_mask_base is not None:
                    try:
                        curr_paddy_mask = paddy_mask_base.to_crs(src_pre.crs)
                        
                        # ★ 位置合わせとマスク抽出
                        arr_pre, arr_post = align_and_mask(src_pre, src_post, curr_paddy_mask.geometry)
                        
                        diff = arr_post - arr_pre
                        valid = ~np.isnan(diff)
                        total = np.sum(valid)
                        if total > 0:
                            decreased = np.sum((diff < 0) & valid)
                            res_row['paddy_ratio'] = decreased / total
                            res_row['paddy_px'] = total
                    except Exception as e:
                        logger.error(f"  [Paddy Error] {e}")

            results.append(res_row)
        except Exception as e:
            logger.error(f"  [Error] {e}")

    if results:
        df = pd.DataFrame(results)
        df.to_csv(os.path.join(out_dir, f"{grid_id}_values.csv"), index=False)
        return df
    return None

if __name__ == "__main__":
    if not os.path.exists(RESULT_ROOT): os.makedirs(RESULT_ROOT)
    fude_loader = FudeContentScanner(FUDE_POLYGON_DIR)
    
    if not os.path.exists(SAMPLES_ROOT): exit()
    grid_dirs = [d for d in os.listdir(SAMPLES_ROOT) if os.path.isdir(os.path.join(SAMPLES_ROOT, d))]
    
    all_summaries = []
    for i, grid_id in enumerate(grid_dirs):
        logger.info(f"[{i+1}/{len(grid_dirs)}] Processing {grid_id}")
        df_res = process_grid(grid_id, fude_loader)
        if df_res is not None:
            all_summaries.append({
                'Grid ID': grid_id,
                'Pairs': len(df_res),
                'Avg Road Decrease': df_res['road_ratio'].mean(),
                'Avg Paddy Decrease': df_res['paddy_ratio'].mean(),
                'Road Px': df_res['road_px'].sum(),
                'Paddy Px': df_res['paddy_px'].sum()
            })
            
    md_path = os.path.join(RESULT_ROOT, "all_grids_summary.md")
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(f"# Analysis Summary\nGenerated: {datetime.datetime.now()}\n\n")
        f.write("| Grid ID | Road Dec | Paddy Dec | Road Px | Paddy Px |\n|:---|:---|:---|:---|:---|\n")
        for s in all_summaries:
            f.write(f"| {s['Grid ID']} | {s['Avg Road Decrease']:.2%} | {s['Avg Paddy Decrease']:.2%} | {s['Road Px']} | {s['Paddy Px']} |\n")
    
    logger.info("Done.")