import os
import re
import glob
import zipfile
import tempfile
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

# 解析パラメータ
TRACKING_DELAY_LIMIT = 2.0
SIGNIFICANT_THRESHOLD_DB = -1.0 

# マスク生成用パラメータ
PADDY_EROSION_METER = -2.0
ROAD_BUFFER_METER = 2.5   # 幅5m想定
AIRPORT_BUFFER_METER = 5.0 # 滑走路が線データの場合の保険（幅10m）

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
        logging.FileHandler(os.path.join(BASE_DIR, "analysis_multi_log.txt"), encoding='utf-8')
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

def align_to_ref(src_target, src_ref):
    dest_arr = np.empty((src_ref.height, src_ref.width), dtype=src_target.dtypes[0])
    reproject(
        source=rasterio.band(src_target, 1),
        destination=dest_arr,
        src_transform=src_target.transform,
        src_crs=src_target.crs,
        dst_transform=src_ref.transform,
        dst_crs=src_ref.crs,
        resampling=Resampling.nearest
    )
    return dest_arr

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
        center_lat, center_lon = (south + north) / 2, (west + east) / 2
        target_zips = self._get_candidate_zips(center_lat, center_lon)
        if not target_zips: target_zips = self.all_zips
        
        all_paddy_features = []
        for zip_path in target_zips:
            with tempfile.TemporaryDirectory() as temp_dir:
                try:
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        file_list = [f for f in z.namelist() if f.lower().endswith(('.json', '.geojson'))]
                        if not file_list: continue
                        z.extractall(temp_dir, members=file_list)
                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            try:
                                with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
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
                            except: continue
                except: continue

        if not all_paddy_features: return None
        gdf = gpd.GeoDataFrame.from_features(all_paddy_features)
        if gdf.crs is None: gdf.set_crs("EPSG:6668", inplace=True)
        if gdf.crs != "EPSG:4326": gdf = gdf.to_crs("EPSG:4326")
        return gdf

def get_road_mask(bounds):
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

# --- 新追加: OSM地物取得関数 (公園・空港用) ---
def get_osm_features(bounds, tags, buffer_meter=0.0):
    west, south, east, north = bounds
    try:
        # features_from_bbox でポリゴン等を取得
        gdf = ox.features_from_bbox(bbox=(west, south, east, north), tags=tags)
        if gdf.empty: return None
        
        # ポリゴンまたはマルチポリゴンのみ抽出 (LineStringも混じる可能性あり)
        # 飛行場などはLineStringの場合もあるため、全て残してバッファで処理する方針
        
        utm_crs = gdf.estimate_utm_crs()
        gdf_proj = gdf.to_crs(utm_crs)
        
        # バッファ処理 (Lineを面にする、またはポリゴンを広げる)
        if buffer_meter > 0:
            gdf_proj.geometry = gdf_proj.geometry.buffer(buffer_meter)
            
        try: merged = gdf_proj.union_all()
        except AttributeError: merged = gdf_proj.unary_union
        
        if merged.is_empty: return None
        
        return gpd.GeoDataFrame(geometry=[merged], crs=utm_crs).to_crs("EPSG:4326")
        
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
    
    verified_entries = []
    for entry in entries:
        pb = find_proc_file(grid_sample_dir, entry['before'])
        pa = find_proc_file(grid_sample_dir, entry['after'])
        if pb and pa:
            entry['path_before'] = pb
            entry['path_after'] = pa
            verified_entries.append(entry)

    if not verified_entries: return None
    ref_path = verified_entries[0]['path_before']
    logger.info(f"[{grid_id}] Analyzing...")

    # マスク変数の初期化
    masks = {
        'road': None, 'paddy': None, 'park': None, 'airport': None
    }
    
    # ----------------------------------------------------
    # Phase 0: マスク作成
    # ----------------------------------------------------
    try:
        with rasterio.open(ref_path) as src_ref:
            img_bounds = src_ref.bounds
            
            # 1. Road
            road_gdf = get_road_mask(img_bounds)
            if road_gdf is not None:
                road_aligned = road_gdf.to_crs(src_ref.crs)
                m, _ = mask(src_ref, road_aligned.geometry, crop=False, nodata=0, all_touched=True)
                masks['road'] = (m[0] > 0)

            # 2. Paddy
            paddy_gdf = fude_loader.get_paddy_gdf(img_bounds)
            if paddy_gdf is not None:
                utm = paddy_gdf.estimate_utm_crs()
                buffered = paddy_gdf.to_crs(utm).geometry.buffer(PADDY_EROSION_METER)
                valid = buffered[~buffered.is_empty]
                if len(valid) > 0:
                    try: poly = valid.union_all()
                    except: poly = valid.unary_union
                    paddy_base = gpd.GeoDataFrame(geometry=[poly], crs=utm).to_crs(src_ref.crs)
                    m, _ = mask(src_ref, paddy_base.geometry, crop=False, nodata=0, all_touched=True)
                    masks['paddy'] = (m[0] > 0)

            # 3. Park (OSM: leisure=park, landuse=grass etc)
            tags_park = {'leisure': ['park', 'playground', 'pitch'], 'landuse': ['grass', 'recreation_ground']}
            park_gdf = get_osm_features(img_bounds, tags_park, buffer_meter=0.0)
            if park_gdf is not None:
                park_aligned = park_gdf.to_crs(src_ref.crs)
                m, _ = mask(src_ref, park_aligned.geometry, crop=False, nodata=0, all_touched=True)
                masks['park'] = (m[0] > 0)
                logger.info(f"  > Park found.")

            # 4. Airport (OSM: aeroway=runway/apron)
            tags_airport = {'aeroway': ['runway', 'apron', 'taxiway']}
            airport_gdf = get_osm_features(img_bounds, tags_airport, buffer_meter=AIRPORT_BUFFER_METER)
            if airport_gdf is not None:
                airport_aligned = airport_gdf.to_crs(src_ref.crs)
                m, _ = mask(src_ref, airport_aligned.geometry, crop=False, nodata=0, all_touched=True)
                masks['airport'] = (m[0] > 0)
                logger.info(f"  > Airport found.")

    except Exception as e:
        logger.error(f"[{grid_id}] Mask gen error: {e}")
        return None

    # ----------------------------------------------------
    # Phase 1: トラッキングターゲットの特定 (Delay < 2h)
    # ----------------------------------------------------
    short_delay_entries = [e for e in verified_entries if e.get('delay', 999) < TRACKING_DELAY_LIMIT]
    
    target_masks = {k: None for k in masks.keys()} # road_target, paddy_target...
    has_target = False
    
    with rasterio.open(ref_path) as src_ref:
        # 初期化
        for k in masks:
            target_masks[k] = np.zeros((src_ref.height, src_ref.width), dtype=bool)

        for entry in short_delay_entries:
            try:
                with rasterio.open(entry['path_before']) as src_pre, rasterio.open(entry['path_after']) as src_post:
                    arr_pre = align_to_ref(src_pre, src_ref)
                    arr_post = align_to_ref(src_post, src_ref)
                    diff = arr_post - arr_pre
                    
                    # 減少ピクセルの特定
                    sig_dec = (diff < SIGNIFICANT_THRESHOLD_DB)
                    
                    # 各マスクとのANDをとってターゲットに追加
                    for k, base_mask in masks.items():
                        if base_mask is not None:
                            hit = sig_dec & base_mask
                            target_masks[k] |= hit
                            if np.sum(hit) > 0: has_target = True
            except: pass

    if not has_target:
        logger.info(f"[{grid_id}] No tracking targets found.")
        return None

    # 各ターゲットのピクセル数をログ
    for k, m in target_masks.items():
        logger.info(f"  > Target {k}: {np.sum(m)} px")

    # ----------------------------------------------------
    # Phase 2: 追跡解析
    # ----------------------------------------------------
    results = []
    with rasterio.open(ref_path) as src_ref:
        for entry in verified_entries:
            try:
                res_row = {
                    'grid_id': grid_id, 'delay_hours': entry['delay'],
                    'road_tracked_mean': np.nan, 'paddy_tracked_mean': np.nan,
                    'park_tracked_mean': np.nan, 'airport_tracked_mean': np.nan
                }
                
                with rasterio.open(entry['path_before']) as src_pre, rasterio.open(entry['path_after']) as src_post:
                    arr_pre = align_to_ref(src_pre, src_ref)
                    arr_post = align_to_ref(src_post, src_ref)
                    diff = arr_post - arr_pre
                    
                    # 各地物について平均変化量を計算
                    for k in masks.keys():
                        tgt = target_masks[k]
                        if np.sum(tgt) > 0:
                            vals = diff[tgt]
                            vals = vals[~np.isnan(vals)]
                            if len(vals) > 0:
                                res_row[f'{k}_tracked_mean'] = np.mean(vals)
                
                results.append(res_row)
            except: pass

    if results:
        df = pd.DataFrame(results)
        df.to_csv(os.path.join(out_dir, f"{grid_id}_multi_tracked.csv"), index=False)
        return df
    return None

if __name__ == "__main__":
    if not os.path.exists(RESULT_ROOT): os.makedirs(RESULT_ROOT)
    fude_loader = FudeContentScanner(FUDE_POLYGON_DIR)
    
    if not os.path.exists(SAMPLES_ROOT): exit()
    grid_dirs = [d for d in os.listdir(SAMPLES_ROOT) if os.path.isdir(os.path.join(SAMPLES_ROOT, d))]
    
    for i, grid_id in enumerate(grid_dirs):
        logger.info(f"[{i+1}/{len(grid_dirs)}] Processing {grid_id}")
        process_grid(grid_id, fude_loader)
            
    logger.info("Done.")