import os
import sys
import re
import math
import warnings
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import geometry_mask
from rasterio.warp import reproject, Resampling
import osmnx as ox
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# 警告の抑制
warnings.filterwarnings("ignore")

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("analysis")

# ==========================================
# 定数・パス設定
# ==========================================
BASE_DIR = Path(r"D:\sotsuron")

# 修正: 余分な "sotsuron" を削除
S1_SAMPLES_DIR = BASE_DIR / "s1_samples" 
S1_SAFE_DIR = BASE_DIR / "s1_safe"
JAXA_DATA_DIR = BASE_DIR / "jaxa-data"

# 修正: 出力先を rainsar-hub 内へ
HUB_DIR = BASE_DIR / "rainsar-hub"
OUTPUT_CSV = HUB_DIR / "analysis_result.csv"
RESULT_DIR = HUB_DIR / "result"

# 対象Grid ID
TARGET_GRIDS = [
    "N03145E13095", "N03285E13005", "N03285E13075", "N03285E13085",
    "N03285E13115", "N03285E13165", "N03295E12995", "N03295E13075",
    "N03295E13185", "N03325E13125", "N03335E13095", "N03355E13085",
    "N03355E13125", "N03375E13065", "N03375E13075", "N03385E13065",
    "N03375E13095"
]

ROAD_BUFFER_METER = 5
LARGE_ROAD_WIDTH_THRESHOLD = 10.0

# ==========================================
# ユーティリティ関数
# ==========================================

def decode_grid_id(grid_id):
    """Grid IDから中心緯度経度を取得"""
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, grid_id)
    if not m:
        return None
    ns, lat_str, ew, lon_str = m.groups()
    lat = float(lat_str) / 100.0
    if ns == 'S': lat = -lat
    lon = float(lon_str) / 100.0
    if ew == 'W': lon = -lon
    return lat, lon

def db_to_linear(db_val):
    """dB値を線形値に変換"""
    return 10 ** (db_val / 10.0)

def get_jaxa_lulc_path(year, lat, lon):
    """対象年と座標に基づいて適切なJAXA土地利用図のパスを返す"""
    lat_int = math.floor(lat)
    lon_int = math.floor(lon)
    filename = f"LC_N{lat_int:02d}E{lon_int:03d}.tif"
    
    if year >= 2022:
        version_dir = "2024JPN_v25.04"
    elif year >= 2020:
        version_dir = "2020JPN_v25.04"
    else:
        version_dir = "2018-2020JPN_v21.11_10m"
        
    path = JAXA_DATA_DIR / version_dir / filename
    
    if not path.exists():
        path_old = JAXA_DATA_DIR / "2018-2020JPN_v21.11_10m" / filename
        if path_old.exists():
            return path_old
            
    return path

def parse_summary_txt(grid_id):
    """summary_delay.txtを解析してイベント情報のリストを返す"""
    summary_path = S1_SAFE_DIR / grid_id / "summary_delay.txt"
    if not summary_path.exists():
        return []

    with open(summary_path, 'r', encoding='utf-8') as f:
        content = f.read()

    events = []
    blocks = content.split('-' * 60)
    
    for block in blocks:
        if "Event Start" not in block:
            continue
            
        data = {}
        start_m = re.search(r"Event Start \(UTC\) : ([\d\- :]+)", block)
        rain_m = re.search(r"Rain Info\s+: (.+)", block)
        delay_m = re.search(r"Delay \(Hours\)\s+: ([\d\.]+)", block)
        after_m = re.search(r"After Scene\s*:? (S1\w+)", block)
        before_m = re.search(r"Before Scene\s*:? (S1\w+)", block)

        if start_m and delay_m and after_m and before_m:
            data['date'] = start_m.group(1).split(' ')[0]
            data['datetime'] = datetime.strptime(start_m.group(1), "%Y-%m-%d %H:%M:%S")
            data['rain_info'] = rain_m.group(1).strip() if rain_m else "N/A"
            data['delay'] = float(delay_m.group(1))
            data['after_scene'] = after_m.group(1)
            data['before_scene'] = before_m.group(1)
            events.append(data)
            
    return events

def get_road_mask(bounds):
    """OSMから道路ネットワークを取得"""
    west, south, east, north = bounds
    try:
        G = ox.graph_from_bbox(bbox=(north, south, east, west), network_type='drive')
        edges = ox.graph_to_gdfs(G, nodes=False)
        if edges.empty: return None

        is_large = pd.Series([False] * len(edges), index=edges.index)
        
        if 'width' in edges.columns:
            def parse_width(w):
                if isinstance(w, list): return float(w[0])
                try: return float(w)
                except: return 0.0
            widths = edges['width'].apply(parse_width)
            is_large |= (widths >= LARGE_ROAD_WIDTH_THRESHOLD)
            
        target_highways = ['motorway', 'trunk', 'primary']
        if 'highway' in edges.columns:
            def check_highway(h):
                val = h[0] if isinstance(h, list) else h
                return val in target_highways
            is_large |= edges['highway'].apply(check_highway)

        target_edges = edges[is_large]
        if target_edges.empty:
            target_edges = edges # Fallback

        utm_crs = target_edges.estimate_utm_crs()
        edges_proj = target_edges.to_crs(utm_crs)
        buffered = edges_proj.geometry.buffer(ROAD_BUFFER_METER)
        
        try: road_poly = buffered.union_all()
        except AttributeError: road_poly = buffered.unary_union
            
        return gpd.GeoDataFrame(geometry=[road_poly], crs=utm_crs).to_crs("EPSG:4326")
    except Exception:
        return None

def analyze_pixel_changes(after_path, before_path, mask_gdf=None, jaxa_path=None, target_type=None):
    """
    AfterとBeforeをピクセルごとに比較し統計量を返す
    """
    if not after_path.exists() or not before_path.exists():
        return None

    try:
        with rasterio.open(after_path) as src_after:
            data_after_db = src_after.read(1)
            nodata = src_after.nodata
            profile = src_after.profile
            transform = src_after.transform
            crs = src_after.crs
            shape = src_after.shape

        with rasterio.open(before_path) as src_before:
            data_before_db = np.zeros(shape, dtype=src_before.dtypes[0])
            reproject(
                source=rasterio.band(src_before, 1),
                destination=data_before_db,
                src_transform=src_before.transform,
                src_crs=src_before.crs,
                dst_transform=transform,
                dst_crs=crs,
                resampling=Resampling.nearest
            )

        valid_mask = ~np.isnan(data_after_db) & ~np.isnan(data_before_db)
        if nodata is not None:
            valid_mask &= (data_after_db != nodata) & (data_before_db != nodata)

        target_mask = np.zeros(shape, dtype=bool)

        if target_type == 'road' and mask_gdf is not None:
            shapes = mask_gdf.geometry.values
            rasterized_road = geometry_mask(shapes, transform=transform, invert=True, out_shape=shape)
            target_mask = rasterized_road

        elif target_type == 'paddy' and jaxa_path is not None and jaxa_path.exists():
            with rasterio.open(jaxa_path) as jaxa_src:
                jaxa_reprojected = np.zeros(shape, dtype=jaxa_src.dtypes[0])
                reproject(
                    source=rasterio.band(jaxa_src, 1),
                    destination=jaxa_reprojected,
                    src_transform=jaxa_src.transform,
                    src_crs=jaxa_src.crs,
                    dst_transform=transform,
                    dst_crs=crs,
                    resampling=Resampling.nearest
                )
                target_mask = (jaxa_reprojected == 3) # 3: Paddy

        final_mask = valid_mask & target_mask
        
        if not np.any(final_mask):
            return {
                'count': 0, 'ratio': 0.0, 
                'mean_dec_after': np.nan, 'mean_dec_before': np.nan,
                'mean_dec_diff': np.nan
            }

        vals_after_lin = db_to_linear(data_after_db[final_mask])
        vals_before_lin = db_to_linear(data_before_db[final_mask])

        decrease_idx = vals_after_lin < vals_before_lin
        
        count_decrease = np.sum(decrease_idx)
        total_pixels = len(vals_after_lin)
        ratio_decrease = (count_decrease / total_pixels) * 100 if total_pixels > 0 else 0

        if count_decrease > 0:
            dec_after_vals = vals_after_lin[decrease_idx]
            dec_before_vals = vals_before_lin[decrease_idx]
            
            mean_dec_after = np.mean(dec_after_vals)
            mean_dec_before = np.mean(dec_before_vals)
            mean_dec_diff = np.mean(dec_after_vals - dec_before_vals) # usually negative
        else:
            mean_dec_after = np.nan
            mean_dec_before = np.nan
            mean_dec_diff = np.nan

        return {
            'count': count_decrease,
            'ratio': ratio_decrease,
            'mean_dec_after': mean_dec_after,
            'mean_dec_before': mean_dec_before,
            'mean_dec_diff': mean_dec_diff
        }

    except Exception as e:
        logger.error(f"Error in pixel analysis: {e}")
        return None

# ==========================================
# グラフ作成機能
# ==========================================

def plot_analysis_results(df, output_dir):
    """解析結果のDataFrameを用いてグラフを作成・保存する"""
    if df.empty:
        return

    # 日付型への変換 (念のため)
    df['date'] = pd.to_datetime(df['date'])
    
    # プロット共通設定
    plt.style.use('ggplot')
    
    # --------------------------------------------------
    # 1. Delay vs Difference (Road & Paddy)
    # --------------------------------------------------
    plt.figure(figsize=(10, 6))
    plt.scatter(df['delay'], df['paddy_dec_mean_diff'], label='Paddy Field', alpha=0.7, s=50, c='green')
    plt.scatter(df['delay'], df['road_dec_mean_diff'], label='Road', alpha=0.7, s=50, c='gray', marker='^')
    
    plt.title("Relationship between Rain Delay and Backscatter Difference\n(Decreased Pixels Only)")
    plt.xlabel("Delay after Rainfall (hours)")
    plt.ylabel("Mean Difference (Linear: After - Before)")
    plt.axhline(0, color='black', linestyle='--', linewidth=0.8)
    plt.legend()
    plt.grid(True)
    
    out_path1 = output_dir / "plot_delay_vs_diff.png"
    plt.savefig(out_path1, dpi=300)
    plt.close()
    logger.info(f"  > Saved graph: {out_path1.name}")

    # --------------------------------------------------
    # 2. Delay vs Decreased Ratio
    # --------------------------------------------------
    plt.figure(figsize=(10, 6))
    plt.scatter(df['delay'], df['paddy_dec_ratio'], label='Paddy Field', alpha=0.7, s=50, c='green')
    plt.scatter(df['delay'], df['road_dec_ratio'], label='Road', alpha=0.7, s=50, c='gray', marker='^')
    
    plt.title("Relationship between Rain Delay and Decreased Pixel Ratio")
    plt.xlabel("Delay after Rainfall (hours)")
    plt.ylabel("Ratio of Pixels with Decreased Intensity (%)")
    plt.ylim(0, 105)
    plt.legend()
    plt.grid(True)
    
    out_path2 = output_dir / "plot_delay_vs_ratio.png"
    plt.savefig(out_path2, dpi=300)
    plt.close()
    logger.info(f"  > Saved graph: {out_path2.name}")

    # --------------------------------------------------
    # 3. Time Series: Date vs Difference
    # --------------------------------------------------
    plt.figure(figsize=(12, 6))
    plt.scatter(df['date'], df['paddy_dec_mean_diff'], label='Paddy Field', c='green', alpha=0.6)
    plt.scatter(df['date'], df['road_dec_mean_diff'], label='Road', c='gray', marker='^', alpha=0.6)
    
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.gcf().autofmt_xdate()
    
    plt.title("Time Series of Backscatter Difference")
    plt.xlabel("Date")
    plt.ylabel("Mean Difference (Linear)")
    plt.legend()
    plt.grid(True)
    
    out_path3 = output_dir / "plot_timeseries_diff.png"
    plt.savefig(out_path3, dpi=300)
    plt.close()
    logger.info(f"  > Saved graph: {out_path3.name}")


# ==========================================
# メイン処理
# ==========================================

def main():
    logger.info("Starting Analysis with Graphs...")
    
    # 出力ディレクトリ作成
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    
    total_grids = len(TARGET_GRIDS)
    all_results = []
    
    for i, grid_id in enumerate(TARGET_GRIDS):
        progress_pct = (i / total_grids) * 100
        logger.info(f"[Progress] {progress_pct:.1f}% - Processing Grid {i+1}/{total_grids}: {grid_id}")
        
        events = parse_summary_txt(grid_id)
        if not events:
            continue

        lat, lon = decode_grid_id(grid_id)
        grid_dir = S1_SAMPLES_DIR / grid_id
        
        # OSMロード
        sample_tif = list(grid_dir.glob("*.tif"))
        road_mask = None
        if sample_tif:
            try:
                with rasterio.open(sample_tif[0]) as src:
                    road_mask = get_road_mask(src.bounds)
            except: pass

        for evt in events:
            evt_year = evt['datetime'].year
            after_file = grid_dir / f"{evt['after_scene']}_proc.tif"
            before_file = grid_dir / f"{evt['before_scene']}_proc.tif"
            jaxa_path = get_jaxa_lulc_path(evt_year, lat, lon)

            road_stats = analyze_pixel_changes(
                after_file, before_file, mask_gdf=road_mask, target_type='road'
            )
            paddy_stats = analyze_pixel_changes(
                after_file, before_file, jaxa_path=jaxa_path, target_type='paddy'
            )

            if road_stats and paddy_stats:
                record = {
                    'grid_id': grid_id,
                    'date': evt['date'],
                    'rain_info': evt['rain_info'],
                    'delay': evt['delay'],
                    # Road
                    'road_dec_count': road_stats['count'],
                    'road_dec_ratio': road_stats['ratio'],
                    'road_dec_mean_after': road_stats['mean_dec_after'],
                    'road_dec_mean_before': road_stats['mean_dec_before'],
                    'road_dec_mean_diff': road_stats['mean_dec_diff'],
                    # Paddy
                    'paddy_dec_count': paddy_stats['count'],
                    'paddy_dec_ratio': paddy_stats['ratio'],
                    'paddy_dec_mean_after': paddy_stats['mean_dec_after'],
                    'paddy_dec_mean_before': paddy_stats['mean_dec_before'],
                    'paddy_dec_mean_diff': paddy_stats['mean_dec_diff'],
                }
                all_results.append(record)
            
            logger.debug(f"  > Event {evt['date']} processed.")

    logger.info("[Progress] 100% - Finalizing comparisons and graphs...")
    
    if all_results:
        df = pd.DataFrame(all_results)
        
        # Short vs Long 比較計算
        final_df_list = []
        for g_id, group in df.groupby('grid_id'):
            group = group.sort_values('delay')
            comp_val = np.nan
            if len(group) >= 2:
                short_val = group.iloc[0]['paddy_dec_ratio']
                long_val = group.iloc[-1]['paddy_dec_ratio']
                comp_val = short_val - long_val
            group['comp_paddy_ratio_short_minus_long'] = comp_val
            final_df_list.append(group)
        
        final_df = pd.concat(final_df_list)
        
        # CSV 保存
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        logger.info(f"Results saved to {OUTPUT_CSV}")
        
        # グラフ作成
        plot_analysis_results(final_df, RESULT_DIR)
        
    else:
        logger.warning("No results found.")

if __name__ == "__main__":
    main()