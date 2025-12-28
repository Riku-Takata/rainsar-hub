import os
import re
import sys
import logging
import math
from pathlib import Path
import numpy as np
import pandas as pd
import rasterio

# --- Logging Setup ---
def setup_logger(name, log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    
    # Stream Handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    
    # File Handler
    if log_file:
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
    return logger

# --- Path Settings ---
BASE_DIR = Path(r"D:\sotsuron")
S1_SAMPLES_DIR = BASE_DIR / "s1_samples"
S1_SAFE_DIR = BASE_DIR / "s1_safe"
HUB_DIR = BASE_DIR / "rainsar-hub"
RESULT_DIR = HUB_DIR / "result"

TARGET_GRIDS = [
    "N03145E13095", "N03285E13005", "N03285E13075", "N03285E13085",
    "N03285E13115", "N03285E13165", "N03295E12995", "N03295E13075",
    "N03295E13185", "N03325E13125", "N03335E13095", "N03355E13085",
    "N03355E13125", "N03375E13065", "N03375E13075", "N03385E13065",
    "N03375E13095"
]

# --- Common Functions ---

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

        if start_m and after_m and before_m:
            data['date'] = start_m.group(1).split(' ')[0]
            # data['datetime'] = datetime.strptime(start_m.group(1), "%Y-%m-%d %H:%M:%S")
            data['rain_info'] = rain_m.group(1).strip() if rain_m else "N/A"
            data['delay'] = float(delay_m.group(1)) if delay_m else None
            data['after_scene'] = after_m.group(1)
            data['before_scene'] = before_m.group(1)
            events.append(data)
            
    return events

def read_linear_values(tif_path):
    """GeoTIFFを読み込み、リニア値に変換して返す"""
    if not tif_path.exists(): return None
    with rasterio.open(tif_path) as src:
        data = src.read(1)
        valid_mask = ~np.isnan(data)
        if not np.any(valid_mask): return None
        valid_data_db = data[valid_mask]
        valid_data_linear = 10 ** (valid_data_db / 10.0)
        return valid_data_linear

def db_to_linear(db_val):
    """dB値を線形値に変換"""
    return 10 ** (db_val / 10.0)

def linear_to_db(linear_val):
    """線形値をdB値に変換"""
    return 10 * np.log10(linear_val)
