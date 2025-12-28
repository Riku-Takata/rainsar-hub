import os
import sys
import re
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm
import numpy as np
import pandas as pd
import rasterio
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.font_manager as fm

def sigmaclip(a, low=4.0, high=4.0):
    """
    SciPy's sigmaclip implementation using only NumPy.
    Iterative sigma-clipping of array elements.
    """
    c = np.asarray(a).ravel()
    delta = 1
    while delta:
        c_std = c.std()
        c_mean = c.mean()
        size = c.size
        critlower = c_mean - c_std * low
        critupper = c_mean + c_std * high
        c = c[(c >= critlower) & (c <= critupper)]
        delta = size - c.size
    return c, critlower, critupper

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("sigma_clipped_analysis")

# ==========================================
# 設定
# ==========================================
BASE_DIR = Path(r"D:\sotsuron")
S1_SAMPLES_DIR = BASE_DIR / "s1_samples"
S1_SAFE_DIR = BASE_DIR / "s1_safe"
RESULT_DIR = BASE_DIR / "rainsar-hub" / "result" / "sigma_clipped_stats"

# 対象Grid
TARGET_GRIDS = [
    "N03145E13095", "N03285E13005", "N03285E13075", "N03285E13085",
    "N03285E13115", "N03285E13165", "N03295E12995", "N03295E13075",
    "N03295E13185", "N03325E13125", "N03335E13095", "N03355E13085",
    "N03355E13125", "N03375E13065", "N03375E13075", "N03385E13065",
    "N03375E13095"
]

SUFFIX_HIGHWAY = "_highway_mask.tif"
SUFFIX_PADDY = "_paddy_mask.tif"

SIGMA_LOW = 3.0
SIGMA_HIGH = 3.0

# ==========================================
# 関数
# ==========================================

def configure_plotting():
    """グラフ描画の設定（日本語フォント対応）"""
    sns.set_style("whitegrid")
    target_fonts = ['MS Gothic', 'Meiryo', 'Yu Gothic', 'SimHei', 'Arial Unicode MS']
    found_font = None
    for font_name in target_fonts:
        try:
            if fm.findfont(font_name, fallback_to_default=False) != fm.findfont("NonExistentFont"):
                found_font = font_name
                break
        except:
            continue
    if found_font:
        plt.rcParams['font.family'] = found_font
        plt.rcParams['axes.unicode_minus'] = False
    else:
        plt.rcParams['font.family'] = 'sans-serif'

def parse_summary_txt(grid_id):
    summary_path = S1_SAFE_DIR / grid_id / "summary_delay.txt"
    if not summary_path.exists(): return []

    with open(summary_path, 'r', encoding='utf-8') as f:
        content = f.read()

    events = []
    blocks = content.split('-' * 60)
    for block in blocks:
        if "Event Start" not in block: continue
        data = {}
        start_m = re.search(r"Event Start \(UTC\) : ([\d\- :]+)", block)
        after_m = re.search(r"After Scene\s*:? (S1\w+)", block)
        before_m = re.search(r"Before Scene\s*:? (S1\w+)", block)
        
        if start_m and after_m and before_m:
            data['date'] = start_m.group(1).split(' ')[0]
            data['after_scene'] = after_m.group(1)
            data['before_scene'] = before_m.group(1)
            events.append(data)
    return events

def read_linear_values(tif_path):
    if not tif_path.exists(): return None
    with rasterio.open(tif_path) as src:
        data = src.read(1)
        valid_mask = ~np.isnan(data)
        if not np.any(valid_mask): return None
        valid_data_db = data[valid_mask]
        valid_data_linear = 10 ** (valid_data_db / 10.0)
        return valid_data_linear

def calculate_sigma_clipped_stats(data, low=3.0, high=3.0):
    """シグマクリッピングを行い、統計量を返す"""
    if len(data) == 0:
        return None
    
    # scipy.stats.sigmaclip は (clipped_data, lower_bound, upper_bound) を返す
    # デフォルトは反復的にクリッピングを行う
    c, low_bound, high_bound = sigmaclip(data, low, high)
    
    return {
        'original_count': len(data),
        'clipped_count': len(c),
        'mean': np.mean(c),
        'median': np.median(c),
        'std': np.std(c),
        'min': np.min(c),
        'max': np.max(c),
        'discarded_ratio': 1.0 - (len(c) / len(data))
    }

def main():
    logger.info("Starting Sigma-clipped Mean Analysis...")
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    configure_plotting()

    for grid_id in TARGET_GRIDS:
        events = parse_summary_txt(grid_id)
        if not events:
            continue
        
        logger.info(f"Processing Grid: {grid_id} ...")
        
        grid_stats = []
        
        grid_dir = S1_SAMPLES_DIR / grid_id
        
        for evt in events:
            stem_after = f"{evt['after_scene']}_proc"
            stem_before = f"{evt['before_scene']}_proc"
            
            # 処理対象: 道路と田んぼ
            targets = [
                ('道路', SUFFIX_HIGHWAY),
                ('田んぼ', SUFFIX_PADDY)
            ]
            
            for type_name, suffix in targets:
                path_after = grid_dir / f"{stem_after}{suffix}"
                path_before = grid_dir / f"{stem_before}{suffix}"
                
                arr_a = read_linear_values(path_after)
                arr_b = read_linear_values(path_before)
                
                if arr_a is not None and arr_b is not None:
                    min_len = min(len(arr_a), len(arr_b))
                    arr_a = arr_a[:min_len]
                    arr_b = arr_b[:min_len]
                    
                    # 差分 (After - Before)
                    diff = arr_a - arr_b
                    
                    # シグマクリッピング統計量の計算
                    stats_res = calculate_sigma_clipped_stats(diff, SIGMA_LOW, SIGMA_HIGH)
                    
                    if stats_res:
                        row = {
                            'grid_id': grid_id,
                            'date': evt['date'],
                            'type': type_name,
                            **stats_res
                        }
                        grid_stats.append(row)

        if grid_stats:
            df_stats = pd.DataFrame(grid_stats)
            
            # Gridごとの結果保存
            grid_out_dir = RESULT_DIR / grid_id
            grid_out_dir.mkdir(parents=True, exist_ok=True)
            
            csv_path = grid_out_dir / "sigma_clipped_stats.csv"
            df_stats.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            # 簡単な可視化 (Mean vs Median)
            plt.figure(figsize=(10, 6))
            sns.scatterplot(data=df_stats, x='mean', y='median', hue='type')
            plt.plot([-0.1, 0.1], [-0.1, 0.1], 'k--', alpha=0.5) # y=x line
            plt.title(f"Sigma-clipped Mean vs Median (Grid: {grid_id})")
            plt.xlabel("Sigma-clipped Mean")
            plt.ylabel("Sigma-clipped Median")
            plt.tight_layout()
            plt.savefig(grid_out_dir / "mean_vs_median.png", dpi=300)
            plt.close()
            
            logger.info(f"  > Saved stats for {grid_id}")

    logger.info("All grids processed.")

if __name__ == "__main__":
    main()
