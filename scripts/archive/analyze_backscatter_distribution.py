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

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("distribution_analysis")

# ==========================================
# 設定
# ==========================================
BASE_DIR = Path(r"D:\sotsuron")
S1_SAMPLES_DIR = BASE_DIR / "s1_samples"
S1_SAFE_DIR = BASE_DIR / "s1_safe"
RESULT_DIR = BASE_DIR / "rainsar-hub" / "result" / "distributions"

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
    """summary_delay.txtからイベント情報を取得"""
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
        delay_m = re.search(r"Delay \(Hours\)\s+: ([\d\.]+)", block)

        if start_m and after_m and before_m:
            data['date'] = start_m.group(1).split(' ')[0]
            data['after_scene'] = after_m.group(1)
            data['before_scene'] = before_m.group(1)
            # Delayをフォーマット済み文字列として保持 (例: "5.35")
            raw_delay = float(delay_m.group(1)) if delay_m else 0
            data['delay_str'] = f"{raw_delay:.2f}"
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

def plot_for_single_event(df_diff, df_single, output_dir, grid_id, delay_str):
    """
    1つのイベントに対してグラフを作成して保存
    """
    # ---------------------------------------------------------
    # 1. 差分のヒストグラム
    # ---------------------------------------------------------
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df_diff, x='diff_val', hue='type', element="step", stat="density", common_norm=False, bins=100, alpha=0.5)
    plt.title(f"後方散乱強度 差分の分布 (After - Before) [線形値]\nGrid: {grid_id} / Delay: {delay_str}h")
    plt.xlabel("差分値 (Linear)")
    plt.ylabel("密度")
    plt.xlim(-0.3, 0.3)
    plt.tight_layout()
    plt.savefig(output_dir / "01_histogram_difference.png", dpi=300)
    plt.close()

    # ---------------------------------------------------------
    # 2. 差分の箱ひげ図
    # ---------------------------------------------------------
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=df_diff, x='type', y='diff_val', showfliers=False)
    plt.title(f"後方散乱強度 差分の箱ひげ図\nGrid: {grid_id} / Delay: {delay_str}h")
    plt.xlabel("対象領域")
    plt.ylabel("差分値 (Linear)")
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(output_dir / "02_boxplot_difference.png", dpi=300)
    plt.close()

    # ---------------------------------------------------------
    # 3. 単画像のヒストグラム (まとめて表示)
    # ---------------------------------------------------------
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df_single, x='val', hue='timing', element="step", stat="density", common_norm=False, bins=100, alpha=0.3)
    plt.title(f"単画像の後方散乱強度分布 (道路・田んぼ合算)\nGrid: {grid_id} / Delay: {delay_str}h")
    plt.xlabel("後方散乱強度 [線形値]")
    plt.ylabel("密度")
    plt.xlim(0, 0.5)
    plt.tight_layout()
    plt.savefig(output_dir / "03_histogram_single_all.png", dpi=300)
    plt.close()

    # ---------------------------------------------------------
    # 4. 単画像の箱ひげ図
    # ---------------------------------------------------------
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df_single, x='type', y='val', hue='timing', showfliers=False)
    plt.title(f"単画像の後方散乱強度 箱ひげ図\nGrid: {grid_id} / Delay: {delay_str}h")
    plt.xlabel("対象領域")
    plt.ylabel("後方散乱強度 [線形値]")
    plt.legend(title="時期")
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(output_dir / "04_boxplot_single.png", dpi=300)
    plt.close()

def main():
    logger.info("Starting Backscatter Analysis (Per Event/Delay)...")
    
    # 描画設定
    configure_plotting()

    for grid_id in TARGET_GRIDS:
        events = parse_summary_txt(grid_id)
        if not events:
            continue
        
        logger.info(f"Processing Grid: {grid_id} ({len(events)} events) ...")
        grid_dir = S1_SAMPLES_DIR / grid_id
        
        # Gridフォルダ作成
        grid_result_root = RESULT_DIR / grid_id
        grid_result_root.mkdir(parents=True, exist_ok=True)

        # イベントごとにループ
        for evt in events:
            delay_str = evt['delay_str']
            logger.info(f"  > Event Delay: {delay_str}h (Date: {evt['date']})")
            
            # イベントごとの出力フォルダ作成: {delay}h
            event_out_dir = grid_result_root / f"{delay_str}h"
            event_out_dir.mkdir(parents=True, exist_ok=True)

            # このイベント用のデータリスト
            event_diff_data = []
            event_single_data = []
            has_data = False

            stem_after = f"{evt['after_scene']}_proc"
            stem_before = f"{evt['before_scene']}_proc"

            # --- 道路 ---
            path_after_road = grid_dir / f"{stem_after}{SUFFIX_HIGHWAY}"
            path_before_road = grid_dir / f"{stem_before}{SUFFIX_HIGHWAY}"
            
            arr_a_road = read_linear_values(path_after_road)
            arr_b_road = read_linear_values(path_before_road)

            if arr_a_road is not None and arr_b_road is not None:
                min_len = min(len(arr_a_road), len(arr_b_road))
                arr_a_road = arr_a_road[:min_len]
                arr_b_road = arr_b_road[:min_len]
                diff_road = arr_a_road - arr_b_road
                
                event_diff_data.extend([{'type': '道路', 'diff_val': v} for v in diff_road])
                event_single_data.extend([{'type': '道路', 'timing': 'After', 'val': v} for v in arr_a_road])
                event_single_data.extend([{'type': '道路', 'timing': 'Before', 'val': v} for v in arr_b_road])
                has_data = True

            # --- 田んぼ ---
            path_after_paddy = grid_dir / f"{stem_after}{SUFFIX_PADDY}"
            path_before_paddy = grid_dir / f"{stem_before}{SUFFIX_PADDY}"
            
            arr_a_paddy = read_linear_values(path_after_paddy)
            arr_b_paddy = read_linear_values(path_before_paddy)

            if arr_a_paddy is not None and arr_b_paddy is not None:
                min_len = min(len(arr_a_paddy), len(arr_b_paddy))
                arr_a_paddy = arr_a_paddy[:min_len]
                arr_b_paddy = arr_b_paddy[:min_len]
                diff_paddy = arr_a_paddy - arr_b_paddy
                
                event_diff_data.extend([{'type': '田んぼ', 'diff_val': v} for v in diff_paddy])
                event_single_data.extend([{'type': '田んぼ', 'timing': 'After', 'val': v} for v in arr_a_paddy])
                event_single_data.extend([{'type': '田んぼ', 'timing': 'Before', 'val': v} for v in arr_b_paddy])
                has_data = True

            # このイベントのグラフ作成と統計量保存
            if has_data:
                df_diff = pd.DataFrame(event_diff_data)
                df_single = pd.DataFrame(event_single_data)
                
                # グラフ描画
                plot_for_single_event(df_diff, df_single, event_out_dir, grid_id, delay_str)
                
                # 統計量CSV保存
                df_diff.groupby('type')['diff_val'].describe().to_csv(event_out_dir / "stats_difference.csv", encoding='utf-8-sig')
                df_single.groupby(['type', 'timing'])['val'].describe().to_csv(event_out_dir / "stats_single.csv", encoding='utf-8-sig')
                
                logger.info(f"    Saved results to {event_out_dir.name}")
            else:
                logger.warning(f"    No valid masked data for event {delay_str}h")

    logger.info("All processing completed.")

if __name__ == "__main__":
    main()