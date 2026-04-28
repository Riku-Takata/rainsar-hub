"""
後方散乱強度 詳細分析スクリプト

統計データ（CSV）とデータベース（降雨情報・日時）を結合し、
1. 季節変化（月別分布）
2. 降雨強度との相関
3. 道路・田んぼの比較
を分析・可視化する。

Usage:
    python analyze_sigma_detail.py --polarization vv
    python analyze_sigma_detail.py --polarization vh
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
from dotenv import load_dotenv
import os
import logging

# Setup
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
BACKEND_DIR = BASE_DIR / "backend"

sys.path.append(str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = 3306
    if db_host == "rainsarhub-db":
        db_host = "127.0.0.1"
        db_port = 3307

    logger.info(f"Connecting to DB at {db_host}:{db_port}...")
    return mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "password"),
        database=os.getenv("DB_NAME", "rainsar_hub")
    )

from concurrent.futures import ThreadPoolExecutor

def load_all_statistics(pol):
    """全てのイベントのstatistics.csvを読み込んで結合する（RawとClean両方を含む）"""
    sigma_dir = DATA_DIR / "result" / pol / "sigma"
    if not sigma_dir.exists():
        logger.error(f"Directory not found: {sigma_dir}")
        return None
    
    csv_files = list(sigma_dir.glob("*/*/statistics.csv"))
    logger.info(f"Found {len(csv_files)} statistics files.")
    
    if not csv_files:
        return None

    def read_csv(file_path):
        try:
            df = pd.read_csv(file_path)
            # パスからメタデータを抽出
            parts = file_path.parts
            event_id = parts[-2]
            grid_id = parts[-3]
            df['grid_id'] = grid_id
            df['event_id'] = event_id
            return df
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")
            return None

    # 並列処理で読み込み
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(read_csv, csv_files))
    
    # 結合
    combined_df = pd.concat([df for df in dfs if df is not None], ignore_index=True)
    logger.info(f"Loaded {len(combined_df)} rows of statistics.")
    
    # labelカラムをcategoryカラムに変換
    if 'label' in combined_df.columns:
        # "After Road (Clean)" -> "after_road_clean"
        combined_df['category'] = combined_df['label'].apply(
            lambda x: x.lower().replace(' ', '_').replace('(', '').replace(')', '')
        )
        
    return combined_df

def fetch_event_metadata(grid_ids, event_ids):
    """DBからイベントのメタデータ（日時、降雨強度）を取得"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # event_id は 'delay_XXh_YYYYMMDD' 形式だが、DB検索には grid_id と日付などがキーになる
    # しかし、s1_pairs テーブルには event_id カラムがない場合がある
    # gsmap_events テーブルの情報が必要
    
    # 効率化のため、target_grids に含まれる gsmap_events を全取得してからマージする
    
    query = """
        SELECT 
            g.grid_id, 
            g.id as db_event_id,
            g.start_ts_utc as event_start,
            g.max_gauge_mm_h,
            MONTH(g.start_ts_utc) as month
        FROM gsmap_events g
        WHERE g.grid_id IN ({})
    """.format(','.join(['%s'] * len(grid_ids)))
    
    # Unique grid_ids
    unique_grids = list(set(grid_ids))
    
    logger.info(f"Fetching metadata for {len(unique_grids)} grids...")
    
    cursor.execute(query, unique_grids)
    results = cursor.fetchall()
    conn.close()
    
    meta_df = pd.DataFrame(results)
    return meta_df

def match_metadata(stats_df, meta_df):
    """統計データとメタデータを結合"""
    
    # stats_df の event_id から日付部分を抽出してマッチングキーにする
    # event_id format: delay_10h_20180928 -> date: 2018-09-28 ?
    # いや、gsmap_events の event_id と stats_df の event_id が一致しているか確認が必要
    # stats_df['event_id'] はフォルダ名。 build_s1_pairs... でフォルダ名はどう決まった？
    # folder name = f"delay_{delay}h_{date_str}"
    
    # DBの gsmap_events には event_id (int PK) があるが、フォルダ名には含まれていない可能性がある
    # 日付とGrid IDで結合するのが安全
    
    stats_df['date_str'] = stats_df['event_id'].apply(lambda x: x.split('_')[-1]) # 20180928
    stats_df['event_date'] = pd.to_datetime(stats_df['date_str'], format='%Y%m%d').dt.date
    
    meta_df['event_date'] = pd.to_datetime(meta_df['event_start']).dt.date
    
    # Merge
    merged = pd.merge(
        stats_df, 
        meta_df, 
        left_on=['grid_id', 'event_date'], 
        right_on=['grid_id', 'event_date'], 
        how='inner'
    )
    
    return merged

def plot_seasonal_trend(df, output_dir, pol):
    """月別のSigma分布（箱ひげ図） - RawとCleanを比較"""
    
    # Beforeデータに着目
    # category例: before_road_raw, before_road_clean, before_paddy_raw, before_paddy_clean
    
    targets_clean = ['before_road_clean', 'before_paddy_clean']
    targets_raw = ['before_road_raw', 'before_paddy_raw']
    
    df['Land Type'] = df['category'].apply(lambda x: 'Road' if 'road' in x else 'Paddy')
    df_sorted = df.sort_values('month')

    # Clean Plot
    clean_df = df_sorted[df_sorted['category'].isin(targets_clean)].copy()
    if not clean_df.empty:
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=clean_df, x='month', y='median', hue='Land Type', palette='Set2')
        plt.title(f'Baseline Seasonal Variation (Before Rain, Clean) - {pol.upper()}')
        plt.xlabel('Month')
        plt.ylabel('Sigma0 Median (dB)')
        plt.grid(True, alpha=0.3)
        plt.ylim(-25, 5)
        plt.savefig(output_dir / 'seasonal_trend_before_clean.png')
        plt.close()
    else:
        logger.warning("No Clean Before data found for plotting seasonal trend.")
    
    # Raw Plot
    raw_df = df_sorted[df_sorted['category'].isin(targets_raw)].copy()
    if not raw_df.empty:
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=raw_df, x='month', y='median', hue='Land Type', palette='Set2')
        plt.title(f'Baseline Seasonal Variation (Before Rain, Raw) - {pol.upper()}')
        plt.xlabel('Month')
        plt.ylabel('Sigma0 Median (dB)')
        plt.grid(True, alpha=0.3)
        plt.ylim(-25, 5)
        plt.savefig(output_dir / 'seasonal_trend_before_raw.png')
        plt.close()
    else:
        logger.warning("No Raw Before data found for plotting seasonal trend.")


def main():
    parser = argparse.ArgumentParser(description='Analyze sigma details')
    parser.add_argument('--polarization', type=str, required=True, choices=['vv', 'vh'])
    args = parser.parse_args()
    
    pol = args.polarization.lower()
    output_dir = DATA_DIR / "result" / pol / "sigma"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting Detailed Analysis for {pol.upper()}...")
    
    # 1. 統計データ読み込み
    stats_df = load_all_statistics(pol)
    if stats_df is None:
        return
    logger.info(f"Loaded {len(stats_df)} statistics rows.")
    
    # 2. DBからメタデータ取得
    grid_ids = stats_df['grid_id'].unique().tolist()
    meta_df = fetch_event_metadata(grid_ids, [])
    logger.info(f"Fetched metadata for {len(meta_df)} events.")
    
    # 3. 結合
    merged_df = match_metadata(stats_df, meta_df)
    logger.info(f"Merged data: {len(merged_df)} rows.")
    
    if len(merged_df) == 0:
        logger.warning("No matching data found between CSV and DB.")
        return

    # 4. 季節変化プロット（降雨前）
    logger.info("Plotting seasonal trend for Before scenes...")
    plot_seasonal_trend(merged_df, output_dir, pol)
    
    # レポート更新
    with open(output_dir / "detailed_analysis_summary.md", "w") as f:
        f.write(f"# Detailed Analysis Summary ({pol.upper()})\n\n")
        f.write("## 1. Baseline Seasonal Trend (Before Rain)\n")
        f.write("Comparison of Clean (Noise Removed) and Raw data.\n\n")
        
        f.write("### Clean Data (Noise Removed)\n")
        f.write("![Seasonal Trend Before Clean](seasonal_trend_before_clean.png)\n\n")
        
        f.write("### Raw Data\n")
        f.write("![Seasonal Trend Before Raw](seasonal_trend_before_raw.png)\n\n")
        
    logger.info(f"Analysis completed. Check results in {output_dir}")

if __name__ == "__main__":
    main()
