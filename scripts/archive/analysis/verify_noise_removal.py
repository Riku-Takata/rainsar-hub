"""
ノイズ除去検証スクリプト

個別のイベントフォルダにある statistics.csv を集計し、
RawデータとCleanデータ（ノイズ除去後）の統計値を比較する。
また、平均値と中央値の差異を分析し、代表値の選定を支援する。

Usage:
    python verify_noise_removal.py --polarization vv
    python verify_noise_removal.py --polarization vh
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor
import logging

# Setup
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def load_all_statistics(pol):
    """全てのイベントのstatistics.csvを読み込んで結合する"""
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
    
    return combined_df

def analyze_noise_reduction(df, output_dir):
    """ノイズ除去によるばらつき減少効果を分析"""
    
    pairs = [
        ('After Road (Raw)', 'After Road (Clean)', 'After Road'),
        ('After Paddy (Raw)', 'After Paddy (Clean)', 'After Paddy'),
        ('Before Road (Raw)', 'Before Road (Clean)', 'Before Road'),
        ('Before Paddy (Raw)', 'Before Paddy (Clean)', 'Before Paddy')
    ]
    
    summary_stats = []
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Effect of Noise Removal on Standard Deviation', fontsize=16)
    
    for (raw_label, clean_label, title), ax in zip(pairs, axes.flat):
        raw_data = df[df['label'] == raw_label]
        clean_data = df[df['label'] == clean_label]
        
        if raw_data.empty or clean_data.empty:
            ax.text(0.5, 0.5, 'No Data', ha='center', va='center')
            continue

        # 統計量の平均を計算
        avg_std_raw = raw_data['std'].mean()
        avg_std_clean = clean_data['std'].mean()
        
        if pd.isna(avg_std_raw) or avg_std_raw == 0:
            reduction = 0
        else:
            reduction = (avg_std_raw - avg_std_clean) / avg_std_raw * 100
        
        summary_stats.append({
            'Category': title,
            'Avg Std (Raw)': avg_std_raw,
            'Avg Std (Clean)': avg_std_clean,
            'Reduction (%)': reduction,
            'Avg Range (Raw)': (raw_data['max'] - raw_data['min']).mean(),
            'Avg Range (Clean)': (clean_data['max'] - clean_data['min']).mean()
        })
        
        # 分布比較 (KDE)
        try:
            sns.kdeplot(data=raw_data['std'], label='Raw', fill=True, ax=ax, color='red', alpha=0.3, warn_singular=False)
            sns.kdeplot(data=clean_data['std'], label='Clean', fill=True, ax=ax, color='blue', alpha=0.3, warn_singular=False)
        except Exception as e:
            logger.warning(f"KDE plot failed for {title}: {e}")
        
        ax.set_title(f'{title}\nStd Reduction: {reduction:.1f}%')
        ax.set_xlabel('Standard Deviation (dB)')
        # ax.legend() # kdeplot handles legend if label is passed, but ax.legend() ensures it displays
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_dir / 'noise_reduction_std_plot.png')
    plt.close()
    
    return pd.DataFrame(summary_stats)

def analyze_representative_value(df, output_dir):
    """平均値と中央値の差異を分析（Cleanデータのみ）"""
    
    clean_labels = [
        'After Road (Clean)', 'After Paddy (Clean)', 
        'Before Road (Clean)', 'Before Paddy (Clean)'
    ]
    
    clean_df = df[df['label'].isin(clean_labels)].copy()
    
    if clean_df.empty:
        return pd.DataFrame()

    # 差異を計算
    clean_df['diff_mean_median'] = clean_df['mean'] - clean_df['median']
    clean_df['abs_diff'] = clean_df['diff_mean_median'].abs()
    
    # プロット
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # 1. 散布図: Mean vs Median
    sns.scatterplot(data=clean_df, x='mean', y='median', hue='label', alpha=0.5, ax=axes[0])
    
    # y=x line
    try:
        lims = [
            min(axes[0].get_xlim(), axes[0].get_ylim()),
            max(axes[0].get_xlim(), axes[0].get_ylim()),
        ]
        axes[0].plot(lims, lims, 'k--', alpha=0.75, zorder=0)
    except:
        pass
        
    axes[0].set_title('Mean vs Median Correlation')
    axes[0].grid(True, alpha=0.3)
    
    # 2. 差異の分布
    try:
        sns.histplot(data=clean_df, x='diff_mean_median', hue='label', kde=True, element="step", ax=axes[1])
        axes[1].axvline(0, color='k', linestyle='--')
    except Exception as e:
        logger.warning(f"Hist plot failed: {e}")
        
    axes[1].set_title('Distribution of (Mean - Median)')
    axes[1].set_xlabel('Difference (dB)')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_dir / 'mean_vs_median_plot.png')
    plt.close()
    
    # 統計サマリー
    stats = clean_df.groupby('label')['abs_diff'].agg(['mean', 'std', 'max']).reset_index()
    stats.columns = ['Category', 'Mean Abs Diff', 'Std Abs Diff', 'Max Abs Diff']
    
    return stats

def main():
    parser = argparse.ArgumentParser(description='Verify noise removal effects')
    parser.add_argument('--polarization', type=str, required=True, choices=['vv', 'vh'])
    args = parser.parse_args()
    
    pol = args.polarization.lower()
    output_dir = DATA_DIR / "result" / pol / "sigma"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Analyzing {pol.upper()} polarization...")
    
    df = load_all_statistics(pol)
    if df is None or df.empty:
        logger.error("No data loaded.")
        return
    
    # 1. ノイズ除去効果分析
    logger.info("Running Noise Reduction Analysis...")
    noise_stats = analyze_noise_reduction(df, output_dir)
    print("\nNoise Reduction Summary:")
    print(noise_stats.to_string(index=False))
    
    # 2. 代表値選定分析
    logger.info("Running Representative Value Analysis...")
    rep_stats = analyze_representative_value(df, output_dir)
    print("\nMean vs Median Difference Summary (Clean Data):")
    print(rep_stats.to_string(index=False))
    
    # レポート出力
    report_file = output_dir / "noise_verification_report.md"
    with open(report_file, 'w') as f:
        f.write(f"# Noise Verification Report ({pol.upper()})\n\n")
        
        f.write("## 1. Effect of Noise Removal\n")
        f.write("Comparison of standard deviation (Std) between Raw and Clean data.\n\n")
        f.write(noise_stats.to_markdown(index=False))
        f.write("\n\n![Noise Reduction Plot](noise_reduction_std_plot.png)\n\n")
        
        f.write("## 2. Representative Value Analysis\n")
        f.write("Difference between Mean and Median in Clean data.\n\n")
        f.write(rep_stats.to_markdown(index=False))
        f.write("\n\n![Mean vs Median Plot](mean_vs_median_plot.png)\n\n")
        
        f.write("## 3. Conclusion\n")
        
        if not rep_stats.empty:
            avg_diff = rep_stats['Mean Abs Diff'].mean()
            f.write(f"- Average absolute difference between Mean and Median is **{avg_diff:.3f} dB**.\n")
            
            if avg_diff < 0.5:
                f.write("- Since the difference is small (< 0.5 dB), **Mean** is likely a good representative value.\n")
            else:
                f.write("- Since the difference is typically > 0.5 dB, **Median** is recommended as a more robust representative value.\n")
        else:
            f.write("- Analysis failed or no data.\n")

    logger.info(f"Report saved to: {report_file}")

if __name__ == "__main__":
    main()
