
import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import logging

# Setup
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def load_diff_stats(pol):
    """Load overall diff stats CSV"""
    csv_path = DATA_DIR / "result" / pol / "diff" / "overall_diff_stats.csv"
    if not csv_path.exists():
        logger.error(f"Diff stats file not found: {csv_path}")
        return None
    return pd.read_csv(csv_path)

def define_delay_bins(df):
    """Delay bin definition helper"""
    bins = [0, 6, 12, 18, 24, 48, 72, 96, 120, 9999]
    labels = ['0-6', '6-12', '12-18', '18-24', '24-48', '48-72', '72-96', '96-120', '>120']
    df['delay_bin'] = pd.cut(df['delay_hours'], bins=bins, labels=labels)
    return df

def detect_outliers_iqr(df, col, group_col='delay_bin'):
    """
    Detect outliers using IQR method within each group.
    Returns a boolean mask where True means NOT an outlier (Keep).
    """
    groups = df.groupby(group_col)
    
    # Calculate bounds per group
    # Q1, Q3
    Q1 = groups[col].transform(lambda x: x.quantile(0.25))
    Q3 = groups[col].transform(lambda x: x.quantile(0.75))
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    mask = (df[col] >= lower_bound) & (df[col] <= upper_bound)
    return mask

def plot_distribution_with_bounds(df, col, output_dir, pol, title_suffix):
    """Diffの分布と外れ値境界をプロット（全期間まとめて計算した境界ではなく、Binごとの分布を見る）"""
    plt.figure(figsize=(12, 6))
    
    # 全体の分布
    sns.histplot(data=df, x=col, kde=True, bins=50)
    plt.title(f'Distribution of {title_suffix} (All Delays) - {pol.upper()}')
    plt.xlabel('Difference [dB]')
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / f"dist_{col}.png")
    plt.close()
    
    # BinごとのBoxplot（外れ値が点として見える）
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x='delay_bin', y=col)
    plt.title(f'Distribution of {title_suffix} by Delay (Raw) - {pol.upper()}')
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / f"dist_by_bin_{col}_raw.png")
    plt.close()

def plot_evolution_clean(df, output_dir, pol, target_col, title_suffix):
    """CleanデータのEvolution Plot"""
    
    # Scatter
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='delay_hours', y=target_col, alpha=0.5, color='blue' if 'road' in target_col else 'orange')
    # Trend line
    sns.regplot(data=df, x='delay_hours', y=target_col, scatter=False, color='red', label='Trend (Linear)')
    
    plt.title(f'Difference Evolution ({title_suffix}) [Clean] - {pol.upper()}')
    plt.xlabel('Delay (hours)')
    plt.ylabel('Difference (After - Before) [dB]')
    plt.axhline(0, color='gray', linestyle='--')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.ylim(-10, 10) # 範囲を制限して見やすくする
    plt.savefig(output_dir / f"evolution_{target_col}_clean_scatter.png")
    plt.close()
    
    # Boxplot
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x='delay_bin', y=target_col, color='lightblue' if 'road' in target_col else 'orange')
    plt.title(f'Difference Evolution Boxplot ({title_suffix}) [Clean] - {pol.upper()}')
    plt.xlabel('Delay Bin (hours)')
    plt.ylabel('Difference [dB]')
    plt.axhline(0, color='gray', linestyle='--')
    plt.grid(True, alpha=0.3)
    plt.ylim(-10, 10)
    plt.savefig(output_dir / f"evolution_{target_col}_clean_boxplot.png")
    plt.close()

def main():
    parser = argparse.ArgumentParser(description='Analyze and filter diff outliers')
    parser.add_argument('--polarization', type=str, required=True, choices=['vv', 'vh'])
    args = parser.parse_args()
    
    pol = args.polarization.lower()
    output_dir = DATA_DIR / "result" / pol / "diff"
    
    df = load_diff_stats(pol)
    if df is None:
        return
    
    df = define_delay_bins(df)
    
    logger.info(f"Loaded {len(df)} events.")
    
    # Detect outliers for Road
    mask_road = detect_outliers_iqr(df, 'diff_road')
    outliers_road = df[~mask_road]
    clean_road = df[mask_road]
    
    logger.info(f"Road Outliers: {len(outliers_road)} ({len(outliers_road)/len(df)*100:.1f}%)")
    
    # Detect outliers for Paddy
    mask_paddy = detect_outliers_iqr(df, 'diff_paddy')
    outliers_paddy = df[~mask_paddy]
    clean_paddy = df[mask_paddy] # Paddy用のCleanデータセット（Roadとは別の行が消える可能性あり）
    
    logger.info(f"Paddy Outliers: {len(outliers_paddy)} ({len(outliers_paddy)/len(df)*100:.1f}%)")
    
    # 完全にクリーンなセット（RoadもPaddyも正常）は必要か？
    # 分析は別々に行うので、Road分析にはClean Road、PaddyにはClean Paddyを使えばデータロスが少ない。
    
    # Plot Distributions (Raw containing all outliers)
    plot_distribution_with_bounds(df, 'diff_road', output_dir, pol, "Road")
    plot_distribution_with_bounds(df, 'diff_paddy', output_dir, pol, "Paddy")
    
    # Plot Evolution (Clean)
    plot_evolution_clean(clean_road, output_dir, pol, 'diff_road', "Road")
    plot_evolution_clean(clean_paddy, output_dir, pol, 'diff_paddy', "Paddy")
    
    # Report
    with open(output_dir / "outlier_analysis_report.md", "w") as f:
        f.write(f"# Outlier Analysis Report ({pol.upper()})\n\n")
        
        f.write("## Outlier Detection Stats (IQR Method)\n")
        f.write(f"- **Total Events**: {len(df)}\n")
        f.write(f"- **Road Outliers**: {len(outliers_road)} ({len(outliers_road)/len(df)*100:.1f}%)\n")
        f.write(f"- **Paddy Outliers**: {len(outliers_paddy)} ({len(outliers_paddy)/len(df)*100:.1f}%)\n\n")
        
        f.write("## Distributions (Raw)\n")
        f.write("![Road Dist Raw](dist_diff_road.png)\n")
        f.write("![Road Boxplot Raw](dist_by_bin_diff_road_raw.png)\n\n")
        
        f.write("## Evolution After Outlier Removal\n")
        f.write("### Road (Clean)\n")
        f.write("![Road Scatter Clean](evolution_diff_road_clean_scatter.png)\n")
        f.write("![Road Boxplot Clean](evolution_diff_road_clean_boxplot.png)\n\n")
        
        f.write("### Paddy (Clean)\n")
        f.write("![Paddy Scatter Clean](evolution_diff_paddy_clean_scatter.png)\n")
        f.write("![Paddy Boxplot Clean](evolution_diff_paddy_clean_boxplot.png)\n")
        
    logger.info("Outlier analysis completed.")

if __name__ == "__main__":
    main()
