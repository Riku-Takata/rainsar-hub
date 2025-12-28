import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import matplotlib.font_manager as fm

def configure_plotting():
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

def main():
    configure_plotting()
    
    csv_path = Path(r"d:\sotsuron\rainsar-hub\result\distributions\analysis_v2\valid_stats.csv")
    output_dir = Path(r"d:\sotsuron\rainsar-hub\result\distributions\analysis_v2")
    
    if not csv_path.exists():
        print("Valid Stats CSV not found")
        return

    df = pd.read_csv(csv_path)
    
    # Calculate Median from '50%' column
    df['Median'] = df['50%']

    # --- Categorize Delay ---
    def categorize_delay(d):
        if d <= 1.5: return 'Short (<=1.5h)'
        elif d > 6.0: return 'Long (>6h)'
        else: return 'Middle (1.5-6h)'
        
    df['DelayCategory'] = df['Delay'].apply(categorize_delay)
    df_compare = df[df['DelayCategory'].isin(['Short (<=1.5h)', 'Long (>6h)'])].copy()
    order = ['Short (<=1.5h)', 'Long (>6h)']

    # --- 1. Boxplot: Mean vs Median Difference ---
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    
    sns.boxplot(data=df_compare, x='DelayCategory', y='Mean', hue='Type', order=order, showfliers=False, ax=axes[0])
    axes[0].set_title("平均値 (Mean) の分布")
    axes[0].set_ylabel("Mean Difference (Linear)")
    
    sns.boxplot(data=df_compare, x='DelayCategory', y='Median', hue='Type', order=order, showfliers=False, ax=axes[1])
    axes[1].set_title("中央値 (Median) の分布")
    axes[1].set_ylabel("Median Difference (Linear)")
    
    plt.tight_layout()
    plt.savefig(output_dir / "boxplot_mean_vs_median.png", dpi=300)
    plt.close()

    # --- 2. Trend Plot: Delay vs Mean/Median ---
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    
    colors = {'道路': 'orange', '田んぼ': 'blue'}
    
    # Mean Trend
    sns.scatterplot(data=df, x='Delay', y='Mean', hue='Type', style='Type', s=100, alpha=0.7, ax=axes[0])
    for t in ['道路', '田んぼ']:
        sub = df[df['Type'] == t]
        sns.regplot(data=sub, x='Delay', y='Mean', scatter=False, color=colors.get(t, 'black'), label=f"{t} Trend", ci=None, ax=axes[0])
    axes[0].set_title("遅延時間 vs 平均値 (Mean)")
    
    # Median Trend
    sns.scatterplot(data=df, x='Delay', y='Median', hue='Type', style='Type', s=100, alpha=0.7, ax=axes[1])
    for t in ['道路', '田んぼ']:
        sub = df[df['Type'] == t]
        sns.regplot(data=sub, x='Delay', y='Median', scatter=False, color=colors.get(t, 'black'), label=f"{t} Trend", ci=None, ax=axes[1])
    axes[1].set_title("遅延時間 vs 中央値 (Median)")
    
    plt.tight_layout()
    plt.savefig(output_dir / "trend_mean_vs_median.png", dpi=300)
    plt.close()
    
    print(f"Saved plots to {output_dir}")

    # --- Numerical Comparison ---
    print("\n=== Comparison: Mean vs Median (Short vs Long) ===")
    summary = df_compare.groupby(['DelayCategory', 'Type'])[['Mean', 'Median', 'Std']].describe()
    print(summary.to_string())
    
    # Check if Median provides better separation
    short_stats = df_compare[df_compare['DelayCategory'] == 'Short (<=1.5h)'].groupby('Type')[['Mean', 'Median']].mean()
    print("\n=== Short-term Separation (Signal) ===")
    print(short_stats)
    
    mean_diff = short_stats.loc['田んぼ', 'Mean'] - short_stats.loc['道路', 'Mean']
    median_diff = short_stats.loc['田んぼ', 'Median'] - short_stats.loc['道路', 'Median']
    
    print(f"\nPaddy - Road (Mean): {mean_diff:.6f}")
    print(f"Paddy - Road (Median): {median_diff:.6f}")

if __name__ == "__main__":
    main()
