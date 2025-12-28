import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import matplotlib.font_manager as fm

def configure_plotting():
    """日本語フォント設定"""
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
        print("Warning: Japanese font not found. Using default.")

def main():
    configure_plotting()
    
    csv_path = Path(r"d:\sotsuron\rainsar-hub\result\distributions\aggregated_stats_difference.csv")
    output_dir = Path(r"d:\sotsuron\rainsar-hub\result\distributions\global_analysis")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not csv_path.exists():
        print("CSV not found")
        return

    df = pd.read_csv(csv_path)
    
    # --- Filter Noise ---
    blacklist_grids = ['N03355E13125']
    df_clean = df[~df['Grid'].isin(blacklist_grids)].copy()
    
    # Remove specific outliers
    outliers = [
        ('N03335E13095', 9.36, '道路'),
        ('N03295E13185', 0.15, '田んぼ'),
        ('N03375E13095', 10.23, '田んぼ')
    ]
    for g, d, t in outliers:
        mask = (df_clean['Grid'] == g) & (df_clean['Delay'] == d) & (df_clean['Type'] == t)
        df_clean = df_clean[~mask]

    # --- Categorize Delay ---
    # Short: <= 1.5h, Long: > 6h (ignoring middle for clear contrast)
    def categorize_delay(d):
        if d <= 1.5: return 'Short (<=1.5h)'
        elif d > 6.0: return 'Long (>6h)'
        else: return 'Middle (1.5-6h)'
        
    df_clean['DelayCategory'] = df_clean['Delay'].apply(categorize_delay)
    
    # Filter for Short vs Long only
    df_compare = df_clean[df_clean['DelayCategory'].isin(['Short (<=1.5h)', 'Long (>6h)'])].copy()
    
    # Order for plotting
    order = ['Short (<=1.5h)', 'Long (>6h)']

    # --- 1. Boxplot: Mean Difference (Signal) ---
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df_compare, x='DelayCategory', y='Mean', hue='Type', order=order, showfliers=False)
    plt.title("平均後方散乱強度差分の分布 (Short vs Long)")
    plt.ylabel("Mean Difference (Linear)")
    plt.xlabel("Delay Category")
    plt.tight_layout()
    plt.savefig(output_dir / "boxplot_mean_diff.png", dpi=300)
    plt.close()

    # --- 2. Boxplot: Standard Deviation (Noise) ---
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df_compare, x='DelayCategory', y='Std', hue='Type', order=order, showfliers=False)
    plt.title("後方散乱強度差分のばらつき (Short vs Long)")
    plt.ylabel("Standard Deviation")
    plt.xlabel("Delay Category")
    plt.tight_layout()
    plt.savefig(output_dir / "boxplot_std_dev.png", dpi=300)
    plt.close()

    # --- 3. Trend Plot: Delay vs Mean ---
    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=df_clean, x='Delay', y='Mean', hue='Type', style='Type', s=100, alpha=0.7)
    # Add trend lines (Lowess)
    # sns.regplot is tricky with hue, so loop
    colors = {'道路': 'orange', '田んぼ': 'blue'}
    for t in ['道路', '田んぼ']:
        sub = df_clean[df_clean['Type'] == t]
        sns.regplot(data=sub, x='Delay', y='Mean', scatter=False, color=colors.get(t, 'black'), label=f"{t} Trend", ci=None)
    
    plt.title("遅延時間と平均差分の関係 (全体トレンド)")
    plt.ylabel("Mean Difference (Linear)")
    plt.xlabel("Delay (Hours)")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_dir / "trend_delay_mean.png", dpi=300)
    plt.close()

    # --- 4. Trend Plot: Delay vs Std ---
    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=df_clean, x='Delay', y='Std', hue='Type', style='Type', s=100, alpha=0.7)
    for t in ['道路', '田んぼ']:
        sub = df_clean[df_clean['Type'] == t]
        sns.regplot(data=sub, x='Delay', y='Std', scatter=False, color=colors.get(t, 'black'), label=f"{t} Trend", ci=None)
        
    plt.title("遅延時間とばらつきの関係 (全体トレンド)")
    plt.ylabel("Standard Deviation")
    plt.xlabel("Delay (Hours)")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(output_dir / "trend_delay_std.png", dpi=300)
    plt.close()

    print(f"Saved plots to {output_dir}")
    
    # --- Print Numerical Summary for Analysis ---
    print("\n=== Numerical Summary (Short vs Long) ===")
    print(df_compare.groupby(['DelayCategory', 'Type'])[['Mean', 'Std']].describe().to_string())

if __name__ == "__main__":
    main()
