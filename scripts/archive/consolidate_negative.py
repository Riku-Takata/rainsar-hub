import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import matplotlib.font_manager as fm
import numpy as np
import rasterio
import re

# Settings
BASE_DIR = Path(r"d:\sotsuron\rainsar-hub")
OUTPUT_DIR = BASE_DIR / "result" / "20251205" / "negative"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATA_BASE_DIR = Path(r"D:\sotsuron")
S1_SAMPLES_DIR = DATA_BASE_DIR / "s1_samples"
S1_SAFE_DIR = DATA_BASE_DIR / "s1_safe"
TARGET_GRIDS = [
    "N03145E13095", "N03285E13005", "N03285E13075", "N03285E13085",
    "N03285E13115", "N03285E13165", "N03295E12995", "N03295E13075",
    "N03295E13185", "N03325E13125", "N03335E13095", "N03355E13085",
    "N03355E13125", "N03375E13065", "N03375E13075", "N03385E13065",
    "N03375E13095"
]
SUFFIX_HIGHWAY = "_highway_mask.tif"
SUFFIX_PADDY = "_paddy_mask.tif"

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
        delay_m = re.search(r"Delay \(Hours\)\s+: ([\d\.]+)", block)
        after_m = re.search(r"After Scene\s*:? (S1\w+)", block)
        before_m = re.search(r"Before Scene\s*:? (S1\w+)", block)
        if delay_m and after_m and before_m:
            data['delay'] = float(delay_m.group(1))
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
        return 10 ** (data[valid_mask] / 10.0)

def main():
    configure_plotting()
    
    # Check if stats CSV already exists to avoid re-processing
    stats_csv = OUTPUT_DIR / "negative_full_stats.csv"
    if stats_csv.exists():
        print("Loading existing negative stats...")
        df = pd.read_csv(stats_csv)
    else:
        records = []
        print("Processing Negative Pixels for all grids...")
        for grid_id in TARGET_GRIDS:
            events = parse_summary_txt(grid_id)
            if not events: continue
            grid_dir = S1_SAMPLES_DIR / grid_id
            
            for evt in events:
                delay = evt['delay']
                stem_after = f"{evt['after_scene']}_proc"
                stem_before = f"{evt['before_scene']}_proc"
                
                for type_name, suffix in [('道路', SUFFIX_HIGHWAY), ('田んぼ', SUFFIX_PADDY)]:
                    path_after = grid_dir / f"{stem_after}{suffix}"
                    path_before = grid_dir / f"{stem_before}{suffix}"
                    
                    arr_a = read_linear_values(path_after)
                    arr_b = read_linear_values(path_before)
                    
                    if arr_a is not None and arr_b is not None:
                        min_len = min(len(arr_a), len(arr_b))
                        arr_a = arr_a[:min_len]
                        arr_b = arr_b[:min_len]
                        diff = arr_a - arr_b
                        
                        # Negative Pixels
                        neg_mask = diff < 0
                        if np.sum(neg_mask) > 0:
                            neg_diff = diff[neg_mask]
                            neg_a = arr_a[neg_mask]
                            neg_b = arr_b[neg_mask]
                            
                            records.append({
                                'Grid': grid_id, 'Delay': delay, 'Type': type_name,
                                'Diff_Mean': np.mean(neg_diff),
                                'Diff_Median': np.median(neg_diff),
                                'Diff_Variance': np.var(neg_diff),
                                'Int_After_Mean': np.mean(neg_a),
                                'Int_After_Median': np.median(neg_a),
                                'Int_After_Variance': np.var(neg_a),
                                'Int_Before_Mean': np.mean(neg_b),
                                'Int_Before_Median': np.median(neg_b),
                                'Int_Before_Variance': np.var(neg_b)
                            })

        if not records:
            print("No negative pixel data found.")
            return

        df = pd.DataFrame(records)
        df.to_csv(stats_csv, index=False)
    
    # --- PLOTTING ---
    metrics = ['Mean', 'Median', 'Variance']
    
    # 1. Negative Diff Trends
    for metric in metrics:
        col = f'Diff_{metric}'
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df, x='Delay', y=col, hue='Type', alpha=0.6)
        for t, color in [('道路', 'orange'), ('田んぼ', 'blue')]:
            sub = df[df['Type'] == t]
            sns.regplot(data=sub, x='Delay', y=col, scatter=False, color=color, ci=None, label=f"{t} Trend")
        plt.title(f"負の差分 (Negative Diff) - {metric}")
        plt.xlabel("Delay (Hours)")
        plt.ylabel(f"Negative Diff {metric}")
        plt.legend()
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / f"negative_diff_{metric.lower()}.png")
        plt.close()

    # 2. Negative Intensity Trends (Combined After & Before)
    # Melt the dataframe to have a 'Time' column
    df_long_list = []
    for metric in metrics:
        # Create a subset for this metric
        cols = ['Grid', 'Delay', 'Type', f'Int_After_{metric}', f'Int_Before_{metric}']
        sub = df[cols].copy()
        sub.rename(columns={f'Int_After_{metric}': 'After', f'Int_Before_{metric}': 'Before'}, inplace=True)
        melted = sub.melt(id_vars=['Grid', 'Delay', 'Type'], value_vars=['After', 'Before'], var_name='Time', value_name='Value')
        melted['Metric'] = metric
        df_long_list.append(melted)
    
    if df_long_list:
        df_long = pd.concat(df_long_list)
        
        for metric in metrics:
            plt.figure(figsize=(10, 6))
            data_metric = df_long[df_long['Metric'] == metric]
            
            # Plot scatter with hue=Type and style=Time
            sns.scatterplot(data=data_metric, x='Delay', y='Value', hue='Type', style='Time', alpha=0.7, s=80)
            
            plt.title(f"負の差分ピクセル・強度比較 (Intensity Comparison) - {metric}")
            plt.xlabel("Delay (Hours)")
            plt.ylabel(f"Intensity {metric}")
            plt.legend()
            plt.tight_layout()
            plt.savefig(OUTPUT_DIR / f"negative_intensity_combined_{metric.lower()}.png")
            plt.close()

    print(f"Saved negative analysis to {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
