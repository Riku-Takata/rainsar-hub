import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Config
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_EXP_DIR = BASE_DIR / "data" / "expanded"
ANALYSIS_DIR = DATA_EXP_DIR / "analysis"
EVO_DIR = ANALYSIS_DIR / "evolution"
CSV_PATH = EVO_DIR / "evolution_data_filtered.csv" # Filtered Input
OUT_DIR = ANALYSIS_DIR / "rain_categories_filtered"

def main():
    if not CSV_PATH.exists():
        print(f"Error: {CSV_PATH} not found.")
        return
    
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_csv(CSV_PATH)
    
    # Filter valid
    df = df.dropna(subset=['rain_total_est_mm', 'road_diff_mean', 'paddy_diff_mean', 'delay_h'])
    
    def classify(row):
        total = row['rain_total_est_mm']
        if total < 20: return "Light (<20mm)"
        elif total < 50: return "Moderate (20-50mm)"
        else: return "Heavy (>50mm)"
        
    df['category'] = df.apply(classify, axis=1)
    
    categories = ["Moderate (20-50mm)", "Heavy (>50mm)"]
    sns.set_style("whitegrid")
    
    for cat in categories:
        sub = df[df['category'] == cat].copy()
        if len(sub) < 5:
            print(f"Skipping {cat}: Not enough data (N={len(sub)})")
            continue
            
        print(f"Analyzing {cat} (N={len(sub)})...")
        
        # Melt for Seaborn
        road = sub[['delay_h', 'road_diff_mean']].copy()
        road['Type'] = 'Road'
        road = road.rename(columns={'road_diff_mean': 'Diff (dB)'})
        
        paddy = sub[['delay_h', 'paddy_diff_mean']].copy()
        paddy['Type'] = 'Paddy'
        paddy = paddy.rename(columns={'paddy_diff_mean': 'Diff (dB)'})
        
        melted = pd.concat([road, paddy], ignore_index=True)
        
        # Binning
        bins = [0, 3, 6, 9, 12, 24]
        labels = ["0-3h", "3-6h", "6-9h", "9-12h", "12h+"]
        melted['Time Bin'] = pd.cut(melted['delay_h'], bins=bins, labels=labels, right=False)
        
        plt.figure(figsize=(10, 6))
        
        # Pointplot
        sns.pointplot(data=melted, x='Time Bin', y='Diff (dB)', hue='Type', 
                      palette={'Road': 'red', 'Paddy': 'green'}, 
                      markers=['o', 's'], linestyles=['-', '--'], capsize=0.1)
        
        plt.axhline(0, color='gray', linestyle=':', alpha=0.5)
        plt.title(f"Filtered (Sigma=2.0) Evolution: {cat}")
        plt.ylabel("Difference (After - Before) [dB]")
        plt.xlabel("Wait Time (Delay)")
        plt.ylim(-2.0, 2.0)
        
        fname = f"evolution_{cat.split(' ')[0].lower()}_filtered.png"
        plt.savefig(OUT_DIR / fname)
        plt.close()
        print(f"  Saved plot: {fname}")
        
        # Summary
        summary = melted.groupby(['Time Bin', 'Type'], observed=False)['Diff (dB)'].agg(['mean', 'std', 'count']).unstack()
        print(summary.round(3))
        print("-" * 40)

    print("\nComparison Complete. See output in:", OUT_DIR)

if __name__ == "__main__":
    main()
