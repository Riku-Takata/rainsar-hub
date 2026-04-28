import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Config
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
INPUT_CSV = BASE_DIR / "data/result/River_vs_Road_Aug/metrics_aug_refined.csv"
OUTPUT_DIR = BASE_DIR / "data/result/River_vs_Road_Aug"
ARTIFACT_DIR = Path(r"C:\Users\riku_\.gemini\antigravity\brain\b329ce41-a43c-48c1-b77b-c2a6700a3f1f")

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def run():
    if not INPUT_CSV.exists():
        print(f"Error: CSV not found at {INPUT_CSV}")
        return

    df = pd.read_csv(INPUT_CSV)
    
    # Plot
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='delay', y='accuracy', marker='o')
    
    plt.title("Random Forest 分類精度の推移", fontsize=16)
    plt.xlabel("降雨後経過時間 [時間後]", fontsize=14)
    plt.ylabel("精度", fontsize=14)
    
    # Y-axis limits
    plt.ylim(0.70, 1.00)
    
    # Baseline line
    baseline_val = 0.751
    plt.axhline(baseline_val, color='red', linestyle='--', linewidth=1.5, label=f'Baseline ({baseline_val})')
    
    # Grid
    plt.grid(True)
    
    # Add annotations (numeric values)
    for i, row in df.iterrows():
        plt.text(row['delay'], row['accuracy'] + 0.005, f"{row['accuracy']:.3f}", 
                 fontsize=11, ha='center', va='bottom', color='black', fontweight='bold')
    
    plt.legend()
    
    # Save
    out_path = OUTPUT_DIR / "accuracy_trend_aug_river.png"
    art_path = ARTIFACT_DIR / "accuracy_trend_aug_river.png"
    
    plt.savefig(out_path, dpi=300)
    plt.savefig(art_path, dpi=300)
    plt.close()
    
    print(f"Generated graph from CSV:\n - {out_path}\n - {art_path}")

if __name__ == "__main__":
    run()
