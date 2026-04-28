import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Config
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
INPUT_CSV = BASE_DIR / "data/analysis/rf_classification_metrics.csv"
OUTPUT_DIR = BASE_DIR / "data/analysis/rf_plots"
ARTIFACT_DIR = Path(r"C:\Users\riku_\.gemini\antigravity\brain\b329ce41-a43c-48c1-b77b-c2a6700a3f1f")
TARGET_MONTH = 10
BASELINE_VAL = 0.594

# Font setup
plt.rcParams['font.family'] = ['Meiryo', 'MS Gothic', 'sans-serif']

def run():
    if not INPUT_CSV.exists():
        print(f"Error: CSV not found at {INPUT_CSV}")
        return

    df = pd.read_csv(INPUT_CSV)
    
    # Filter for Month 10
    df = df[df['month'] == TARGET_MONTH].copy()
    df = df.sort_values('delay_int')
    
    if df.empty:
        print(f"No data found for Month {TARGET_MONTH}")
        return

    # Plot
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df, x='delay_int', y='accuracy', marker='o')
    
    plt.title(f"Random Forest 分類精度の推移 ({TARGET_MONTH}月)", fontsize=16)
    plt.xlabel("降雨後経過時間 [時間後]", fontsize=14)
    plt.ylabel("精度", fontsize=14)
    
    # Adjust Y-axis limits dynamically but with reasonable margin for baseline
    y_min = min(df['accuracy'].min(), BASELINE_VAL)
    y_max = df['accuracy'].max()
    margin = 0.02
    plt.ylim(y_min - margin, y_max + margin + 0.01)
    
    # Grid
    plt.grid(True)
    
    # Baseline line
    plt.axhline(BASELINE_VAL, color='red', linestyle='--', linewidth=1.5, label=f'Baseline ({BASELINE_VAL})')
    
    # Add annotations (numeric values)
    for i, row in df.iterrows():
        plt.text(row['delay_int'], row['accuracy'] + 0.002, f"{row['accuracy']:.3f}", 
                 fontsize=11, ha='center', va='bottom', color='black', fontweight='bold')
    
    plt.legend()
    
    # Save
    out_path = OUTPUT_DIR / "rf_accuracy_trend.png"
    art_path = ARTIFACT_DIR / "rf_accuracy_trend.png" # Optional: update artifact if tracked there
    # Note: User referred to data/analysis/rf_plots/rf_accuracy_trend.png
    
    plt.savefig(out_path, dpi=300)
    # Also save to artifact dir for consistency if needed, though not explicitly requested as artifact update
    # But useful for embedding in walkthrough
    plt.savefig(art_path, dpi=300)
    
    plt.close()
    
    print(f"Generated graph from CSV:\n - {out_path}")

if __name__ == "__main__":
    run()
