"""
Analyze correlation between RF Accuracy and Sample Counts (or Unique Events).
Identify if high accuracy correlates with low sample size.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
from pathlib import Path

matplotlib.rcParams['font.family'] = 'MS Gothic'
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
METRICS_PATH = BASE_DIR / "data/result/seasonal/rf_results/rf_accuracy_metrics.csv"
OUTPUT_DIR = BASE_DIR / "data/result/seasonal/rf_results"

def main():
    if not METRICS_PATH.exists():
        print("Metrics file not found.")
        return
        
    df = pd.read_csv(METRICS_PATH)
    
    # df columns: month, delay, train_samples, test_samples, test_accuracy...
    # We want to check 'test_samples' vs 'test_accuracy'
    
    print("Columns:", df.columns.tolist())
    
    # 1. Scatter Plot: Accuracy vs Sample Size (colored by Month)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='test_samples', y='test_accuracy', hue='month', style='month', s=100, palette='tab10')
    
    plt.xscale('log') # Sample size varies widely
    plt.title('分類精度 vs テストサンプル数 (対数スケール)')
    plt.xlabel('テストサンプル数 (Log)')
    plt.ylabel('テスト精度')
    plt.grid(True, which="both", ls="-", alpha=0.2)
    plt.ylim(0.4, 1.0)
    
    # Add annotations for high accuracy (>0.8) or low samples (<1000)
    for idx, row in df.iterrows():
        if row['test_accuracy'] > 0.8 or row['test_samples'] < 2000:
            plt.text(row['test_samples'], row['test_accuracy']+0.01, 
                     f"M{int(row['month'])} D{int(row['delay'])}", 
                     fontsize=8, ha='center')
            
    plt.savefig(OUTPUT_DIR / "accuracy_vs_samples.png", dpi=150)
    print(f"Saved: {OUTPUT_DIR}/accuracy_vs_samples.png")
    
    # 2. Dual Axis Plot per Month: Accuracy and Sample Size over Delay
    months = df['month'].unique()
    
    for month in months:
        subset = df[df['month'] == month].sort_values('delay')
        
        fig, ax1 = plt.subplots(figsize=(10, 5))
        
        # Axis 1: Accuracy
        color = 'tab:blue'
        ax1.set_xlabel('Delay (h)')
        ax1.set_ylabel('Accuracy', color=color)
        ax1.plot(subset['delay'], subset['test_accuracy'], 'o-', color=color, label='Accuracy')
        ax1.tick_params(axis='y', labelcolor=color)
        ax1.set_ylim(0.4, 1.0)
        ax1.grid()
        
        # Axis 2: Sample Size
        ax2 = ax1.twinx()
        color = 'tab:gray'
        ax2.set_ylabel('Sample Size (Test)', color=color)
        ax2.bar(subset['delay'], subset['test_samples'], alpha=0.3, color=color, label='Samples')
        ax2.tick_params(axis='y', labelcolor=color)
        ax2.set_yscale('log') # Log scale for samples
        
        plt.title(f"{int(month)}月: 精度とサンプル数の関係")
        fig.tight_layout()
        plt.savefig(OUTPUT_DIR / f"accuracy_samples_M{int(month)}.png")
        print(f"Saved: accuracy_samples_M{int(month)}.png")
        
    # 3. Correlation
    corr = df[['test_accuracy', 'test_samples']].corr().iloc[0, 1]
    print(f"\nOverall Correlation (Accuracy vs Samples): {corr:.4f}")

if __name__ == "__main__":
    main()
