
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import matplotlib

# Japanese font configuration
matplotlib.rcParams['font.family'] = 'MS Gothic'

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
METRICS_CSV = BASE_DIR / "data" / "result" / "seasonal" / "rf_results" / "rf_accuracy_metrics.csv"
OUTPUT_DIR = BASE_DIR / "data" / "result" / "seasonal" / "rf_results"

def main():
    if not METRICS_CSV.exists():
        print(f"Metrics CSV not found: {METRICS_CSV}")
        return

    df = pd.read_csv(METRICS_CSV)
    
    # Pivot for Heatmap
    # Index: Delay, Columns: Month, Values: test_accuracy
    pivot_acc = df.pivot(index='delay', columns='month', values='test_accuracy')
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot_acc, annot=True, fmt='.3f', cmap='viridis', vmin=0.3, vmax=0.7)
    plt.title("テスト精度マトリクス (月 vs 経過時間)")
    plt.ylabel("経過時間 (h)")
    plt.xlabel("月")
    plt.tight_layout()
    
    out_path = OUTPUT_DIR / "heatmap_rf_test_accuracy.png"
    plt.savefig(out_path)
    print(f"Saved Heatmap to {out_path}")

    # Road Accuracy (Recall Class 0) Matrix
    pivot_road = df.pivot(index='delay', columns='month', values='test_acc_road')
    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot_road, annot=True, fmt='.3f', cmap='Blues', vmin=0.0, vmax=1.0)
    plt.title("道路分類精度マトリクス")
    plt.ylabel("経過時間 (h)")
    plt.xlabel("月")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "heatmap_rf_road_accuracy.png")

    # Paddy Accuracy (Recall Class 1) Matrix
    pivot_paddy = df.pivot(index='delay', columns='month', values='test_acc_paddy')
    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot_paddy, annot=True, fmt='.3f', cmap='Greens', vmin=0.0, vmax=1.0)
    plt.title("田んぼ分類精度マトリクス")
    plt.ylabel("経過時間 (h)")
    plt.xlabel("月")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "heatmap_rf_paddy_accuracy.png")

    # Also Train Accuracy for reference
    pivot_train = df.pivot(index='delay', columns='month', values='train_accuracy')
    plt.figure(figsize=(10, 8))
    sns.heatmap(pivot_train, annot=True, fmt='.3f', cmap='magma', vmin=0.8, vmax=1.0)
    plt.title("学習精度マトリクス (月 vs 経過時間)")
    plt.ylabel("経過時間 (h)")
    plt.xlabel("月")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "heatmap_rf_train_accuracy.png")

if __name__ == "__main__":
    main()
