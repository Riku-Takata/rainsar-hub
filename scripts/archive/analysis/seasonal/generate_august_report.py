"""
Generate August-focused analysis figures in Japanese for result/Aug folder
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
from pathlib import Path

# Japanese font
matplotlib.rcParams['font.family'] = 'MS Gothic'

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
OUTPUT_DIR = BASE_DIR / "data" / "result" / "Aug"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# 1. Monthly Event Counts (Japanese version)
# ============================================================
def plot_monthly_counts():
    delay_csv = BASE_DIR / "data" / "result" / "event_distribution" / "delay_values.csv"
    df = pd.read_csv(delay_csv)
    df['event_end_ts_utc'] = pd.to_datetime(df['event_end_ts_utc'])
    df['month'] = df['event_end_ts_utc'].dt.month
    
    counts = df.groupby('month').size()
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(counts.index, counts.values, color='steelblue', edgecolor='black')
    
    # Highlight August
    if 8 in counts.index:
        aug_idx = list(counts.index).index(8)
        bars[aug_idx].set_color('orange')
    
    plt.title('月別イベント数', fontsize=14)
    plt.xlabel('月', fontsize=12)
    plt.ylabel('イベント数', fontsize=12)
    plt.xticks(range(1, 13))
    plt.grid(axis='y', alpha=0.3)
    
    for i, v in enumerate(counts.values):
        plt.text(counts.index[i], v + 5, str(v), ha='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "月別イベント数.png", dpi=150)
    plt.close()
    print("Saved: 月別イベント数.png")

# ============================================================
# 2. Monthly Delay Boxplot (Japanese version)
# ============================================================
def plot_monthly_delays_boxplot():
    delay_csv = BASE_DIR / "data" / "result" / "event_distribution" / "delay_values.csv"
    df = pd.read_csv(delay_csv)
    df['event_end_ts_utc'] = pd.to_datetime(df['event_end_ts_utc'])
    df['month'] = df['event_end_ts_utc'].dt.month
    
    plt.figure(figsize=(12, 6))
    df.boxplot(column='delay_h', by='month', grid=False)
    plt.title('月別遅延時間分布', fontsize=14)
    plt.suptitle('')  # Remove default title
    plt.xlabel('月', fontsize=12)
    plt.ylabel('遅延時間 (h)', fontsize=12)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "月別遅延時間分布.png", dpi=150)
    plt.close()
    print("Saved: 月別遅延時間分布.png")

# ============================================================
# 3. August-only Pixel Heatmaps (Road & Paddy)
# ============================================================
def plot_august_pixel_heatmaps():
    # Load seasonal stats
    stats_csv = BASE_DIR / "data" / "result" / "seasonal" / "seasonal_stats_all.csv"
    if not stats_csv.exists():
        print("Stats CSV not found, skipping heatmaps")
        return
    
    df = pd.read_csv(stats_csv)
    df['delay_bin'] = df['delay_h'].astype(int)
    aug_df = df[(df['month'] == 8) & (df['pol'] == 'vv')]
    
    # Road pixels by delay
    road_df = aug_df[aug_df['type'] == 'road']
    road_pivot = road_df.groupby('delay_bin')['clean_pixel_count'].sum()
    
    plt.figure(figsize=(8, 6))
    if not road_pivot.empty:
        plt.barh(road_pivot.index, road_pivot.values, color='steelblue')
        plt.title('8月 道路ピクセル数 (経過時間別)', fontsize=14)
        plt.xlabel('ピクセル数', fontsize=12)
        plt.ylabel('経過時間 (h)', fontsize=12)
        plt.gca().invert_yaxis()
        for i, v in enumerate(road_pivot.values):
            plt.text(v + 1000, road_pivot.index[i], f'{int(v):,}', va='center', fontsize=9)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "8月_道路ピクセル数.png", dpi=150)
    plt.close()
    print("Saved: 8月_道路ピクセル数.png")
    
    # Paddy pixels by delay
    paddy_df = aug_df[aug_df['type'] == 'paddy']
    paddy_pivot = paddy_df.groupby('delay_bin')['clean_pixel_count'].sum()
    
    plt.figure(figsize=(8, 6))
    if not paddy_pivot.empty:
        plt.barh(paddy_pivot.index, paddy_pivot.values, color='forestgreen')
        plt.title('8月 田んぼピクセル数 (経過時間別)', fontsize=14)
        plt.xlabel('ピクセル数', fontsize=12)
        plt.ylabel('経過時間 (h)', fontsize=12)
        plt.gca().invert_yaxis()
        for i, v in enumerate(paddy_pivot.values):
            plt.text(v + 1000, paddy_pivot.index[i], f'{int(v):,}', va='center', fontsize=9)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "8月_田んぼピクセル数.png", dpi=150)
    plt.close()
    print("Saved: 8月_田んぼピクセル数.png")

# ============================================================
# 4. August RF Model Results (CM Grid + Accuracy Plot)
# ============================================================
def plot_august_rf_results():
    metrics_csv = BASE_DIR / "data" / "result" / "seasonal" / "rf_results" / "rf_accuracy_metrics.csv"
    if not metrics_csv.exists():
        print("Metrics CSV not found")
        return
    
    df = pd.read_csv(metrics_csv)
    aug_df = df[df['month'] == 8].sort_values('delay')
    
    if aug_df.empty:
        print("No August data in metrics")
        return
    
    # Accuracy line plot
    plt.figure(figsize=(10, 6))
    plt.plot(aug_df['delay'], aug_df['test_accuracy'], 'o-', linewidth=2, markersize=8, color='blue', label='テスト精度')
    plt.plot(aug_df['delay'], aug_df['train_accuracy'], 's--', linewidth=1, markersize=6, color='gray', alpha=0.7, label='学習精度')
    
    plt.title('8月 分類精度 (道路 vs 田んぼ)', fontsize=14)
    plt.xlabel('経過時間 (h)', fontsize=12)
    plt.ylabel('精度', fontsize=12)
    plt.ylim(0.4, 1.05)
    plt.xlim(-0.5, 11.5)
    plt.xticks(range(0, 12))
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Annotate peak
    peak_idx = aug_df['test_accuracy'].idxmax()
    peak_row = aug_df.loc[peak_idx]
    plt.annotate(f"ピーク: {peak_row['test_accuracy']:.3f}\n({int(peak_row['delay'])}時間)", 
                 xy=(peak_row['delay'], peak_row['test_accuracy']),
                 xytext=(peak_row['delay']+1, peak_row['test_accuracy']-0.08),
                 fontsize=10, color='red',
                 arrowprops=dict(arrowstyle='->', color='red', lw=1))
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "8月_分類精度推移.png", dpi=150)
    plt.close()
    print("Saved: 8月_分類精度推移.png")
    
    # Confusion Matrix Grid for August
    delays = sorted(aug_df['delay'].unique())
    cols = 4
    rows = int(np.ceil(len(delays) / cols))
    
    fig, axes = plt.subplots(rows, cols, figsize=(4*cols, 4*rows))
    axes = axes.flatten()
    
    labels = ["道路", "田んぼ"]
    
    for i, delay in enumerate(delays):
        ax = axes[i]
        row = aug_df[aug_df['delay'] == delay].iloc[0]
        
        tn = int(row['tn'])
        fp = int(row['fp'])
        fn = int(row['fn'])
        tp = int(row['tp'])
        
        cm = np.array([[tn, fp], [fn, tp]])
        acc = row['test_accuracy']
        n_test = int(row['test_samples'])
        
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", cbar=False, ax=ax,
                    xticklabels=labels, yticklabels=labels)
        
        ax.set_title(f"{delay}時間経過\n(n={n_test:,}, 精度={acc:.2f})")
        ax.set_ylabel("実測")
        ax.set_xlabel("予測")
    
    for j in range(len(delays), len(axes)):
        axes[j].axis('off')
    
    plt.suptitle("8月 混同行列 (道路 vs 田んぼ)", fontsize=16)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "8月_混同行列.png", dpi=150)
    plt.close()
    print("Saved: 8月_混同行列.png")

# ============================================================
# 5. Summary Table CSV
# ============================================================
def create_summary_csv():
    metrics_csv = BASE_DIR / "data" / "result" / "seasonal" / "rf_results" / "rf_accuracy_metrics.csv"
    df = pd.read_csv(metrics_csv)
    aug_df = df[df['month'] == 8][['delay', 'train_samples', 'test_samples', 'test_accuracy', 'test_acc_road', 'test_acc_paddy', 'tn', 'fp', 'fn', 'tp']].copy()
    aug_df.columns = ['経過時間', '学習サンプル数', 'テストサンプル数', 'テスト精度', '道路精度', '田んぼ精度', 'TN', 'FP', 'FN', 'TP']
    aug_df = aug_df.sort_values('経過時間')
    aug_df.to_csv(OUTPUT_DIR / "8月_分類結果サマリー.csv", index=False, encoding='utf-8-sig')
    print("Saved: 8月_分類結果サマリー.csv")

if __name__ == "__main__":
    plot_monthly_counts()
    plot_monthly_delays_boxplot()
    plot_august_pixel_heatmaps()
    plot_august_rf_results()
    create_summary_csv()
    print("\nAll August analysis figures saved to:", OUTPUT_DIR)
