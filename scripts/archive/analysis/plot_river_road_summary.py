import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from pathlib import Path

# Set Japanese font
# Try common Japanese fonts
font_list = ['Meiryo', 'MS Gothic', 'Yu Gothic', 'sans-serif']
mpl.rcParams['font.family'] = font_list

DATA_FILE = Path(r"d:/sotsuron/rainsar-hub/data/result/River_vs_Road_Oct/metrics.csv")
OUTPUT_DIR = Path(r"d:/sotsuron/rainsar-hub/data/result/River_vs_Road_Oct")

def plot_results():
    if not DATA_FILE.exists():
        print(f"Data file not found: {DATA_FILE}")
        return

    df = pd.read_csv(DATA_FILE)
    
    # 1. Confusion Matrix Grid (Summary)
    # We have delay 0-5. Let's make a 2x3 grid.
    
    # Define grid size
    rows = 2
    cols = 3
    fig, axes = plt.subplots(rows, cols, figsize=(15, 10))
    fig.suptitle("River vs Road (Oct) 混同行列まとめ", fontsize=20, y=0.98)
    axes = axes.flatten()
    
    delays = sorted(df['delay'].unique())
    
    for i, delay in enumerate(delays):
        if i >= len(axes): break
        
        ax = axes[i]
        row = df[df['delay'] == delay].iloc[0]
        
        # Load CM values
        tn = int(row['tn'])
        fp = int(row['fp'])
        fn = int(row['fn'])
        tp = int(row['tp'])
        
        cm = np.array([[tn, fp], [fn, tp]])
        acc = row['accuracy']
        
        # Plot Heatmap
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', ax=ax, cbar=False,
                    annot_kws={"size": 14},
                    xticklabels=['河川', '道路'],
                    yticklabels=['河川', '道路'])
        
        ax.set_title(f"Delay {delay}h (Acc: {acc:.3f})")
        
        # Labels only on edge plots for cleaner look
        if i % cols == 0:
            ax.set_ylabel("正解")
        else:
            ax.set_ylabel("")
            
        if i >= (rows - 1) * cols:
            ax.set_xlabel("予測")
        else:
            ax.set_xlabel("")
            
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    cm_save_path = OUTPUT_DIR / "cm_summary_river_road_oct.png"
    plt.savefig(cm_save_path)
    print(f"Saved: {cm_save_path}")
    plt.close()

    # 2. Line Plot (Accuracy, etc could be added if F1 was here, but just Accuracy for now)
    plt.figure(figsize=(10, 6))
    
    plt.plot(df['delay'], df['accuracy'], marker='o', linewidth=2, label='Accuracy')
    
    plt.title("River vs Road (Oct) 分類精度の推移", fontsize=16)
    plt.xlabel("Delay (時間)", fontsize=14)
    plt.ylabel("Accuracy", fontsize=14)
    plt.ylim(0.5, 1.0)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    # Annotate points
    for idx, row in df.iterrows():
        plt.annotate(f"{row['accuracy']:.3f}", 
                     (row['delay'], row['accuracy']),
                     textcoords="offset points", 
                     xytext=(0, 10), 
                     ha='center')
    
    line_save_path = OUTPUT_DIR / "accuracy_trend_river_road_oct.png"
    plt.savefig(line_save_path)
    print(f"Saved: {line_save_path}")
    plt.close()

if __name__ == "__main__":
    plot_results()
