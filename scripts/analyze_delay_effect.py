import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats

# --- 設定 ---
BASE_DIR = r"D:\sotsuron"
RESULT_ROOT = os.path.join(BASE_DIR, "result")
OUTPUT_IMG = os.path.join(BASE_DIR, "delay_analysis_plot.png")
OUTPUT_CSV = os.path.join(BASE_DIR, "all_grids_combined.csv")

def analyze_delay():
    # 1. 全CSVファイルの探索
    search_pattern = os.path.join(RESULT_ROOT, "**", "*_values.csv")
    csv_files = glob.glob(search_pattern, recursive=True)
    
    if not csv_files:
        print("エラー: CSVファイルが見つかりません。")
        return

    print(f"{len(csv_files)} 個のファイルを読み込み中...")

    # 2. データの結合
    df_list = []
    for f in csv_files:
        try:
            tmp = pd.read_csv(f)
            df_list.append(tmp)
        except Exception as e:
            print(f"Skipping {os.path.basename(f)}: {e}")
    
    if not df_list:
        print("有効なデータがありません。")
        return

    df_all = pd.concat(df_list, ignore_index=True)
    
    # 統合データを保存（バックアップ用）
    df_all.to_csv(OUTPUT_CSV, index=False)
    print(f"統合データを保存しました: {OUTPUT_CSV}")
    print(f"総データ数: {len(df_all)} ペア")

    # 3. 解析と可視化
    # Delayが極端に大きい外れ値があれば除外することも検討（今回はそのまま）
    # df_all = df_all[df_all['delay_hours'] < 2.0] 
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # --- Road Analysis ---
    ax_road = axes[0]
    data_road = df_all.dropna(subset=['delay_hours', 'road_ratio'])
    
    if len(data_road) > 0:
        x = data_road['delay_hours']
        y = data_road['road_ratio']
        
        # 散布図
        ax_road.scatter(x, y, alpha=0.5, label='Data Points')
        
        # 回帰直線
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        ax_road.plot(x, slope*x + intercept, color='red', label=f'Fit (R={r_value:.3f})')
        
        ax_road.set_title(f"Road: Delay vs Decrease Ratio\n(n={len(data_road)})")
        ax_road.set_xlabel("Delay (Hours)")
        ax_road.set_ylabel("Decrease Ratio")
        ax_road.grid(True, linestyle='--', alpha=0.7)
        ax_road.legend()
        
        print("-" * 30)
        print(f"[Road] 相関係数 (R): {r_value:.4f}")
        print(f"[Road] P値: {p_value:.4e}")
    else:
        ax_road.set_title("Road: No Data")

    # --- Paddy Analysis ---
    ax_paddy = axes[1]
    data_paddy = df_all.dropna(subset=['delay_hours', 'paddy_ratio'])
    
    if len(data_paddy) > 0:
        x = data_paddy['delay_hours']
        y = data_paddy['paddy_ratio']
        
        # 散布図
        ax_paddy.scatter(x, y, color='green', alpha=0.5, label='Data Points')
        
        # 回帰直線
        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
        ax_paddy.plot(x, slope*x + intercept, color='darkgreen', linestyle='--', label=f'Fit (R={r_value:.3f})')
        
        ax_paddy.set_title(f"Paddy: Delay vs Decrease Ratio\n(n={len(data_paddy)})")
        ax_paddy.set_xlabel("Delay (Hours)")
        ax_paddy.set_ylabel("Decrease Ratio")
        ax_paddy.grid(True, linestyle='--', alpha=0.7)
        ax_paddy.legend()
        
        print("-" * 30)
        print(f"[Paddy] 相関係数 (R): {r_value:.4f}")
        print(f"[Paddy] P値: {p_value:.4e}")
    else:
        ax_paddy.set_title("Paddy: No Data")

    plt.tight_layout()
    plt.savefig(OUTPUT_IMG)
    print("-" * 30)
    print(f"グラフを保存しました: {OUTPUT_IMG}")
    # plt.show() # 必要ならコメントアウトを外してください

if __name__ == "__main__":
    analyze_delay()