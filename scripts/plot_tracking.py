import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# --- 設定 ---
BASE_DIR = r"D:\sotsuron"
RESULT_ROOT = os.path.join(BASE_DIR, "result")
OUTPUT_IMG = os.path.join(BASE_DIR, "tracking_lines_plot.png")

# 変化判定の閾値
MIN_CHANGE_THRESHOLD_DB = 0.5

def plot_tracking_lines():
    files = glob.glob(os.path.join(RESULT_ROOT, "**", "*_tracked.csv"), recursive=True)
    if not files:
        print("エラー: CSVファイル (*_tracked.csv) が見つかりません。")
        return
        
    df_list = []
    
    print("データを読み込み中...")
    for f in files:
        try:
            tmp_df = pd.read_csv(f)
            if len(tmp_df) < 2: continue 

            # 変化があるかチェック
            is_dynamic = False
            if 'road_tracked_mean' in tmp_df.columns:
                vals = tmp_df['road_tracked_mean'].dropna()
                if not vals.empty and (vals.max() - vals.min()) >= MIN_CHANGE_THRESHOLD_DB:
                    is_dynamic = True
            
            if not is_dynamic and 'paddy_tracked_mean' in tmp_df.columns:
                vals = tmp_df['paddy_tracked_mean'].dropna()
                if not vals.empty and (vals.max() - vals.min()) >= MIN_CHANGE_THRESHOLD_DB:
                    is_dynamic = True
            
            if is_dynamic:
                df_list.append(tmp_df)
                
        except Exception: pass
    
    if not df_list:
        print("有効なデータがありませんでした。")
        return

    df = pd.concat(df_list, ignore_index=True)
    print(f"描画対象: {len(df['grid_id'].unique())} グリッド")
    
    plt.figure(figsize=(12, 7))
    
    # --- 道路 (Blue) ---
    if 'road_tracked_mean' in df.columns:
        # 個別の線 (薄く表示)
        sns.lineplot(data=df, x='delay_hours', y='road_tracked_mean', 
                     units='grid_id', estimator=None, 
                     color='blue', alpha=0.15, linewidth=1)
        
        # 全体のトレンド (太い点線)
        # zorder引数を削除しました
        sns.regplot(data=df, x='delay_hours', y='road_tracked_mean', 
                    color='blue', scatter=False, order=2,
                    line_kws={'linewidth': 2.5, 'linestyle': '--', 'alpha': 1.0}, 
                    label='Road Trend')

    # --- 田んぼ (Red) ---
    if 'paddy_tracked_mean' in df.columns:
        # 個別の線 (薄く表示)
        sns.lineplot(data=df, x='delay_hours', y='paddy_tracked_mean', 
                     units='grid_id', estimator=None, 
                     color='red', alpha=0.15, linewidth=1)
        
        # 全体のトレンド (太い点線)
        # zorder引数を削除しました
        sns.regplot(data=df, x='delay_hours', y='paddy_tracked_mean', 
                    color='red', scatter=False, order=2,
                    line_kws={'linewidth': 2.5, 'linestyle': '--', 'alpha': 1.0}, 
                    label='Paddy Trend')
    
    plt.axhline(0, color='black', linestyle='-', linewidth=1)
    plt.title(f"Recovery Trajectories by Grid ID (Filtered > {MIN_CHANGE_THRESHOLD_DB}dB change)")
    plt.ylabel("Mean Difference (dB)")
    plt.xlabel("Delay (Hours)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.savefig(OUTPUT_IMG)
    print(f"グラフを保存しました: {OUTPUT_IMG}")

if __name__ == "__main__":
    plot_tracking_lines()