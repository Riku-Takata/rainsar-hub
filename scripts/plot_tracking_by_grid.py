import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# --- 設定 ---
BASE_DIR = r"D:\sotsuron"
RESULT_ROOT = os.path.join(BASE_DIR, "result")
OUTPUT_DIR = os.path.join(BASE_DIR, "tracking_plots_by_grid")

# ★変化判定の閾値
# 時間経過による変動幅(Max - Min)がこれ未満なら「変化なし」としてグラフを作らない
MIN_CHANGE_THRESHOLD_DB = 0.5 

def plot_tracking_by_grid():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"出力フォルダ: {OUTPUT_DIR}")

    files = glob.glob(os.path.join(RESULT_ROOT, "**", "*_tracked.csv"), recursive=True)
    if not files:
        print("エラー: CSVファイル (*_tracked.csv) が見つかりません。")
        return
        
    df_list = []
    for f in files:
        try: df_list.append(pd.read_csv(f))
        except: pass
    
    if not df_list:
        print("有効なデータがありません。")
        return

    df_all = pd.concat(df_list, ignore_index=True)
    unique_grids = df_all['grid_id'].unique()
    print(f"検出Grid数: {len(unique_grids)}")

    saved_count = 0
    skipped_count = 0

    for grid_id in unique_grids:
        group = df_all[df_all['grid_id'] == grid_id]
        
        # データ点数が少なすぎる場合はスキップ
        if len(group) < 2:
            continue

        # --- 「変化があるか」のチェック ---
        is_dynamic = False
        
        # 道路の変化チェック
        if 'road_tracked_mean' in group.columns:
            vals = group['road_tracked_mean'].dropna()
            if not vals.empty:
                # 変動幅 (Max - Min) が閾値を超えているか？
                if (vals.max() - vals.min()) >= MIN_CHANGE_THRESHOLD_DB:
                    is_dynamic = True
        
        # 田んぼの変化チェック (道路がダメでも田んぼに変化があれば描画する)
        if not is_dynamic and 'paddy_tracked_mean' in group.columns:
            vals = group['paddy_tracked_mean'].dropna()
            if not vals.empty:
                if (vals.max() - vals.min()) >= MIN_CHANGE_THRESHOLD_DB:
                    is_dynamic = True

        # どちらにも有意な変化がなければスキップ
        if not is_dynamic:
            skipped_count += 1
            # print(f"[{grid_id}] 変化が乏しいためスキップ")
            continue

        # --- 描画処理 ---
        print(f"[{grid_id}] Plotting...")
        plt.figure(figsize=(10, 6))
        
        # データ点数に応じて近似曲線の次数を調整
        n_samples = len(group)
        order = 2 if n_samples >= 5 else 1
        
        # Road (Blue)
        if 'road_tracked_mean' in group.columns:
            valid_road = group.dropna(subset=['road_tracked_mean'])
            if len(valid_road) > 1:
                try:
                    sns.regplot(data=group, x='delay_hours', y='road_tracked_mean', 
                                color='blue', label='Tracked Road', order=order, scatter_kws={'alpha':0.6})
                except:
                    sns.scatterplot(data=group, x='delay_hours', y='road_tracked_mean', color='blue', label='Tracked Road')

        # Paddy (Red)
        if 'paddy_tracked_mean' in group.columns:
            valid_paddy = group.dropna(subset=['paddy_tracked_mean'])
            if len(valid_paddy) > 1:
                try:
                    sns.regplot(data=group, x='delay_hours', y='paddy_tracked_mean', 
                                color='red', label='Tracked Paddy', order=order, scatter_kws={'alpha':0.6})
                except:
                    sns.scatterplot(data=group, x='delay_hours', y='paddy_tracked_mean', color='red', label='Tracked Paddy')
        
        # Park (Green) - もしあれば
        if 'park_tracked_mean' in group.columns:
            valid_park = group.dropna(subset=['park_tracked_mean'])
            if len(valid_park) > 1:
                try:
                    sns.regplot(data=group, x='delay_hours', y='park_tracked_mean', 
                                color='green', label='Tracked Park', order=order, scatter_kws={'alpha':0.4, 'marker':'s'})
                except: pass

        # Airport (Purple) - もしあれば
        if 'airport_tracked_mean' in group.columns:
            valid_airport = group.dropna(subset=['airport_tracked_mean'])
            if len(valid_airport) > 1:
                try:
                    sns.regplot(data=group, x='delay_hours', y='airport_tracked_mean', 
                                color='purple', label='Tracked Airport', order=order, scatter_kws={'alpha':0.5, 'marker':'^'})
                except: pass

        plt.axhline(0, color='black', linestyle='-', linewidth=1)
        plt.title(f"Recovery Process: {grid_id}\n(Pixels with Initial Decrease <-1.0dB)")
        plt.ylabel("Mean Difference (dB)")
        plt.xlabel("Delay (Hours)")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # 保存
        out_path = os.path.join(OUTPUT_DIR, f"tracking_plot_{grid_id}.png")
        plt.savefig(out_path)
        plt.close()
        saved_count += 1
        
    print(f"\n完了: {saved_count} 枚のグラフを保存しました。(変化なしスキップ: {skipped_count} 件)")

if __name__ == "__main__":
    plot_tracking_by_grid()