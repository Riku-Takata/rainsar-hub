import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# --- 設定 ---
BASE_DIR = r"D:\sotsuron"
RESULT_ROOT = os.path.join(BASE_DIR, "result")
OUTPUT_IMG_SCATTER = os.path.join(BASE_DIR, "trend_scatter_dB.png")
OUTPUT_IMG_BOX = os.path.join(BASE_DIR, "trend_boxplot_dB.png")

def analyze_trend():
    # 1. 全CSVの読み込み
    search_pattern = os.path.join(RESULT_ROOT, "**", "*_values.csv")
    csv_files = glob.glob(search_pattern, recursive=True)
    
    if not csv_files:
        print("エラー: CSVファイルが見つかりません。")
        return

    print(f"{len(csv_files)} ファイルを統合中...")
    
    df_list = []
    for f in csv_files:
        try:
            tmp = pd.read_csv(f)
            df_list.append(tmp)
        except Exception: pass
    
    if not df_list:
        print("データがありません。")
        return

    df = pd.concat(df_list, ignore_index=True)
    print(f"総データ数: {len(df)} ペア")
    
    # 2. データの整理 (外れ値除去など)
    # Delayが極端に大きいものは除外（例: 24時間以上など。必要に応じて調整）
    # df = df[df['delay_hours'] <= 12.0]

    # --- プロット1: 散布図と回帰直線 ---
    plt.figure(figsize=(12, 7))
    
    # 田んぼ (Red)
    if 'paddy_diff_mean' in df.columns:
        sns.regplot(
            data=df, x='delay_hours', y='paddy_diff_mean',
            color='red', label='Paddy (Farm)',
            scatter_kws={'alpha':0.5}, line_kws={'linestyle':'--'}
        )
        
    # 道路 (Blue)
    if 'road_diff_mean' in df.columns:
        sns.regplot(
            data=df, x='delay_hours', y='road_diff_mean',
            color='blue', label='Road',
            scatter_kws={'alpha':0.5}, line_kws={'linestyle':'--'}
        )

    plt.axhline(0, color='black', linestyle='-', linewidth=1, alpha=0.5)
    plt.title("Backscatter Mean Difference vs Delay Hours")
    plt.ylabel("Mean Difference (dB) [After - Before]\n(+) Increase, (-) Decrease")
    plt.xlabel("Delay (Hours)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(OUTPUT_IMG_SCATTER)
    print(f"散布図を保存しました: {OUTPUT_IMG_SCATTER}")

    # --- プロット2: 時間帯ごとの分布 (Boxplot) ---
    # Delayを「0-2h」「2-6h」「6h+」などに区分けして分布を見る
    try:
        df['delay_bin'] = pd.cut(df['delay_hours'], bins=[0, 2, 6, 12, 24, 100], labels=['0-2h', '2-6h', '6-12h', '12-24h', '24h+'])
        
        # データをLong形式に変換してSeabornで描画
        df_long = pd.melt(df, id_vars=['delay_bin'], value_vars=['road_diff_mean', 'paddy_diff_mean'], 
                          var_name='LandType', value_name='Difference_dB')
        
        plt.figure(figsize=(10, 6))
        sns.boxplot(data=df_long, x='delay_bin', y='Difference_dB', hue='LandType', palette={'road_diff_mean':'blue', 'paddy_diff_mean':'red'})
        plt.axhline(0, color='black', linestyle='-', linewidth=1)
        plt.title("Distribution of Backscatter Difference by Delay Time")
        plt.ylabel("Mean Difference (dB)")
        plt.xlabel("Delay Time Range")
        plt.grid(True, axis='y', alpha=0.3)
        plt.savefig(OUTPUT_IMG_BOX)
        print(f"箱ひげ図を保存しました: {OUTPUT_IMG_BOX}")
    except Exception as e:
        print(f"箱ひげ図の作成スキップ: {e}")

if __name__ == "__main__":
    analyze_trend()