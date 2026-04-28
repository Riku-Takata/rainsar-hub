
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
import warnings
import os
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

# Japanese font
plt.rcParams['font.family'] = 'MS Gothic'

# Config
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
EXISTING_CSV = BASE_DIR / "data/result/vv/diff/all_events_diff_vv.csv"
DIFF_PIXEL_DIR = BASE_DIR / "data/result/vv/diff"
OUTPUT_DIR = BASE_DIR / "data/result/Aug2_Normalized"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# No per-file cap, we want to see the real totals first
LOAD_PER_FILE_CAP = None

# DB Config
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

def load_target_events():
    print("Loading target event list...")
    if not EXISTING_CSV.exists(): return None
    
    df = pd.read_csv(EXISTING_CSV)
    df['delay_h'] = df['event_name'].apply(lambda x: float(x.split('_')[1].replace('h', '')) if len(x.split('_')) > 1 else -1)
    df['month'] = df['event_name'].apply(lambda x: int(x.split('_')[2][4:6]) if len(x.split('_')) > 2 else 0)
    df_aug = df[df['month'] == 8].copy()
    
    # DB Link
    df_aug['date_str'] = df_aug['event_name'].apply(lambda x: pd.to_datetime(x.split('_')[2]).strftime('%Y-%m-%d'))
    grids = df_aug['grid_id'].unique().tolist()
    
    engine = create_engine(DATABASE_URL)
    query = text("""
        SELECT grid_id, DATE_FORMAT(end_ts_utc, '%Y-%m-%d') as date_str, 
               sum_gauge_mm_h as total_precip,
               TIMESTAMPDIFF(HOUR, start_ts_utc, end_ts_utc) as duration_h
        FROM gsmap_events
        WHERE grid_id IN :grids
        AND MONTH(end_ts_utc) = 8
    """)
    with engine.connect() as conn:
        df_rain = pd.read_sql(query, conn, params={"grids": tuple(grids)})
        
    df_rain = df_rain.sort_values('total_precip', ascending=False).groupby(['grid_id', 'date_str']).first().reset_index()
    df_merged = pd.merge(df_aug, df_rain, on=['grid_id', 'date_str'], how='inner')
    
    df_merged['duration_h'] = df_merged['duration_h'].replace(0, 1)
    df_merged['avg_intensity'] = df_merged['total_precip'] / df_merged['duration_h']
    
    # Filter 10mm
    df_final = df_merged[df_merged['avg_intensity'] >= 10.0].copy()
    
    # Pixel check
    paddy_col = 'paddy_diff_count' if 'paddy_diff_count' in df_final.columns else 'paddy_count'
    road_col = 'road_diff_count' if 'road_diff_count' in df_final.columns else 'road_count'
    if paddy_col in df_final.columns:
        df_final = df_final[(df_final[paddy_col] > 0) | (df_final[road_col] > 0)].copy()
        
    df_final['delay_int'] = df_final['delay_h'].apply(lambda x: int(x) if x < 12 else 11)
    print(f"Target Events: {len(df_final)}")
    return df_final

def run_normalized_analysis(events_df):
    unique_delays = sorted(events_df['delay_int'].unique())
    print(f"Delays to process: {unique_delays}")
    
    # 1. Collect Data & Find Global Minimum
    delay_data_map = {}
    
    print("Collecting data to determine normalization size...")
    for d in unique_delays:
        target_events = events_df[events_df['delay_int'] == d]
        if target_events.empty: continue
        
        pixels_list = []
        for _, row in target_events.iterrows():
            grid_id = row['grid_id']
            event_name = row['event_name']
            csv_path = DIFF_PIXEL_DIR / grid_id / event_name / "diff_pixel_values.csv"
            
            if not csv_path.exists(): continue
            try:
                df_p = pd.read_csv(csv_path)
                if df_p.empty: continue
                
                df_p['total_precip'] = row['total_precip']
                df_p['duration'] = row['duration_h']
                pixels_list.append(df_p)
            except: pass
            
        if not pixels_list:
            print(f"  Delay {d}h: No data.")
            continue
            
        df_pixels = pd.concat(pixels_list, ignore_index=True)
        df_pixels['label'] = df_pixels['land_type'].map({'paddy': 0, 'road': 1})
        
        # Calculate Balanced Count
        n_paddy = len(df_pixels[df_pixels['label'] == 0])
        n_road = len(df_pixels[df_pixels['label'] == 1])
        
        if n_paddy < 10 or n_road < 10:
            print(f"  Delay {d}h: Insufficient classes (P{n_paddy}/R{n_road}).")
            continue
            
        balanced_count = min(n_paddy, n_road)
        print(f"  Delay {d}h: Max Balanced Size = {balanced_count} (P{n_paddy}, R{n_road})")
        
        delay_data_map[d] = {
            'df': df_pixels, 
            'max_balanced': balanced_count,
            'n_paddy': n_paddy,
            'n_road': n_road
        }

    if not delay_data_map:
        print("No valid data.")
        return

    # Determine Global Min
    valid_counts = [v['max_balanced'] for v in delay_data_map.values()]
    global_min = min(valid_counts)
    print(f"\n>>> GLOBAL MINIMUM BALANCED SIZE: {global_min} (Standard for all delays) <<<\n")
    
    # 2. Train & Evaluate with Fixed Size
    results = []
    cm_data = [] 
    
    for d, data_info in delay_data_map.items():
        print(f"Analyzing Delay {d}h (Downsampling to {global_min})...")
        
        df_pixels = data_info['df']
        
        # Balance & Downsample to Global Min
        paddy = df_pixels[df_pixels['label'] == 0].sample(n=global_min, random_state=42)
        road = df_pixels[df_pixels['label'] == 1].sample(n=global_min, random_state=42)
        
        df_final = pd.concat([paddy, road]).sample(frac=1, random_state=42) # Total = 2 * global_min
        
        # Split 80/20
        X = df_final[['diff_db', 'total_precip', 'duration']]
        y = df_final['label']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        
        # Train
        model = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        
        acc = accuracy_score(y_test, y_pred)
        cm = confusion_matrix(y_test, y_pred, labels=[0, 1])
        tn, fp, fn, tp = cm.ravel()
        
        acc_road = tp / (tp + fn) if (tp + fn) > 0 else 0
        acc_paddy = tn / (tn + fp) if (tn + fp) > 0 else 0
        
        print(f"  Result: Acc={acc:.3f} (Road={acc_road:.3f}, Paddy={acc_paddy:.3f})")
        
        results.append({
            'delay': d,
            'test_accuracy': acc,
            'test_acc_road': acc_road,
            'test_acc_paddy': acc_paddy,
            'train_size_p_class': len(y_train)//2,
            'test_size_p_class': len(y_test)//2,
            'tn': tn, 'fp': fp, 'fn': fn, 'tp': tp
        })
        cm_data.append({'delay': d, 'cm': cm, 'acc': acc, 'n': len(X_test)})

    # Save
    pd.DataFrame(results).to_csv(OUTPUT_DIR / "8月_正規化分析サマリー.csv", index=False)
    
    # Plot
    if results:
        res_df = pd.DataFrame(results)
        plt.figure(figsize=(10, 6))
        plt.plot(res_df['delay'], res_df['test_accuracy'], 'o-', label='全体精度', linewidth=2)
        plt.plot(res_df['delay'], res_df['test_acc_road'], 's--', label='道路精度', alpha=0.7)
        plt.plot(res_df['delay'], res_df['test_acc_paddy'], '^--', label='田んぼ精度', alpha=0.7)
        plt.xlabel('経過時間 (h)')
        plt.ylabel('精度')
        plt.title(f'8月_分類精度推移 (サンプル数統一: {global_min}/class)')
        plt.ylim(0, 1.05)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.savefig(OUTPUT_DIR / "8月_正規化精度推移.png")
        plt.close()
        
    if cm_data:
        cols = 4
        rows = int(np.ceil(len(cm_data) / cols))
        fig, axes = plt.subplots(rows, cols, figsize=(4*cols, 4*rows))
        axes = axes.flatten() if rows*cols > 1 else [axes]
        for i, item in enumerate(cm_data):
            ax = axes[i]
            sns.heatmap(item['cm'], annot=True, fmt='d', cmap='Blues', ax=ax,
                        xticklabels=['田んぼ', '道路'], yticklabels=['田んぼ', '道路'])
            ax.set_title(f"{item['delay']}h (Acc={item['acc']:.2f})")
        for j in range(len(cm_data), len(axes)): axes[j].axis('off')
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "8月_正規化混同行列.png")
        plt.close()

if __name__ == "__main__":
    df = load_target_events()
    if df is not None:
        run_normalized_analysis(df)
