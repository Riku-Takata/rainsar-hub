
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
OUTPUT_DIR = BASE_DIR / "data/result/Aug2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Load all pixels (no cap)
LOAD_PER_FILE_CAP = None

# DB Config
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

def load_target_events():
    print("Loading target event list...")
    if not EXISTING_CSV.exists():
        return None
    
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
    
    # Filter
    df_merged['duration_h'] = df_merged['duration_h'].replace(0, 1)
    df_merged['avg_intensity'] = df_merged['total_precip'] / df_merged['duration_h']
    df_final = df_merged[df_merged['avg_intensity'] >= 10.0].copy()
    
    # Pixel check (Either > 0)
    paddy_col = 'paddy_diff_count' if 'paddy_diff_count' in df_final.columns else 'paddy_count'
    road_col = 'road_diff_count' if 'road_diff_count' in df_final.columns else 'road_count'
    if paddy_col in df_final.columns:
        df_final = df_final[(df_final[paddy_col] > 0) | (df_final[road_col] > 0)].copy()
        
    print(f"Target Events: {len(df_final)}")
    
    # Discrete Delay Bin
    df_final['delay_int'] = df_final['delay_h'].apply(lambda x: int(x) if x < 12 else 11)
    return df_final

def run_balanced_analysis(events_df):
    results = []
    cm_data = [] # (delay, cm, acc)
    
    unique_delays = sorted(events_df['delay_int'].unique())
    print(f"Processing Delays: {unique_delays}")
    
    for d in unique_delays:
        print(f"\nProcessing Delay {d}h...")
        
        # 1. Identify Events
        target_events = events_df[events_df['delay_int'] == d]
        if target_events.empty: continue
        
        # 2. Collect Pixels
        pixels_list = []
        
        for _, row in target_events.iterrows():
            grid_id = row['grid_id']
            event_name = row['event_name']
            csv_path = DIFF_PIXEL_DIR / grid_id / event_name / "diff_pixel_values.csv"
            
            if not csv_path.exists(): continue
            try:
                # Load with cap
                df_p = pd.read_csv(csv_path)
                if df_p.empty: continue
                
                # Sample a chunk to avoid one file dominating, but large enough
                if LOAD_PER_FILE_CAP and len(df_p) > LOAD_PER_FILE_CAP:
                    df_p = df_p.sample(n=LOAD_PER_FILE_CAP, random_state=42)
                
                df_p['total_precip'] = row['total_precip']
                df_p['duration'] = row['duration_h']
                pixels_list.append(df_p)
            except: pass
            
        if not pixels_list:
            print(f"  No pixel data found for Delay {d}h")
            continue
            
        df_pixels = pd.concat(pixels_list, ignore_index=True)
        df_pixels['label'] = df_pixels['land_type'].map({'paddy': 0, 'road': 1})
        
        # 3. Balance (Global for this Delay)
        paddy_df = df_pixels[df_pixels['label'] == 0]
        road_df = df_pixels[df_pixels['label'] == 1]
        
        n_paddy = len(paddy_df)
        n_road = len(road_df)
        
        print(f"  Raw Counts: Paddy={n_paddy}, Road={n_road}")
        
        if n_paddy < 10 or n_road < 10:
            print("  Skipping (Insufficient classes)")
            continue
            
        min_count = min(n_paddy, n_road)
        
        paddy_bal = paddy_df.sample(n=min_count, random_state=42)
        road_bal = road_df.sample(n=min_count, random_state=42)
        
        df_bal = pd.concat([paddy_bal, road_bal]).sample(frac=1, random_state=42)
        print(f"  Balanced Count: {len(df_bal)} ({min_count} per class)")
        
        # 4. Split 80/20 (Pixel-based)
        X = df_bal[['diff_db', 'total_precip', 'duration']]
        y = df_bal['label']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        
        # 5. Train & Verify
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
            'train_samples': len(X_train),
            'test_samples': len(X_test),
            'tn': tn, 'fp': fp, 'fn': fn, 'tp': tp
        })
        cm_data.append({'delay': d, 'cm': cm, 'acc': acc, 'n': len(X_test)})

    # Save outputs
    if results:
        pd.DataFrame(results).to_csv(OUTPUT_DIR / "8月_分類結果サマリー.csv", index=False)
        
        # Plot Acc
        res_df = pd.DataFrame(results)
        plt.figure(figsize=(10, 6))
        plt.plot(res_df['delay'], res_df['test_accuracy'], 'o-', label='全体精度', linewidth=2)
        plt.plot(res_df['delay'], res_df['test_acc_road'], 's--', label='道路精度', alpha=0.7)
        plt.plot(res_df['delay'], res_df['test_acc_paddy'], '^--', label='田んぼ精度', alpha=0.7)
        plt.xlabel('経過時間 (h)')
        plt.ylabel('精度')
        plt.title('8月_分類精度推移 (バランス・ピクセル分割)')
        plt.ylim(0, 1.05)
        plt.axhline(0.5, color='gray', linestyle='--', alpha=0.5)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "8月_分類精度推移.png")
        plt.close()
        
        # Plot CM
        cols = 4
        rows = int(np.ceil(len(cm_data) / cols))
        fig, axes = plt.subplots(rows, cols, figsize=(4*cols, 4*rows))
        if rows*cols == 1: axes = [axes]
        else: axes = axes.flatten()
        
        for i, item in enumerate(cm_data):
            ax = axes[i]
            sns.heatmap(item['cm'], annot=True, fmt='d', cmap='Blues', ax=ax,
                        xticklabels=['田んぼ', '道路'], yticklabels=['田んぼ', '道路'])
            ax.set_title(f"{item['delay']}h (Acc={item['acc']:.2f})")
            ax.set_ylabel('実測')
            ax.set_xlabel('予測')
            
        for j in range(len(cm_data), len(axes)):
            axes[j].axis('off')
            
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "8月_混同行列.png")
        plt.close()

if __name__ == "__main__":
    df = load_target_events()
    if df is not None:
        run_balanced_analysis(df)
