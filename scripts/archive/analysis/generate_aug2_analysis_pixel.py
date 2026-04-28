
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sqlalchemy import create_engine, text
from sklearn.model_selection import GroupKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, confusion_matrix
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

# Sample Size per Event/Class (Downsampling for Loading)
# We load slightly more to have freedom to balance later
MAX_SAMPLES_PER_CLASS = 2000

# DB Config
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

def load_target_events():
    print("Loading event list...")
    if not EXISTING_CSV.exists():
        print("Error: CSV not found")
        return None
    
    df = pd.read_csv(EXISTING_CSV)
    
    # 1. Basic Parse
    df['delay_h'] = df['event_name'].apply(lambda x: float(x.split('_')[1].replace('h', '')) if len(x.split('_')) > 1 else -1)
    df['month'] = df['event_name'].apply(lambda x: int(x.split('_')[2][4:6]) if len(x.split('_')) > 2 else 0)
    
    # 2. Filter August
    df_aug = df[df['month'] == 8].copy()
    
    # 3. Link DB Rain
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
        
    # Merge
    df_rain = df_rain.sort_values('total_precip', ascending=False).groupby(['grid_id', 'date_str']).first().reset_index()
    df_merged = pd.merge(df_aug, df_rain, on=['grid_id', 'date_str'], how='inner')
    
    # 4. Filter
    df_merged['duration_h'] = df_merged['duration_h'].replace(0, 1)
    df_merged['avg_intensity'] = df_merged['total_precip'] / df_merged['duration_h']
    
    # Intensity >= 10
    df_mid = df_merged[df_merged['avg_intensity'] >= 10.0].copy()
    
    # Pixel Either > 0
    paddy_col = 'paddy_diff_count' if 'paddy_diff_count' in df_mid.columns else 'paddy_count'
    road_col = 'road_diff_count' if 'road_diff_count' in df_mid.columns else 'road_count'
    
    if paddy_col in df_mid.columns and road_col in df_mid.columns:
        df_final = df_mid[(df_mid[paddy_col] > 0) | (df_mid[road_col] > 0)].copy()
    else:
        df_final = df_mid.copy()
        
    print(f"Target Events: {len(df_final)}")
    return df_final

def load_pixel_data(event_list_df):
    print("Loading pixel data from CSV files...")
    all_pixels = []
    total_events = len(event_list_df)
    loaded_events = 0
    
    for idx, row in event_list_df.iterrows():
        grid_id = row['grid_id']
        event_name = row['event_name']
        pixel_csv = DIFF_PIXEL_DIR / grid_id / event_name / "diff_pixel_values.csv"
        
        if not pixel_csv.exists(): continue
            
        try:
            df_pix = pd.read_csv(pixel_csv)
            if df_pix.empty: continue
            
            paddy = df_pix[df_pix['land_type'] == 'paddy']
            road = df_pix[df_pix['land_type'] == 'road']
            
            # Simple cap loading
            if len(paddy) > MAX_SAMPLES_PER_CLASS:
                paddy = paddy.sample(n=MAX_SAMPLES_PER_CLASS, random_state=42)
            if len(road) > MAX_SAMPLES_PER_CLASS:
                road = road.sample(n=MAX_SAMPLES_PER_CLASS, random_state=42)
                
            combined = pd.concat([paddy, road])
            combined['grid_id'] = grid_id
            combined['delay_h'] = row['delay_h']
            combined['total_precip'] = row['total_precip']
            combined['duration'] = row['duration_h']
            
            all_pixels.append(combined)
            loaded_events += 1
            
        except: pass
        if loaded_events % 50 == 0: print(f"  Loaded {loaded_events}/{total_events} events...")
            
    if not all_pixels: return None
    master_df = pd.concat(all_pixels, ignore_index=True)
    master_df['label'] = master_df['land_type'].map({'paddy': 0, 'road': 1})
    
    print(f"Total Pixels: {len(master_df)}")
    print(f"  Paddy: {len(master_df[master_df['label']==0])}")
    print(f"  Road:  {len(master_df[master_df['label']==1])}")
    return master_df

def run_rf_pixel_analysis_cv(df):
    print("\nRunning RF Analysis (GroupKFold CV + Balancing)...")
    
    feature_cols = ['diff_db', 'total_precip', 'duration']
    df['delay_int'] = df['delay_h'].apply(lambda x: int(x) if x < 12 else 11)
    unique_delays = sorted(df['delay_int'].unique())
    
    results = []
    cm_data = []
    
    # Setup CV
    gkf = GroupKFold(n_splits=5)
    
    # We iterate by delay first, then do CV inside?
    # Or strict CV globally?
    # classify_pixel_level.py did "Train models for each delay".
    # So we split dataset by delay, then do CV within that delay subset?
    # But small delays (e.g. 7-8h with 6 events) can't do 5-fold CV (requires >= 5 groups).
    # If groups < 5, we use Leave-One-Group-Out or max splits.
    
    for d in unique_delays:
        df_d = df[df['delay_int'] == d].copy()
        groups = df_d['grid_id'].values
        n_groups = df_d['grid_id'].nunique()
        
        if n_groups < 2:
            print(f"Delay {d}h: Skip (Only {n_groups} grids)")
            continue
            
        # Dynamically adjust splits for small data
        n_splits = min(5, n_groups)
        kf = GroupKFold(n_splits=n_splits)
        
        # Accumulate metrics across folds
        fold_cms = []
        y_true_all = []
        y_pred_all = []
        
        print(f"Delay {d}h (Grids={n_groups}, Splits={n_splits})...")
        
        fold_idx = 0
        for train_idx, val_idx in kf.split(df_d, groups=groups):
            fold_df_train = df_d.iloc[train_idx]
            fold_df_val = df_d.iloc[val_idx]
            
            # --- BALANCE TRAINING DATA ---
            paddy_train = fold_df_train[fold_df_train['label'] == 0]
            road_train = fold_df_train[fold_df_train['label'] == 1]
            
            n_min = min(len(paddy_train), len(road_train))
            if n_min < 10: continue # Skip this fold if empty class
            
            paddy_bal = paddy_train.sample(n=n_min, random_state=42)
            road_bal = road_train.sample(n=n_min, random_state=42)
            
            train_bal = pd.concat([paddy_bal, road_bal]).sample(frac=1, random_state=42)
            
            X_train = train_bal[feature_cols]
            y_train = train_bal['label']
            X_val = fold_df_val[feature_cols]
            y_val = fold_df_val['label']
            
            model = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1)
            model.fit(X_train, y_train)
            
            y_pred = model.predict(X_val)
            y_true_all.extend(y_val)
            y_pred_all.extend(y_pred)
            fold_idx += 1
            
        if not y_true_all:
            continue
            
        # Aggregate Results for this Delay
        final_acc = accuracy_score(y_true_all, y_pred_all)
        cm = confusion_matrix(y_true_all, y_pred_all, labels=[0, 1])
        tn, fp, fn, tp = cm.ravel()
        
        acc_road = tp / (tp + fn) if (tp + fn) > 0 else 0
        acc_paddy = tn / (tn + fp) if (tn + fp) > 0 else 0
        
        results.append({
            'delay': d,
            'test_accuracy': final_acc,
            'test_acc_road': acc_road,
            'test_acc_paddy': acc_paddy,
            'n_samples': len(y_true_all),
            'tn': tn, 'fp': fp, 'fn': fn, 'tp': tp
        })
        cm_data.append({'delay': d, 'cm': cm, 'acc': final_acc, 'n': len(y_true_all)})
        print(f"  -> Acc={final_acc:.3f} (Road={acc_road:.3f}, Paddy={acc_paddy:.3f})")

    # Save Results
    pd.DataFrame(results).to_csv(OUTPUT_DIR / "8月_分類結果サマリー.csv", index=False)
    
    # Plot Accuracy
    if results:
        res_df = pd.DataFrame(results)
        plt.figure(figsize=(10, 6))
        plt.plot(res_df['delay'], res_df['test_accuracy'], 'o-', label='全体精度', linewidth=2)
        plt.plot(res_df['delay'], res_df['test_acc_road'], 's--', label='道路精度', alpha=0.7)
        plt.plot(res_df['delay'], res_df['test_acc_paddy'], '^--', label='田んぼ精度', alpha=0.7)
        plt.xlabel('経過時間 (h)')
        plt.ylabel('精度')
        plt.title('8月_分類精度推移 (ピクセルベース, 1:1学習, CV)')
        plt.ylim(0, 1.05)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.savefig(OUTPUT_DIR / "8月_分類精度推移.png")
        plt.close()
        
    # Plot CM
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
            ax.set_ylabel('実測')
            ax.set_xlabel('予測')
            
        for j in range(len(cm_data), len(axes)):
            axes[j].axis('off')
            
        plt.tight_layout()
        plt.savefig(OUTPUT_DIR / "8月_混同行列.png")
        plt.close()

def main():
    events_df = load_target_events()
    if events_df is None: return
    pixel_df = load_pixel_data(events_df)
    if pixel_df is not None:
        run_rf_pixel_analysis_cv(pixel_df)

if __name__ == "__main__":
    main()
