
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
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
OUTPUT_DIR = BASE_DIR / "data/result/Aug2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# DB Config
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

def load_and_filter_data():
    print("Loading data...")
    if not EXISTING_CSV.exists():
        print("Error: CSV not found")
        return None
    
    df = pd.read_csv(EXISTING_CSV)
    
    # 1. Basic Parse
    df['delay_h'] = df['event_name'].apply(lambda x: float(x.split('_')[1].replace('h', '')) if len(x.split('_')) > 1 else -1)
    df['month'] = df['event_name'].apply(lambda x: int(x.split('_')[2][4:6]) if len(x.split('_')) > 2 else 0)
    
    # 2. Filter August
    df_aug = df[df['month'] == 8].copy()
    print(f"Original August Events: {len(df_aug)}")
    
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
    
    # 4. Filter Intensity >= 10
    df_merged['duration_h'] = df_merged['duration_h'].replace(0, 1)
    df_merged['avg_intensity'] = df_merged['total_precip'] / df_merged['duration_h']
    
    df_mid = df_merged[df_merged['avg_intensity'] >= 10.0].copy()
    print(f"Intensity Filtered: {len(df_mid)}")
    
    # 5. Filter Pixel (Either > 0)
    # Check column names
    paddy_col = 'paddy_diff_count' if 'paddy_diff_count' in df_mid.columns else 'paddy_count'
    road_col = 'road_diff_count' if 'road_diff_count' in df_mid.columns else 'road_count'
    
    if paddy_col in df_mid.columns and road_col in df_mid.columns:
        df_final = df_mid[(df_mid[paddy_col] > 0) | (df_mid[road_col] > 0)].copy()
        
        # Determine actual available pixels for later
        df_final['has_paddy'] = df_final[paddy_col] > 0
        df_final['has_road'] = df_final[road_col] > 0
    else:
        df_final = df_mid.copy()
        df_final['has_paddy'] = True
        df_final['has_road'] = True
        
    print(f"Final Count: {len(df_final)}")
    
    # Save List
    list_df = df_final[['grid_id', 'date_str', 'total_precip', 'duration_h', 'delay_h']].copy()
    list_df['イベントID'] = list_df['grid_id'] + '_' + list_df['date_str']
    list_df.to_csv(OUTPUT_DIR / "8月_降雨イベント一覧.csv", index=False, encoding='utf-8-sig')
    
    # Save Delay Bias
    bins = list(range(0, 13))
    df_final['Delay_Bin'] = pd.cut(df_final['delay_h'], bins=bins, labels=False, include_lowest=True) # 0-11
    
    # Need to match expected format?
    # delay, n_samples (pixels), n_events
    bias_stats = []
    for d in range(12):
        grp = df_final[df_final['Delay_Bin'] == d]
        n_ev = len(grp)
        n_sm = grp[paddy_col].sum() + grp[road_col].sum() if paddy_col in grp else 0
        bias_stats.append({'delay': d, 'n_samples': int(n_sm), 'n_events': n_ev})
        
    pd.DataFrame(bias_stats).to_csv(OUTPUT_DIR / "8月_全Delay構成バイアス.csv", index=False)
    
    return df_final

def run_rf_analysis(df):
    print("Running RF Analysis...")
    
    # Prepare Long Format (Paddy Row + Road Row)
    records = []
    
    for _, row in df.iterrows():
        # Paddy
        if row.get('has_paddy', False):
            records.append({
                'grid_id': row['grid_id'],
                'delay_h': row['delay_h'],
                'diff_mean': row['paddy_diff_mean'],
                'total_precip': row['total_precip'],
                'duration': row['duration_h'],
                'label': 0 # Paddy
            })
        # Road
        if row.get('has_road', False):
            records.append({
                'grid_id': row['grid_id'],
                'delay_h': row['delay_h'],
                'diff_mean': row['road_diff_mean'],
                'total_precip': row['total_precip'],
                'duration': row['duration_h'],
                'label': 1 # Road
            })
            
    df_ml = pd.DataFrame(records)
    print(f"ML Samples: {len(df_ml)}")
    
    # Split Grids
    unique_grids = df_ml['grid_id'].unique()
    train_grids, val_grids = train_test_split(unique_grids, test_size=0.2, random_state=42)
    
    df_train = df_ml[df_ml['grid_id'].isin(train_grids)]
    df_val = df_ml[df_ml['grid_id'].isin(val_grids)]
    
    # Train per Delay
    feature_cols = ['diff_mean', 'total_precip', 'duration']
    
    delays = sorted(df_ml['delay_h'].unique())
    # Group delays into integer bins for plotting
    df_ml['delay_int'] = df_ml['delay_h'].apply(lambda x: int(x) if x < 12 else 11)
    
    results = []
    
    # For CM Plotting, we need a list of (delay, cm, accuracy)
    cm_data = []
    
    unique_int_delays = sorted(df_ml['delay_int'].unique())
    
    for d in unique_int_delays:
        # Filter train/val by integer delay bin
        # Note: Previous script did exact match on delay_h float?
        # classify_by_delay.py: for delay in sorted(df_all['delay_hours'].unique())
        # But if delay is float (7.0, 7.5), we might have too few samples per exact delay.
        # User asked for matrix like previous analysis. Previous analysis had 12 bins?
        # Let's bin by integer for robustness, OR proceed with float if data allows.
        # Given small data, integer binning is safer.
        
        train_d = df_train[df_train['delay_h'].apply(int) == d]
        val_d = df_val[df_val['delay_h'].apply(int) == d]
        
        if len(train_d) < 5 or len(val_d) < 2:
            print(f"Skipping Delay {d}h (Insufficient data: Train={len(train_d)}, Val={len(val_d)})")
            continue
            
        X_train = train_d[feature_cols]
        y_train = train_d['label']
        X_val = val_d[feature_cols]
        y_val = val_d['label']
        
        model = RandomForestClassifier(n_estimators=100, random_state=42, max_depth=5)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_val)
        acc = accuracy_score(y_val, y_pred)
        
        cm = confusion_matrix(y_val, y_pred, labels=[0, 1])
        # labels: 0=Paddy, 1=Road
        # cm structure:
        # [[TN (Paddy->Paddy), FP (Paddy->Road)],
        #  [FN (Road->Paddy),  TP (Road->Road)]]
        # Wait:
        # y_true=0 (Paddy), y_pred=0 -> Correct Paddy detection? 
        # Usually target class is 1. If Road is target:
        # TP = Road predicted as Road
        # TN = Paddy predicted as Paddy
        # FP = Paddy predicted as Road
        # FN = Road predicted as Paddy
        
        tn, fp, fn, tp = cm.ravel()
        
        results.append({
            'delay': d,
            'test_accuracy': acc,
            'train_accuracy': model.score(X_train, y_train),
            'test_samples': len(val_d),
            'tn': tn, 'fp': fp, 'fn': fn, 'tp': tp
        })
        
        cm_data.append({
            'delay': d,
            'cm': cm,
            'acc': acc,
            'n': len(val_d)
        })
        
    # Save Metrics
    pd.DataFrame(results).to_csv(OUTPUT_DIR / "8月_分類結果サマリー.csv", index=False)
    
    # Plot Accuracy
    if results:
        res_df = pd.DataFrame(results)
        plt.figure(figsize=(10, 6))
        plt.plot(res_df['delay'], res_df['test_accuracy'], 'o-', label='検証精度')
        plt.plot(res_df['delay'], res_df['train_accuracy'], 's--', label='学習精度', alpha=0.5)
        plt.xlabel('経過時間 (h)')
        plt.ylabel('精度')
        plt.title('8月_分類精度推移 (Aug2)')
        plt.ylim(0, 1.1)
        plt.grid(True)
        plt.legend()
        plt.savefig(OUTPUT_DIR / "8月_分類精度推移.png")
        plt.close()
        
    # Plot Confusion Matrix
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
        
    print("RF Analysis Completed.")

def main():
    df = load_and_filter_data()
    if df is not None:
        run_rf_analysis(df)

if __name__ == "__main__":
    main()
