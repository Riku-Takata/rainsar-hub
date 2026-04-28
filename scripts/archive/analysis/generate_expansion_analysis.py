
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
EXPANSION_PIXEL_DIR = BASE_DIR / "data/expanded/samples"
OUTPUT_DIR = BASE_DIR / "data/result/Expansion"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Load roughly this many pixels per file (before mixing) to ensure diversity
# Final balancing happens at the Delay-Group level
LOAD_PER_FILE_CAP = None

# DB Config
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

def scan_expansion_events():
    """
    Scan data/expanded/samples to find processed events.
    Returns DataFrame: [grid_id, event_name, delay_h, total_precip, duration_h]
    We need to fetch rain data from DB again.
    """
    print("Scanning processed expansion events...")
    
    events_data = []
    
    if not EXPANSION_PIXEL_DIR.exists():
        print("No expansion data dir found.")
        return pd.DataFrame()

    grids = [d for d in EXPANSION_PIXEL_DIR.iterdir() if d.is_dir()]
    
    for g in grids:
        grid_id = g.name
        for ev in g.iterdir():
            if ev.is_dir():
                event_name = ev.name
                # Check pixel csv exists
                if (ev / "diff_pixel_values.csv").exists():
                    # Parse Delay from name: delay_X.Xh_YYYYMMDD
                    try:
                        parts = event_name.split('_')
                        delay_str = parts[1].replace('h', '')
                        delay_val = float(delay_str)
                        date_str = parts[2]
                        # Format YYYY-MM-DD
                        date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                        
                        events_data.append({
                            'grid_id': grid_id,
                            'event_name': event_name,
                            'delay_h': delay_val,
                            'date_str': date_fmt,
                            'pixel_csv': ev / "diff_pixel_values.csv"
                        })
                    except:
                        pass
                        
    if not events_data:
        return pd.DataFrame()
        
    df = pd.DataFrame(events_data)
    print(f"Found {len(df)} extracted events.")
    
    # Enrich with Rain Data
    print("Linking rain data...")
    unique_grids = df['grid_id'].unique().tolist()
    
    # Fetch all August events for these grids
    engine = create_engine(DATABASE_URL)
    # Note: Expansion logic assumed August.
    query = text("""
        SELECT grid_id, DATE_FORMAT(end_ts_utc, '%Y-%m-%d') as date_str, 
               sum_gauge_mm_h as total_precip,
               TIMESTAMPDIFF(HOUR, start_ts_utc, end_ts_utc) as duration_h
        FROM gsmap_events
        WHERE grid_id IN :grids
        AND MONTH(end_ts_utc) = 8
    """)
    
    # Fetch in chunks if too many grids
    chunk_size = 500
    all_rain = []
    
    for i in range(0, len(unique_grids), chunk_size):
        chunk = unique_grids[i:i+chunk_size]
        with engine.connect() as conn:
            r = pd.read_sql(query, conn, params={"grids": tuple(chunk)})
            all_rain.append(r)
            
    df_rain = pd.concat(all_rain, ignore_index=True)
    df_rain = df_rain.sort_values('total_precip', ascending=False).groupby(['grid_id', 'date_str']).first().reset_index()
    
    # Merge
    df_merged = pd.merge(df, df_rain, on=['grid_id', 'date_str'], how='inner')
    
    # Filters? User said "additional processed data". 
    # The preprocessing script already filtered by Intensity >= 10? No, checking logic.
    # execute_expansion_preprocessing.py just filtered August/Delay 0-12. Did not check intensity explicitly?
    # Ah, verify_unused_grids_quality filtered candidates. 
    # But let's apply filter just in case to be safe, or stick to what is there.
    # Let's trust the data is relevant, but valid duration is important.
    
    df_merged['duration_h'] = df_merged['duration_h'].replace(0, 1)
    
    # Bin Delay
    df_merged['delay_int'] = df_merged['delay_h'].apply(lambda x: int(x) if x < 12 else 11)
    
    print(f"Target Events (Linked): {len(df_merged)}")
    return df_merged

def run_balanced_analysis(events_df):
    results = []
    cm_data = [] 
    
    unique_delays = sorted(events_df['delay_int'].unique())
    print(f"Processing Delays: {unique_delays}")
    
    for d in unique_delays:
        print(f"\nProcessing Delay {d}h...")
        
        target_events = events_df[events_df['delay_int'] == d]
        if target_events.empty: continue
        
        pixels_list = []
        
        # Load pixels
        for _, row in target_events.iterrows():
            csv_path = row['pixel_csv']
            try:
                df_p = pd.read_csv(csv_path)
                if df_p.empty: continue
                
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
        
        # Split 80/20
        X = df_bal[['diff_db', 'total_precip', 'duration']]
        y = df_bal['label']
        
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
            'train_samples': len(X_train),
            'test_samples': len(X_test),
            'tn': tn, 'fp': fp, 'fn': fn, 'tp': tp
        })
        cm_data.append({'delay': d, 'cm': cm, 'acc': acc, 'n': len(X_test)})

    # Save outputs
    pd.DataFrame(results).to_csv(OUTPUT_DIR / "Expansion_分類結果サマリー.csv", index=False)
    
    # Plots
    if results:
        res_df = pd.DataFrame(results)
        plt.figure(figsize=(10, 6))
        plt.plot(res_df['delay'], res_df['test_accuracy'], 'o-', label='全体精度', linewidth=2)
        plt.plot(res_df['delay'], res_df['test_acc_road'], 's--', label='道路精度', alpha=0.7)
        plt.plot(res_df['delay'], res_df['test_acc_paddy'], '^--', label='田んぼ精度', alpha=0.7)
        plt.xlabel('経過時間 (h)')
        plt.ylabel('精度')
        plt.title('Expansionデータ_分類精度 (Balanced)')
        plt.ylim(0, 1.05)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.savefig(OUTPUT_DIR / "Expansion_分類精度推移.png")
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
        plt.savefig(OUTPUT_DIR / "Expansion_混同行列.png")
        plt.close()

if __name__ == "__main__":
    df = scan_expansion_events()
    if not df.empty:
        run_balanced_analysis(df)
