
import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
sys.path.append(str(BASE_DIR))

# Load Env
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

EXISTING_CSV = BASE_DIR / "data/result/vv/diff/all_events_diff_vv.csv"
OUTPUT_DIR = BASE_DIR / "data/result/Aug2"

def main():
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not EXISTING_CSV.exists():
        print("Error: Existing CSV not found.")
        return

    engine = create_engine(DATABASE_URL)
    
    # 1. Load Data
    print("Loading existing diff data...")
    df = pd.read_csv(EXISTING_CSV)
    
    # Filter August
    df['delay_h'] = df['event_name'].apply(lambda x: float(x.split('_')[1].replace('h', '')) if len(x.split('_')) > 1 else -1)
    df['month'] = df['event_name'].apply(lambda x: int(x.split('_')[2][4:6]) if len(x.split('_')) > 2 else 0)
    df_aug = df[df['month'] == 8].copy()
    
    print(f"Original August Events: {len(df_aug)}")
    
    # Link with DB Rain
    df_aug['date_str'] = df_aug['event_name'].apply(lambda x: pd.to_datetime(x.split('_')[2]).strftime('%Y-%m-%d'))
    grids = df_aug['grid_id'].unique().tolist()
    
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
    
    # Merge (take max precip per day if dupes)
    df_rain = df_rain.sort_values('total_precip', ascending=False).groupby(['grid_id', 'date_str']).first().reset_index()
    df_merged = pd.merge(df_aug, df_rain, on=['grid_id', 'date_str'], how='inner')
    
    # 2. Filter: Avg Intensity >= 10.0
    df_merged['duration_h'] = df_merged['duration_h'].replace(0, 1)
    df_merged['avg_intensity'] = df_merged['total_precip'] / df_merged['duration_h']
    
    df_filtered = df_merged[df_merged['avg_intensity'] >= 10.0].copy()
    
    # 3. Filter: Pixel Check (Either > 0)
    paddy_col = 'paddy_diff_count' if 'paddy_diff_count' in df_filtered.columns else ('paddy_count' if 'paddy_count' in df_filtered.columns else None)
    road_col = 'road_diff_count' if 'road_diff_count' in df_filtered.columns else ('road_count' if 'road_count' in df_filtered.columns else None)
    
    if paddy_col and road_col:
        df_final = df_filtered[(df_filtered[paddy_col] > 0) | (df_filtered[road_col] > 0)].copy()
    else:
        df_final = df_filtered.copy() # fallback
    
    print(f"Final Count: {len(df_final)}")
    
    # 4. Generate '8月_降雨イベント一覧.csv'
    # Format: イベントID, 総降水量(mm), 継続時間(h), 経過時間(h)
    # EventID = N03135E13085_2024-08-28 (from user example)
    # Our join_key or key
    def make_event_id(row):
        return f"{row['grid_id']}_{row['date_str']}"
    
    df_final['イベントID'] = df_final.apply(make_event_id, axis=1)
    
    list_df = df_final[['イベントID', 'total_precip', 'duration_h', 'delay_h']].copy()
    list_df.columns = ['イベントID', '総降水量(mm)', '継続時間(h)', '経過時間(h)']
    
    list_path = OUTPUT_DIR / "8月_降雨イベント一覧.csv"
    list_df.to_csv(list_path, index=False, encoding='utf-8-sig') # Excel safe
    print(f"Saved: {list_path}")
    
    # 5. Generate '8月_全Delay構成バイアス.csv'
    # Format: delay, n_samples, n_events, top_grid_share, top_5_share (from user check)
    # We only have n_events for now. n_samples = total pixels?
    # n_samples = paddy + road pixels for that delay bin
    # We calculate simple stats.
    
    bins = list(range(0, 13))
    # labels without 'h', just int for grouping? User csv has 0, 1, 2...
    # Let's group by int(delay_h) or bin
    df_final['Delay_Int'] = df_final['delay_h'].apply(lambda x: int(x) if x < 12 else 11)
    
    bias_stats = []
    
    for d in range(12):
        group = df_final[df_final['Delay_Int'] == d]
        n_events = len(group)
        
        # Calculate samples (pixels)
        if paddy_col and road_col:
            n_samples = group[paddy_col].sum() + group[road_col].sum()
        else:
            n_samples = 0
            
        stats = {
            'delay': d,
            'n_samples': int(n_samples),
            'n_events': n_events,
            # Placeholder shares
            'top_grid_share': 0,
            'top_5_share': 0
        }
        bias_stats.append(stats)
        
    bias_df = pd.DataFrame(bias_stats)
    bias_path = OUTPUT_DIR / "8月_全Delay構成バイアス.csv"
    bias_df.to_csv(bias_path, index=False, encoding='utf-8-sig')
    print(f"Saved: {bias_path}")

    # Save detailed list for future steps (RF)
    # We need grid_id, event_name to reload
    df_final.to_csv(OUTPUT_DIR / "aug2_filtered_events_detail.csv", index=False)
    print("Saved detail list for RF step.")

if __name__ == "__main__":
    main()
