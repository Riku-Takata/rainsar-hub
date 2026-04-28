
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

def main():
    if not EXISTING_CSV.exists():
        print("Error: Existing CSV not found.")
        return

    engine = create_engine(DATABASE_URL)
    
    print("Loading existing data...")
    df = pd.read_csv(EXISTING_CSV)
    
    # Filter August
    df['delay_h'] = df['event_name'].apply(lambda x: float(x.split('_')[1].replace('h', '')) if len(x.split('_')) > 1 else -1)
    df['month'] = df['event_name'].apply(lambda x: int(x.split('_')[2][4:6]) if len(x.split('_')) > 2 else 0)
    df_aug = df[df['month'] == 8].copy()
    
    print(f"Original August Events: {len(df_aug)}")
    
    # Link with rainfall data
    # Create key: grid_id, date
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
    
    # Merge
    # Note: DB might have multiple events per day, take max precip one?
    # Simple merge first
    df_rain = df_rain.sort_values('total_precip', ascending=False).groupby(['grid_id', 'date_str']).first().reset_index()
    
    df_merged = pd.merge(df_aug, df_rain, on=['grid_id', 'date_str'], how='inner') # Inner join = must have rain data
    
    print(f"Events with Linked Rain Data: {len(df_merged)}")
    
    # 1. Filter: Avg Intensity >= 10mm/h
    # Intensity = Total / Duration (replace 0 with 1)
    df_merged['duration_h'] = df_merged['duration_h'].replace(0, 1)
    df_merged['avg_intensity'] = df_merged['total_precip'] / df_merged['duration_h']
    
    df_filtered = df_merged[df_merged['avg_intensity'] >= 10.0].copy()
    print(f"Filter (Avg Intensity >= 10mm/h): {len(df_merged)} -> {len(df_filtered)}")
    
    # 2. Filter: Pixel Count (OR condition)
    # Check if we have columns
    paddy_col = 'paddy_diff_count' if 'paddy_diff_count' in df_filtered.columns else ('paddy_count' if 'paddy_count' in df_filtered.columns else None)
    road_col = 'road_diff_count' if 'road_diff_count' in df_filtered.columns else ('road_count' if 'road_count' in df_filtered.columns else None)
    
    if paddy_col and road_col:
        # Relaxed: Either Paddy > 0 OR Road > 0
        df_final = df_filtered[(df_filtered[paddy_col] > 0) | (df_filtered[road_col] > 0)].copy()
        
        # How many have BOTH? (Strict) vs EITHER (Relaxed)
        n_strict = len(df_filtered[(df_filtered[paddy_col] > 0) & (df_filtered[road_col] > 0)])
        n_either = len(df_final)
        
        print(f"Filter (Pixel > 0, Either): {len(df_filtered)} -> {len(df_final)}")
        print(f"  (Note: {n_strict} events have BOTH Paddy and Road data)")
    else:
        print("Warning: Pixel count columns missing, skipping pixel filter.")
        df_final = df_filtered.copy()
        
    # 3. Matrix Analysis (Delay Distribution)
    bins = list(range(0, 13))
    labels = [f"{i}-{i+1}h" for i in range(0, 12)]
    df_final['Delay_Bin'] = pd.cut(df_final['delay_h'], bins=bins, labels=labels, include_lowest=True)
    
    summary = df_final['Delay_Bin'].value_counts().sort_index()
    
    print("\n--- Final Filtered Distribution (Existing Data) ---")
    print(summary)
    
    # Check 7-8h
    print(f"\n7-8h Count: {summary.get('7-8h', 0)}")
    
    # Detailed Matrix (Grid x Delay coverage?)
    # User asked for "Matrix-like analysis". 
    # Maybe simply the distribution table is enough, as "Matrix" usually referred to Confusion Matrix in previous context.
    # But let's show stats per bin.
    
    print("\n--- Stats per Delay Bin ---")
    stats = df_final.groupby('Delay_Bin').agg({
        'total_precip': 'mean',
        'avg_intensity': 'mean',
        'duration_h': 'mean'
    })
    print(stats)

if __name__ == "__main__":
    main()
