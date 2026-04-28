
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
FULL_CSV = BASE_DIR / "data/result/vv/diff/all_events_diff_vv.csv"
FILTERED_CSV = BASE_DIR / "data/result/Aug/8月_降雨イベント一覧.csv"

def parse_event_name(row):
    # event_name: delay_10.0h_20240801
    try:
        parts = row['event_name'].split('_')
        date_str = parts[2] # 20240801
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        return f"{row['grid_id']}_{formatted_date}"
    except:
        return None

def main():
    if not FULL_CSV.exists() or not FILTERED_CSV.exists():
        print("Error: One of the CSV files not found.")
        return

    # 1. Load Data
    df_full = pd.read_csv(FULL_CSV)
    df_filtered = pd.read_csv(FILTERED_CSV, encoding='utf-8') # User file likely contains Japanese header

    # 2. Preprocess Full Data (source of 300 events)
    # Extract August events
    df_full['month'] = df_full['event_name'].apply(lambda x: int(x.split('_')[2][4:6]) if len(x.split('_')) > 2 else 0)
    df_aug = df_full[df_full['month'] == 8].copy()
    
    # Create join key: GridID_YYYY-MM-DD
    df_aug['join_key'] = df_aug.apply(parse_event_name, axis=1)

    # 3. Preprocess Filtered Data (source of 126 events)
    # ID format: N03135E13085_2024-08-28. This is already the key.
    df_filtered['join_key'] = df_filtered['イベントID']
    # If the filtered one has total precip, keep it for comparison
    # Col names: イベントID, 総降水量(mm), 継続時間(h), 経過時間(h)
    # Rename for consistency if needed, but we can just use them.

    # 4. Compare
    filtered_keys = set(df_filtered['join_key'])
    
    df_aug['is_included'] = df_aug['join_key'].isin(filtered_keys)
    
    included = df_aug[df_aug['is_included']]
    excluded = df_aug[~df_aug['is_included']]
    
    print(f"Full August Events: {len(df_aug)}")
    print(f"Included in Filtered List: {len(included)}")
    print(f"Excluded: {len(excluded)}")
    print("-" * 30)

    # 5. Analyze Differences
    # Compare Metrics: total_precip_mm, max_intensity (if available in full csv)
    # df_full likely has 'total_precip_mm'
    
    metric = 'total_precip_mm'
    if metric in df_aug.columns:
        print(f"\nComparing {metric}:")
        print("Included Stats:")
        print(included[metric].describe()[['count', 'mean', 'min', '50%', 'max']])
        print("\nExcluded Stats:")
        print(excluded[metric].describe()[['count', 'mean', 'min', '50%', 'max']])
    else:
        print(f"Warning: {metric} not found in full CSV.")

    # Check date range
    print("\nDate Range:")
    print(f"Included: {included['event_name'].apply(lambda x: x.split('_')[2]).min()} - {included['event_name'].apply(lambda x: x.split('_')[2]).max()}")
    if not excluded.empty:
        print(f"Excluded: {excluded['event_name'].apply(lambda x: x.split('_')[2]).min()} - {excluded['event_name'].apply(lambda x: x.split('_')[2]).max()}")

    # Check Grid coverage
    inc_grids = set(included['grid_id'])
    exc_grids = set(excluded['grid_id'])
    print(f"\nGrid Coverage:")
    print(f"Included Grids: {len(inc_grids)}")
    print(f"Excluded Only Grids: {len(exc_grids - inc_grids)}")

    # Check if there is a threshold
    if metric in df_aug.columns:
        min_inc = included[metric].min()
        max_exc = excluded[metric].max() if not excluded.empty else 0
        print(f"\nThreshold Analysis:")
        print(f"Min Precip in Included: {min_inc}")
        print(f"Max Precip in Excluded: {max_exc}")
        
        # Are there excluded events with High precip?
        high_precip_excluded = excluded[excluded[metric] > min_inc]
        if not high_precip_excluded.empty:
            print(f"\nWarning: {len(high_precip_excluded)} events excluded despite having precip > min_included ({min_inc}).")
            print(high_precip_excluded[['grid_id', 'event_name', 'total_precip_mm']].head())
    
    # Check Delay
    print("\nDelay Analysis (Excluded):")
    # Need to parse delay again or use what we filtered
    excluded = excluded.copy() # Avoid SettingWithCopyWarning
    excluded['delay_h'] = excluded['event_name'].apply(lambda x: float(x.split('_')[1].replace('h', '')))
    print(excluded['delay_h'].value_counts().sort_index())

if __name__ == "__main__":
    main()
