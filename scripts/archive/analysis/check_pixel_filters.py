
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
FULL_CSV = BASE_DIR / "data/result/vv/diff/all_events_diff_vv.csv"
FILTERED_CSV = BASE_DIR / "data/result/Aug/8月_降雨イベント一覧.csv"

# Legacy result files with pixel counts
# We need pixel counts for the "full" dataset.
# If full_csv comes from analyze_diff, it might NOT have pixel counts (depending on version).
# Let's check columns of full_csv first.

def parse_event_name(row):
    try:
        parts = row['event_name'].split('_')
        date_str = parts[2] 
        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        return f"{row['grid_id']}_{formatted_date}"
    except:
        return None

def main():
    if not FULL_CSV.exists() or not FILTERED_CSV.exists():
        print("Error: Files not found.")
        return

    df_full = pd.read_csv(FULL_CSV)
    df_filtered = pd.read_csv(FILTERED_CSV, encoding='utf-8')
    
    # 1. Identify Included/Excluded Grid-Events
    df_full['month'] = df_full['event_name'].apply(lambda x: int(x.split('_')[2][4:6]) if len(x.split('_')) > 2 else 0)
    df_aug = df_full[df_full['month'] == 8].copy()
    df_aug['join_key'] = df_aug.apply(parse_event_name, axis=1)
    
    df_filtered['join_key'] = df_filtered['イベントID']
    filtered_keys = set(df_filtered['join_key'])
    
    df_aug['is_included'] = df_aug['join_key'].isin(filtered_keys)
    
    # 2. Check Pixel Count Columns
    # Typically: 'paddy_pixel_count', 'road_pixel_count' or 'paddy_count', 'road_count'
    # Or in sigma/diff stats: 'paddy_count', 'road_count'
    
    paddy_col = 'paddy_diff_count' if 'paddy_diff_count' in df_aug.columns else ('paddy_count' if 'paddy_count' in df_aug.columns else 'paddy_after_count')
    road_col = 'road_diff_count' if 'road_diff_count' in df_aug.columns else ('road_count' if 'road_count' in df_aug.columns else 'road_after_count')
    
    # Check if we need to load sigma stats (only if count col missing)
    if paddy_col not in df_aug.columns:
        print(f"Counts missing in main CSV. Loading sigma stats...")
        # ... (Sigma loading logic if needed, but likely not needed for Diff csv)
        # Assuming Diff CSV has counts as seen in logs
    pass

    print(f"Using columns: {paddy_col}, {road_col}")
    
    included = df_aug[df_aug['is_included']]
    excluded = df_aug[~df_aug['is_included']]
    
    # 3. Analyze Distribution
    print(f"\nComparing Pixel Counts:")
    print("-" * 30)
    
    stats = pd.DataFrame({
        'Min Included': included[[paddy_col, road_col]].min(),
        'Max Excluded': excluded[[paddy_col, road_col]].max(), # This comparison is tricky. High px can be excluded by rain.
        'Mean Included': included[[paddy_col, road_col]].mean(),
        'Mean Excluded': excluded[[paddy_col, road_col]].mean(),
        'Min Excluded': excluded[[paddy_col, road_col]].min()
    })
    print(stats)
    
    # 4. Check for Minimum Threshold
    # Did we lose events because they had too few pixels?
    # Find max of excluded that were NOT excluded by rain (hard to separate variables here)
    # Just assume threshold is roughly min(Included)
    
    paddy_thresh = included[paddy_col].min()
    road_thresh = included[road_col].min()
    
    print(f"\nPotential Thresholds:")
    print(f"  Paddy > {paddy_thresh}")
    print(f"  Road  > {road_thresh}")
    
    # Check how many excluded events failed this threshold
    failed_paddy = excluded[excluded[paddy_col] < paddy_thresh]
    failed_road = excluded[excluded[road_col] < road_thresh]
    failed_both = excluded[(excluded[paddy_col] < paddy_thresh) | (excluded[road_col] < road_thresh)]
    
    print(f"\nExcluded Analysis (Total Excluded: {len(excluded)}):")
    print(f"  Failed Paddy Threshold (<{paddy_thresh}): {len(failed_paddy)}")
    print(f"  Failed Road Threshold  (<{road_thresh}): {len(failed_road)}")
    print(f"  Failed Either Threshold: {len(failed_both)}")
    
    # Are there excluded events that PASSED both pixel thresholds? (implies Rain was the cause)
    passed_pixels = excluded[~excluded.index.isin(failed_both.index)]
    print(f"  Passed Pixel Thresholds (likely Rain/Qual excluded): {len(passed_pixels)}")
    
    # Check 7-8h events specifically
    print("\n7-8h Events Drop Reason:")
    df_aug['delay_h'] = df_aug['event_name'].apply(lambda x: float(x.split('_')[1].replace('h', '')))
    target_evs = df_aug[~df_aug['is_included'] & (df_aug['delay_h'] >= 7) & (df_aug['delay_h'] <= 8)]
    
    if not target_evs.empty:
        print(target_evs[['grid_id', 'delay_h', paddy_col, road_col]].merge(
            # Add precip if in df_aug
            df_aug[['grid_id', 'total_precip_mm']] if 'total_precip_mm' in df_aug.columns else df_aug[['grid_id']], 
            left_index=True, right_index=True
        ))
    else:
        print("No 7-8h events found in Excluded (unexpected).")

if __name__ == "__main__":
    main()
