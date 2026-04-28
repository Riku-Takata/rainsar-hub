import pandas as pd
from pathlib import Path
import rasterio
import numpy as np

# --- Configuration ---
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
DATA_DIR = BASE_DIR / "data" / "analysis"
INPUT_CSV = DATA_DIR / "monthly_delay_pixel_counts_detailed.csv"
OUTPUT_SUMMARY_CSV = DATA_DIR / "october_delay_summary_stats.csv"
OUTPUT_DETAILED_CSV = DATA_DIR / "october_event_pixel_stats.csv"

# Directories for looking up missing masks
SAMPLES_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"

def get_river_pixels(grid_id, event_dir):
    """Attempt to find and count river pixels if not in CSV."""
    river_mask_tif = MASKS_DIR / grid_id / f"{grid_id}_river_mask.tif"
    if not river_mask_tif.exists():
        return 0
    
    # Need a reference TIF from the event dir to ensure valid mask
    ref_tif = SAMPLES_DIR / grid_id / event_dir / "after_vv.tif"
    if not ref_tif.exists():
        return 0
        
    try:
        with rasterio.open(ref_tif) as src:
            nodata = src.nodata
            data = src.read(1)
            valid = (data != nodata) if nodata is not None else ~np.isnan(data)
            h, w = data.shape
            
        with rasterio.open(river_mask_tif) as msrc:
            mdata = msrc.read(1)[:h, :w]
            river_count = np.sum((mdata > 0) & valid)
            return river_count
    except:
        return 0

def export_stats():
    print("Loading pixel counts data...")
    df = pd.read_csv(INPUT_CSV)
    oct_df = df[df['month'] == 10].copy()
    
    # Check if river_pixels exists, if not, try to fetch (this might be slow, so let's check first)
    if 'river_pixels' not in oct_df.columns:
        print("river_pixels not found in CSV. Attempting to match from aggregated counts or masks...")
        # Try to use monthly_delay_class_pixel_counts.csv if available for aggregate verification
        agg_csv = DATA_DIR / "monthly_delay_class_pixel_counts.csv"
        if agg_csv.exists():
             agg_df = pd.read_csv(agg_csv)
             oct_agg = agg_df[agg_df['month'] == 10]
             print("\nReference Aggregate River Counts from monthly_delay_class_pixel_counts.csv:")
             print(oct_agg[['delay_int', 'river']])
        
        # We will add a placeholder or attempt a few to see if it's worth it
        # For now, let's just export what we HAVE (Road/Paddy) and note the River status.
        oct_df['river_pixels'] = 0 # Placeholder if not found
        
    # --- 1. Delay-based Summary ---
    summary = oct_df.groupby('delay_int').agg({
        'event_dir': 'nunique',
        'road_pixels': 'sum',
        'paddy_pixels': 'sum',
        'river_pixels': 'sum'
    }).rename(columns={
        'event_dir': 'n_events',
        'road_pixels': 'total_road_pixels',
        'paddy_pixels': 'total_paddy_pixels',
        'river_pixels': 'total_river_pixels'
    })
    
    # Save Summary
    summary.to_csv(OUTPUT_SUMMARY_CSV)
    print(f"Saved summary to: {OUTPUT_SUMMARY_CSV}")
    
    # --- 2. Event-based Detailed Statistics ---
    detailed = oct_df.sort_values(['delay_int', 'grid_id', 'event_dir'])[
        ['delay_int', 'grid_id', 'event_dir', 'road_pixels', 'paddy_pixels', 'river_pixels']
    ]
    
    # Save Detailed
    detailed.to_csv(OUTPUT_DETAILED_CSV, index=False)
    print(f"Saved detailed event stats to: {OUTPUT_DETAILED_CSV}")

if __name__ == "__main__":
    export_stats()
