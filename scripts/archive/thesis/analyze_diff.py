import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import common

logger = common.setup_logger("analyze_diff")

def get_rainfall_data(grid_id, event_name):
    """
    Retrieves rainfall data for a specific event from DB.
    Ref: s1_pairs table usually links to rainfall events, but mapping logic needs to be robust.
    Usually we match by grid_id and Approximate datetime or use the event_name timestamp.
    EventName Format: delay_{delay_h}h_{YYYYMMDD}
    
    BUT, s1_pairs has exact link. Let's try to query s1_pairs first for timing, then gsmap_events.
    Or direct query if we can infer the start time.
    
    For now, let's grab the date from the folder name and query gsmap_events for that grid on that day.
    """
    try:
        parts = event_name.split('_')
        # parts: ['delay', '10h', '20180928'] or ['delay', '1.5h', '2018...']
        date_str = parts[-1] 
        # Convert YYYYMMDD to YYYY-MM-DD
        target_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        
        conn = common.get_db_connection()
        if not conn:
            return None
        
        cursor = conn.cursor(dictionary=True)
        
        # Query gsmap_events
        # We look for an event that starts or ends on this date, or covers this date.
        # Ideally, we find the EXACT event. The folder name date is usually the Rain End Date (or near it).
        # Let's find the largest event on that day for that grid?
        # Or look at s1_pairs to see which event is linked.
        
        # Strategy: Find event in `gsmap_events` where `grid_id` matches and date matches `end_ts_utc`.
        # Assuming folder date is close to end_ts.
        query = """
            SELECT max_gauge_mm_h, sum_gauge_mm_h, start_ts_utc, end_ts_utc
            FROM gsmap_events
            WHERE grid_id = %s
            AND DATE(end_ts_utc) = %s
            ORDER BY sum_gauge_mm_h DESC
            LIMIT 1
        """
        cursor.execute(query, (grid_id, target_date))
        row = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if row:
            # duration in hours
            start = row['start_ts_utc']
            end = row['end_ts_utc']
            duration = (end - start).total_seconds() / 3600.0
            
            return {
                "max_intensity": row['max_gauge_mm_h'],
                "total_precip": row['sum_gauge_mm_h'],
                "duration": duration,
                "start_utc": start,
                "end_utc": end
            }
        else:
            return None

    except Exception as e:
        logger.error(f"Error fetching rain data for {grid_id}/{event_name}: {e}")
        return None

def process_diff_event(grid_id, event_dir_name, polarization="vv"):
    """
    Calculates Diff stats for a single event.
    """
    event_dir = common.SAMPLES_DIR / grid_id / event_dir_name
    after_path = event_dir / f"after_{polarization.lower()}.tif"
    before_path = event_dir / f"before_{polarization.lower()}.tif"
    
    if not after_path.exists() or not before_path.exists():
        return None

    paddy_shapes = common.get_mask_shapes(grid_id, "paddy")
    road_shapes = common.get_mask_shapes(grid_id, "road")
    
    res = {
        "grid_id": grid_id,
        "event_name": event_dir_name,
        "polarization": polarization
    }
    
    # Rainfall Info
    rain_info = get_rainfall_data(grid_id, event_dir_name)
    if rain_info:
        res.update(rain_info)

    # Calculate Diff (After dB - Before dB)
    for land_type, shapes in [("paddy", paddy_shapes), ("road", road_shapes)]:
        if not shapes:
            continue
            
        after_vals = common.load_raster_masked(after_path, shapes)
        before_vals = common.load_raster_masked(before_path, shapes)
        
        if after_vals is not None and before_vals is not None and len(after_vals) == len(before_vals) and len(after_vals) > 0:
            # We must ensure creating diffs pixel-by-pixel implies aligned arrays.
            # load_raster_masked returns flat array of VALID pixels under mask.
            # If mask is same and image geometry same, valid pixels should align IF no nodata differences.
            # However, speckle noise or nodata holes might mismatch.
            # For strict pixel diff, we should load whole arrays, mask, then diff.
            
            # Re-approach: Load arrays, then mask.
            # But common.load_raster_masked does mask(crop=True).
            # If bounds differ (unlikely if same S1), crop should be same.
            
            # Data is already in dB, no conversion needed
            after_db = after_vals
            before_db = before_vals
            
            # Simple handling of length mismatch (rare if same geom)
            min_len = min(len(after_db), len(before_db))
            diff_db = after_db[:min_len] - before_db[:min_len]
            
            # Filter noise from Diff
            diff_clean = common.filter_outliers(diff_db, method="iqr")
            
            if len(diff_clean) > 0:
                res[f"{land_type}_diff_mean"] = np.mean(diff_clean)
                res[f"{land_type}_diff_median"] = np.median(diff_clean)
                res[f"{land_type}_diff_std"] = np.std(diff_clean)
            
    return res

def start_analysis(polarization="vv", grid=None, max_workers=4):
    target_grids = [grid] if grid else common.get_grid_ids()
    
    output_dir = common.RESULT_DIR / polarization.lower() / "diff"
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"diff_stats_{polarization}.csv"
    
    processed_keys = set()
    existing_df = pd.DataFrame()
    if csv_path.exists():
        try:
            existing_df = pd.read_csv(csv_path)
            if 'grid_id' in existing_df.columns and 'event_name' in existing_df.columns:
                for _, row in existing_df.iterrows():
                    processed_keys.add((str(row['grid_id']), str(row['event_name'])))
            logger.info(f"Loaded {len(existing_df)} existing records. Skipping {len(processed_keys)} events.")
        except Exception as e:
            logger.warning(f"Failed to load existing CSV: {e}")
    
    all_results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for g in target_grids:
            events = common.get_events(g)
            for e in events:
                if (str(g), str(e)) not in processed_keys:
                    futures.append(executor.submit(process_diff_event, g, e, polarization))
        
        logger.info(f"Submitting {len(futures)} tasks to pool...")
        
        for f in futures:
            try:
                r = f.result()
                if r:
                    all_results.append(r)
            except Exception as e:
                logger.error(f"Error in diff analysis: {e}")
                
    if all_results:
        new_df = pd.DataFrame(all_results)
        final_df = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df
    else:
        final_df = existing_df
        
    df = final_df
    if not df.empty:
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved diff stats to {csv_path} (New: {len(all_results)}, Total: {len(df)})")
        
        # Plot Correlation: Diff vs Rainfall Intensity (Total * Duration as proxy?)
        # Or just Total Precip
        if "paddy_diff_mean" in df.columns and "total_precip" in df.columns:
            plt.figure(figsize=(8,6))
            sns.scatterplot(data=df, x="total_precip", y="paddy_diff_mean", label="Paddy")
            if "road_diff_mean" in df.columns:
                sns.scatterplot(data=df, x="total_precip", y="road_diff_mean", label="Road", marker="x")
            plt.title(f"Diff Mean vs Total Precip ({polarization.upper()})")
            plt.xlabel("Total Precip (mm)")
            plt.ylabel("Diff Mean (dB)")
            plt.savefig(output_dir / f"diff_vs_rain_{polarization}.png")
            plt.close()
            
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--polarization", default="vv")
    parser.add_argument("--grid")
    args = parser.parse_args()
    
    start_analysis(args.polarization, args.grid)

if __name__ == "__main__":
    main()
