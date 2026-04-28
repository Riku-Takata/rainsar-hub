import os
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import common

logger = common.setup_logger("analyze_sigma")

def processing_event(grid_id, event_dir_name, polarization="vv"):
    """
    Process a single event for a single grid.
    Returns a dictionary of stats.
    """
    event_dir = common.SAMPLES_DIR / grid_id / event_dir_name
    
    # Define file paths
    # Note: Filenames are typically "after_vv.tif", "before_vv.tif"
    after_path = event_dir / f"after_{polarization.lower()}.tif"
    before_path = event_dir / f"before_{polarization.lower()}.tif"
    
    if not after_path.exists() or not before_path.exists():
        return None

    # Load Masks
    paddy_shapes = common.get_mask_shapes(grid_id, "paddy")
    road_shapes = common.get_mask_shapes(grid_id, "road")
    
    if not paddy_shapes and not road_shapes:
        return None

    stats = {
        "grid_id": grid_id,
        "event_name": event_dir_name,
        "polarization": polarization
    }
    
    # --- Process PADDY ---
    if paddy_shapes:
        # Load pixels
        after_paddy = common.load_raster_masked(after_path, paddy_shapes)
        before_paddy = common.load_raster_masked(before_path, paddy_shapes)
        
        # Convert to dB for stats - NOTE: Data is already in dB!
        if after_paddy is not None and len(after_paddy) > 0:
            # Data is already in dB, no need to convert
            after_paddy_db = after_paddy
            # Filter Noise
            after_paddy_clean = common.filter_outliers(after_paddy_db, method="iqr")
            
            if len(after_paddy_clean) > 0:
                stats["paddy_after_mean"] = np.mean(after_paddy_clean)
                stats["paddy_after_median"] = np.median(after_paddy_clean)
                stats["paddy_after_std"] = np.std(after_paddy_clean)
                stats["paddy_count"] = len(after_paddy_clean)
        
        if before_paddy is not None and len(before_paddy) > 0:
            before_paddy_db = before_paddy  # Already dB
            before_paddy_clean = common.filter_outliers(before_paddy_db, method="iqr")
            
            if len(before_paddy_clean) > 0:
                stats["paddy_before_mean"] = np.mean(before_paddy_clean)
                stats["paddy_before_median"] = np.median(before_paddy_clean)
                stats["paddy_before_std"] = np.std(before_paddy_clean)
            
    # --- Process ROAD ---
    if road_shapes:
        after_road = common.load_raster_masked(after_path, road_shapes)
        before_road = common.load_raster_masked(before_path, road_shapes)
        
        if after_road is not None and len(after_road) > 0:
            after_road_db = after_road  # Already dB
            after_road_clean = common.filter_outliers(after_road_db, method="iqr")
            
            if len(after_road_clean) > 0:
                stats["road_after_mean"] = np.mean(after_road_clean)
                stats["road_after_median"] = np.median(after_road_clean)
                stats["road_after_std"] = np.std(after_road_clean)
                stats["road_count"] = len(after_road_clean)

        if before_road is not None and len(before_road) > 0:
            before_road_db = before_road  # Already dB
            before_road_clean = common.filter_outliers(before_road_db, method="iqr")
            
            if len(before_road_clean) > 0:
                stats["road_before_mean"] = np.mean(before_road_clean)
                stats["road_before_median"] = np.median(before_road_clean)
                stats["road_before_std"] = np.std(before_road_clean)

    return stats

def process_grid(grid_id, polarization="vv"):
    """
    Process all events for a grid.
    """
    events = common.get_events(grid_id)
    results = []
    
    for event in events:
        try:
            res = processing_event(grid_id, event, polarization)
            if res:
                results.append(res)
        except Exception as e:
            logger.error(f"Error processing {grid_id} - {event}: {e}")
            
    return results

def main():
    parser = argparse.ArgumentParser(description="Analyze Sigma0 distribution.")
    parser.add_argument("--polarization", type=str, default="vv", choices=["vv", "vh", "all"], help="Polarization to analyze")
    parser.add_argument("--grid", type=str, help="Specific grid ID to process (optional)")
    parser.add_argument("--max_workers", type=int, default=4, help="Number of parallel workers")
    
    args = parser.parse_args()
    
    pols = ["vv", "vh"] if args.polarization == "all" else [args.polarization]
    
    target_grids = [args.grid] if args.grid else common.get_grid_ids()
    total_grids = len(target_grids)
    
    for pol in pols:
        logger.info(f"Starting analysis for polarization: {pol.upper()} ({total_grids} grids)")
        
        output_dir = common.RESULT_DIR / pol.lower() / "sigma"
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"all_grids_sigma_stats_{pol}.csv"
        
        # Load existing data to skip processed events
        processed_keys = set()
        existing_df = pd.DataFrame()
        if csv_path.exists():
            try:
                existing_df = pd.read_csv(csv_path)
                # Ensure compatibility if columns changed, but simple check for now
                if 'grid_id' in existing_df.columns and 'event_name' in existing_df.columns:
                    for _, row in existing_df.iterrows():
                        processed_keys.add((str(row['grid_id']), str(row['event_name'])))
                logger.info(f"Loaded {len(existing_df)} existing records. Skipping {len(processed_keys)} events.")
            except Exception as e:
                logger.warning(f"Failed to load existing CSV: {e}")

        all_stats = []
        processed = 0
        newly_processed = 0
        
        # Process grids sequentially with progress logging (more reliable than ThreadPool for large datasets)
        for i, g in enumerate(target_grids):
            try:
                # Pass processed_keys to filter inside process_grid or filter here ?
                # process_grid gets all events. Better filter inside process_grid if we change signature,
                # Or filter events here? common.get_events(g) returns list.
                
                events = common.get_events(g)
                # Filter events
                events_to_run = [e for e in events if (str(g), str(e)) not in processed_keys]
                
                if not events_to_run:
                    processed += 1
                    continue
                    
                # Modified process_grid logic inline or call modified function?
                # Let's call a modified version or just loop here to avoid changing signature extensively
                grid_results = []
                for event in events_to_run:
                    try:
                        res = processing_event(g, event, pol)
                        if res:
                            grid_results.append(res)
                    except Exception as e:
                        logger.error(f"Error processing {g} - {event}: {e}")
                
                if grid_results:
                    all_stats.extend(grid_results)
                    newly_processed += len(grid_results)
                
                processed += 1
                if (i + 1) % 10 == 0 or (i + 1) == total_grids:
                    logger.info(f"Progress: {i+1}/{total_grids} grids checked/processed")
            except Exception as e:
                logger.error(f"Error processing grid {g}: {e}")
        
        # Convert to DataFrame
        if all_stats:
            new_df = pd.DataFrame(all_stats)
            # Combine
            final_df = pd.concat([existing_df, new_df], ignore_index=True) if not existing_df.empty else new_df
        else:
            final_df = existing_df
        
        df = final_df # for plotting
        
        if not df.empty:
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved aggregated stats to {csv_path} (New: {len(all_stats)}, Total: {len(df)})")
            
            # Simple Global Plot
            plt.figure(figsize=(10, 6))
            if "paddy_before_mean" in df.columns:
                sns.histplot(df["paddy_before_mean"], color="green", label="Paddy Before Mean", kde=True)
            if "road_before_mean" in df.columns:
                sns.histplot(df["road_before_mean"], color="gray", label="Road Before Mean", kde=True)
            plt.title(f"Global Distribution of Mean Sigma0 ({pol.upper()})")
            plt.xlabel("Sigma0 (dB)")
            plt.legend()
            plt.savefig(output_dir / f"global_sigma_dist_{pol}.png")
            plt.close()
            
        else:
            logger.warning("No data found to process.")

if __name__ == "__main__":
    main()
