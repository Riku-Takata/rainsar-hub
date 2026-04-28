"""
Extended Sigma Analysis Script
- Per-grid/per-event folder output with CSVs
- Individual event histograms
- Monthly/seasonal distribution analysis
"""
import os
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import common

logger = common.setup_logger("analyze_sigma_detailed")

def extract_month(event_name):
    """Extract month from event name (delay_Xh_YYYYMMDD)"""
    try:
        date_str = event_name.split('_')[-1]
        return int(date_str[4:6])
    except:
        return None

def process_event_detailed(grid_id, event_dir_name, polarization, output_base_dir):
    """
    Process a single event with detailed output.
    Saves individual CSV and histogram for this event.
    """
    event_dir = common.SAMPLES_DIR / grid_id / event_dir_name
    after_path = event_dir / f"after_{polarization.lower()}.tif"
    before_path = event_dir / f"before_{polarization.lower()}.tif"
    
    if not after_path.exists() or not before_path.exists():
        return None

    paddy_shapes = common.get_mask_shapes(grid_id, "paddy")
    road_shapes = common.get_mask_shapes(grid_id, "road")
    
    if not paddy_shapes and not road_shapes:
        return None

    # Create output folder for this event
    event_output_dir = output_base_dir / grid_id / event_dir_name
    event_output_dir.mkdir(parents=True, exist_ok=True)
    
    stats = {
        "grid_id": grid_id,
        "event_name": event_dir_name,
        "polarization": polarization,
        "month": extract_month(event_dir_name)
    }
    
    pixel_data = []  # For per-pixel CSV
    
    # --- Process PADDY ---
    paddy_after_clean = np.array([])
    paddy_before_clean = np.array([])
    
    if paddy_shapes:
        after_paddy = common.load_raster_masked(after_path, paddy_shapes)
        before_paddy = common.load_raster_masked(before_path, paddy_shapes)
        
        if after_paddy is not None and len(after_paddy) > 0:
            paddy_after_clean = common.filter_outliers(after_paddy, method="iqr")
            if len(paddy_after_clean) > 0:
                stats["paddy_after_mean"] = np.mean(paddy_after_clean)
                stats["paddy_after_median"] = np.median(paddy_after_clean)
                stats["paddy_after_std"] = np.std(paddy_after_clean)
                stats["paddy_after_count"] = len(paddy_after_clean)
                
        if before_paddy is not None and len(before_paddy) > 0:
            paddy_before_clean = common.filter_outliers(before_paddy, method="iqr")
            if len(paddy_before_clean) > 0:
                stats["paddy_before_mean"] = np.mean(paddy_before_clean)
                stats["paddy_before_median"] = np.median(paddy_before_clean)
                stats["paddy_before_std"] = np.std(paddy_before_clean)
                stats["paddy_before_count"] = len(paddy_before_clean)
                
        # Add to pixel data
        for val in paddy_after_clean:
            pixel_data.append({"land_type": "paddy", "scene": "after", "sigma0_db": val})
        for val in paddy_before_clean:
            pixel_data.append({"land_type": "paddy", "scene": "before", "sigma0_db": val})
    
    # --- Process ROAD ---
    road_after_clean = np.array([])
    road_before_clean = np.array([])
    
    if road_shapes:
        after_road = common.load_raster_masked(after_path, road_shapes)
        before_road = common.load_raster_masked(before_path, road_shapes)
        
        if after_road is not None and len(after_road) > 0:
            road_after_clean = common.filter_outliers(after_road, method="iqr")
            if len(road_after_clean) > 0:
                stats["road_after_mean"] = np.mean(road_after_clean)
                stats["road_after_median"] = np.median(road_after_clean)
                stats["road_after_std"] = np.std(road_after_clean)
                stats["road_after_count"] = len(road_after_clean)
                
        if before_road is not None and len(before_road) > 0:
            road_before_clean = common.filter_outliers(before_road, method="iqr")
            if len(road_before_clean) > 0:
                stats["road_before_mean"] = np.mean(road_before_clean)
                stats["road_before_median"] = np.median(road_before_clean)
                stats["road_before_std"] = np.std(road_before_clean)
                stats["road_before_count"] = len(road_before_clean)
                
        for val in road_after_clean:
            pixel_data.append({"land_type": "road", "scene": "after", "sigma0_db": val})
        for val in road_before_clean:
            pixel_data.append({"land_type": "road", "scene": "before", "sigma0_db": val})
    
    # Save per-pixel CSV
    if pixel_data:
        pixel_df = pd.DataFrame(pixel_data)
        pixel_df.to_csv(event_output_dir / "pixel_values.csv", index=False)
    
    # Save event stats CSV
    pd.DataFrame([stats]).to_csv(event_output_dir / "stats.csv", index=False)
    
    # Generate histogram for this event
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    # Before histogram
    ax1 = axes[0]
    if len(paddy_before_clean) > 0:
        ax1.hist(paddy_before_clean, bins=30, alpha=0.6, color='green', label='Paddy')
    if len(road_before_clean) > 0:
        ax1.hist(road_before_clean, bins=30, alpha=0.6, color='gray', label='Road')
    ax1.set_title(f"Before Scene - {event_dir_name}")
    ax1.set_xlabel("Sigma0 (dB)")
    ax1.set_ylabel("Frequency")
    ax1.legend()
    
    # After histogram
    ax2 = axes[1]
    if len(paddy_after_clean) > 0:
        ax2.hist(paddy_after_clean, bins=30, alpha=0.6, color='green', label='Paddy')
    if len(road_after_clean) > 0:
        ax2.hist(road_after_clean, bins=30, alpha=0.6, color='gray', label='Road')
    ax2.set_title(f"After Scene - {event_dir_name}")
    ax2.set_xlabel("Sigma0 (dB)")
    ax2.set_ylabel("Frequency")
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig(event_output_dir / "histogram.png", dpi=100)
    plt.close()
    
    return stats

def generate_monthly_analysis(all_stats_df, output_dir, polarization):
    """Generate monthly distribution analysis plots."""
    if "month" not in all_stats_df.columns:
        return
        
    monthly_dir = output_dir / "monthly_analysis"
    monthly_dir.mkdir(exist_ok=True)
    
    # Filter valid months
    df = all_stats_df[all_stats_df["month"].notna()].copy()
    df["month"] = df["month"].astype(int)
    
    # Monthly boxplot for Paddy Before
    if "paddy_before_mean" in df.columns:
        plt.figure(figsize=(12, 6))
        df.boxplot(column="paddy_before_mean", by="month", grid=False)
        plt.title(f"Paddy Before Mean by Month ({polarization.upper()})")
        plt.suptitle("")
        plt.xlabel("Month")
        plt.ylabel("Sigma0 (dB)")
        plt.savefig(monthly_dir / f"paddy_before_monthly_{polarization}.png", dpi=100)
        plt.close()
        
    # Monthly boxplot for Road Before
    if "road_before_mean" in df.columns:
        plt.figure(figsize=(12, 6))
        df.boxplot(column="road_before_mean", by="month", grid=False)
        plt.title(f"Road Before Mean by Month ({polarization.upper()})")
        plt.suptitle("")
        plt.xlabel("Month")
        plt.ylabel("Sigma0 (dB)")
        plt.savefig(monthly_dir / f"road_before_monthly_{polarization}.png", dpi=100)
        plt.close()
        
    # Monthly boxplot for Paddy After
    if "paddy_after_mean" in df.columns:
        plt.figure(figsize=(12, 6))
        df.boxplot(column="paddy_after_mean", by="month", grid=False)
        plt.title(f"Paddy After Mean by Month ({polarization.upper()})")
        plt.suptitle("")
        plt.xlabel("Month")
        plt.ylabel("Sigma0 (dB)")
        plt.savefig(monthly_dir / f"paddy_after_monthly_{polarization}.png", dpi=100)
        plt.close()
        
    # Monthly boxplot for Road After
    if "road_after_mean" in df.columns:
        plt.figure(figsize=(12, 6))
        df.boxplot(column="road_after_mean", by="month", grid=False)
        plt.title(f"Road After Mean by Month ({polarization.upper()})")
        plt.suptitle("")
        plt.xlabel("Month")
        plt.ylabel("Sigma0 (dB)")
        plt.savefig(monthly_dir / f"road_after_monthly_{polarization}.png", dpi=100)
        plt.close()
        
    # Monthly summary CSV
    monthly_summary = df.groupby("month").agg({
        "paddy_before_mean": ["mean", "median", "std", "count"],
        "road_before_mean": ["mean", "median", "std", "count"],
        "paddy_after_mean": ["mean", "median", "std", "count"],
        "road_after_mean": ["mean", "median", "std", "count"]
    }).round(4)
    monthly_summary.to_csv(monthly_dir / f"monthly_summary_{polarization}.csv")
    
    logger.info(f"Monthly analysis saved to {monthly_dir}")

def main():
    parser = argparse.ArgumentParser(description="Detailed Sigma0 analysis with per-event output.")
    parser.add_argument("--polarization", type=str, default="vv", choices=["vv", "vh", "all"])
    parser.add_argument("--grid", type=str, help="Specific grid ID (optional)")
    
    args = parser.parse_args()
    
    pols = ["vv", "vh"] if args.polarization == "all" else [args.polarization]
    target_grids = [args.grid] if args.grid else common.get_grid_ids()
    total_grids = len(target_grids)
    
    for pol in pols:
        logger.info(f"Starting detailed analysis for {pol.upper()} ({total_grids} grids)")
        
        output_base_dir = common.RESULT_DIR / pol.lower() / "sigma"
        output_base_dir.mkdir(parents=True, exist_ok=True)
        
        all_stats = []
        
        for i, grid_id in enumerate(target_grids):
            events = common.get_events(grid_id)
            
            for event in events:
                try:
                    stats = process_event_detailed(grid_id, event, pol, output_base_dir)
                    if stats:
                        all_stats.append(stats)
                except Exception as e:
                    logger.error(f"Error processing {grid_id}/{event}: {e}")
            
            if (i + 1) % 10 == 0 or (i + 1) == total_grids:
                logger.info(f"Progress: {i+1}/{total_grids} grids processed")
        
        # Save aggregated stats
        df = pd.DataFrame(all_stats)
        if not df.empty:
            df.to_csv(output_base_dir / f"all_events_detailed_{pol}.csv", index=False)
            logger.info(f"Saved aggregated stats ({len(df)} events)")
            
            # Generate monthly analysis
            generate_monthly_analysis(df, output_base_dir, pol)
        else:
            logger.warning("No data found.")

if __name__ == "__main__":
    main()
