"""
Extended Difference Analysis Script
- Per-grid/per-event folder output with CSVs
- Individual event histograms for Diff values
- Monthly/seasonal distribution analysis
- Rainfall intensity correlation
"""
import os
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import common

logger = common.setup_logger("analyze_diff_detailed")

def extract_month(event_name):
    """Extract month from event name (delay_Xh_YYYYMMDD)"""
    try:
        date_str = event_name.split('_')[-1]
        return int(date_str[4:6])
    except:
        return None

def extract_delay_hours(event_name):
    """Extract delay hours from event name"""
    try:
        delay_str = event_name.split('_')[1]  # '10h' or '1.5h'
        return float(delay_str.replace('h', ''))
    except:
        return None

def get_rainfall_data(grid_id, event_name):
    """Retrieve rainfall data from DB"""
    try:
        parts = event_name.split('_')
        date_str = parts[-1]
        target_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        
        conn = common.get_db_connection()
        if not conn:
            return None
        
        cursor = conn.cursor(dictionary=True)
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
            start = row['start_ts_utc']
            end = row['end_ts_utc']
            duration = (end - start).total_seconds() / 3600.0
            
            return {
                "max_intensity_mm_h": row['max_gauge_mm_h'],
                "total_precip_mm": row['sum_gauge_mm_h'],
                "duration_hours": duration,
                "rain_start_utc": str(start),
                "rain_end_utc": str(end)
            }
        return None
    except Exception as e:
        logger.error(f"DB error for {grid_id}/{event_name}: {e}")
        return None

def process_diff_event_detailed(grid_id, event_dir_name, polarization, output_base_dir):
    """Process a single event with detailed diff output."""
    event_dir = common.SAMPLES_DIR / grid_id / event_dir_name
    after_path = event_dir / f"after_{polarization.lower()}.tif"
    before_path = event_dir / f"before_{polarization.lower()}.tif"
    
    if not after_path.exists() or not before_path.exists():
        return None

    paddy_shapes = common.get_mask_shapes(grid_id, "paddy")
    road_shapes = common.get_mask_shapes(grid_id, "road")
    
    if not paddy_shapes and not road_shapes:
        return None

    # Create output folder
    event_output_dir = output_base_dir / grid_id / event_dir_name
    event_output_dir.mkdir(parents=True, exist_ok=True)
    
    stats = {
        "grid_id": grid_id,
        "event_name": event_dir_name,
        "polarization": polarization,
        "month": extract_month(event_dir_name),
        "delay_hours": extract_delay_hours(event_dir_name)
    }
    
    # Get rainfall data
    rain_info = get_rainfall_data(grid_id, event_dir_name)
    if rain_info:
        stats.update(rain_info)
    
    pixel_data = []
    diff_arrays = {}
    
    # Process each land type
    for land_type, shapes in [("paddy", paddy_shapes), ("road", road_shapes)]:
        if not shapes:
            continue
            
        after_vals = common.load_raster_masked(after_path, shapes)
        before_vals = common.load_raster_masked(before_path, shapes)
        
        if after_vals is not None and before_vals is not None:
            min_len = min(len(after_vals), len(before_vals))
            if min_len > 0:
                diff_vals = after_vals[:min_len] - before_vals[:min_len]
                diff_clean = common.filter_outliers(diff_vals, method="iqr")
                
                if len(diff_clean) > 0:
                    stats[f"{land_type}_diff_mean"] = np.mean(diff_clean)
                    stats[f"{land_type}_diff_median"] = np.median(diff_clean)
                    stats[f"{land_type}_diff_std"] = np.std(diff_clean)
                    stats[f"{land_type}_diff_count"] = len(diff_clean)
                    
                    diff_arrays[land_type] = diff_clean
                    
                    for val in diff_clean:
                        pixel_data.append({"land_type": land_type, "diff_db": val})
    
    # Save per-pixel diff CSV
    if pixel_data:
        pd.DataFrame(pixel_data).to_csv(event_output_dir / "diff_pixel_values.csv", index=False)
    
    # Save event stats
    pd.DataFrame([stats]).to_csv(event_output_dir / "diff_stats.csv", index=False)
    
    # Generate diff histogram
    if diff_arrays:
        plt.figure(figsize=(10, 6))
        for land_type, arr in diff_arrays.items():
            color = 'green' if land_type == 'paddy' else 'gray'
            plt.hist(arr, bins=30, alpha=0.6, color=color, label=f'{land_type.capitalize()} (n={len(arr)})')
        plt.axvline(x=0, color='red', linestyle='--', alpha=0.5)
        plt.title(f"Diff Distribution (After-Before) - {event_dir_name}")
        plt.xlabel("Diff (dB)")
        plt.ylabel("Frequency")
        plt.legend()
        plt.tight_layout()
        plt.savefig(event_output_dir / "diff_histogram.png", dpi=100)
        plt.close()
    
    return stats

def generate_global_analysis(df, output_dir, polarization):
    """Generate global analysis plots and monthly analysis."""
    if df.empty:
        return
    
    # Overall diff distribution
    plt.figure(figsize=(10, 6))
    if "paddy_diff_mean" in df.columns:
        sns.histplot(df["paddy_diff_mean"].dropna(), color="green", label="Paddy", kde=True, alpha=0.6)
    if "road_diff_mean" in df.columns:
        sns.histplot(df["road_diff_mean"].dropna(), color="gray", label="Road", kde=True, alpha=0.6)
    plt.axvline(x=0, color='red', linestyle='--', alpha=0.5)
    plt.title(f"Global Diff Mean Distribution ({polarization.upper()})")
    plt.xlabel("Diff Mean (dB)")
    plt.legend()
    plt.savefig(output_dir / f"global_diff_dist_{polarization}.png", dpi=100)
    plt.close()
    
    # Rainfall Correlation
    if "total_precip_mm" in df.columns:
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        
        # Paddy vs Rainfall
        if "paddy_diff_mean" in df.columns:
            ax1 = axes[0]
            valid = df[["total_precip_mm", "paddy_diff_mean"]].dropna()
            ax1.scatter(valid["total_precip_mm"], valid["paddy_diff_mean"], alpha=0.5, color="green")
            ax1.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            ax1.set_xlabel("Total Precipitation (mm)")
            ax1.set_ylabel("Paddy Diff Mean (dB)")
            ax1.set_title("Paddy Diff vs Rainfall")
        
        # Road vs Rainfall
        if "road_diff_mean" in df.columns:
            ax2 = axes[1]
            valid = df[["total_precip_mm", "road_diff_mean"]].dropna()
            ax2.scatter(valid["total_precip_mm"], valid["road_diff_mean"], alpha=0.5, color="gray")
            ax2.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            ax2.set_xlabel("Total Precipitation (mm)")
            ax2.set_ylabel("Road Diff Mean (dB)")
            ax2.set_title("Road Diff vs Rainfall")
        
        plt.tight_layout()
        plt.savefig(output_dir / f"diff_vs_rainfall_{polarization}.png", dpi=100)
        plt.close()
    
    # Monthly analysis
    monthly_dir = output_dir / "monthly_analysis"
    monthly_dir.mkdir(exist_ok=True)
    
    if "month" in df.columns:
        df_valid = df[df["month"].notna()].copy()
        df_valid["month"] = df_valid["month"].astype(int)
        
        # Monthly boxplot for Paddy Diff
        if "paddy_diff_mean" in df_valid.columns:
            plt.figure(figsize=(12, 6))
            df_valid.boxplot(column="paddy_diff_mean", by="month", grid=False)
            plt.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            plt.title(f"Paddy Diff Mean by Month ({polarization.upper()})")
            plt.suptitle("")
            plt.xlabel("Month")
            plt.ylabel("Diff (dB)")
            plt.savefig(monthly_dir / f"paddy_diff_monthly_{polarization}.png", dpi=100)
            plt.close()
        
        # Monthly boxplot for Road Diff
        if "road_diff_mean" in df_valid.columns:
            plt.figure(figsize=(12, 6))
            df_valid.boxplot(column="road_diff_mean", by="month", grid=False)
            plt.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            plt.title(f"Road Diff Mean by Month ({polarization.upper()})")
            plt.suptitle("")
            plt.xlabel("Month")
            plt.ylabel("Diff (dB)")
            plt.savefig(monthly_dir / f"road_diff_monthly_{polarization}.png", dpi=100)
            plt.close()
        
        # Monthly summary
        agg_cols = {}
        for col in ["paddy_diff_mean", "road_diff_mean", "total_precip_mm"]:
            if col in df_valid.columns:
                agg_cols[col] = ["mean", "median", "std", "count"]
        
        if agg_cols:
            monthly_summary = df_valid.groupby("month").agg(agg_cols).round(4)
            monthly_summary.to_csv(monthly_dir / f"monthly_diff_summary_{polarization}.csv")
    
    logger.info(f"Global analysis saved to {output_dir}")

def main():
    parser = argparse.ArgumentParser(description="Detailed Diff analysis with per-event output.")
    parser.add_argument("--polarization", type=str, default="vv", choices=["vv", "vh", "all"])
    parser.add_argument("--grid", type=str, help="Specific grid ID (optional)")
    
    args = parser.parse_args()
    
    pols = ["vv", "vh"] if args.polarization == "all" else [args.polarization]
    target_grids = [args.grid] if args.grid else common.get_grid_ids()
    total_grids = len(target_grids)
    
    for pol in pols:
        logger.info(f"Starting detailed diff analysis for {pol.upper()} ({total_grids} grids)")
        
        output_base_dir = common.RESULT_DIR / pol.lower() / "diff"
        output_base_dir.mkdir(parents=True, exist_ok=True)
        
        all_stats = []
        
        for i, grid_id in enumerate(target_grids):
            events = common.get_events(grid_id)
            
            for event in events:
                try:
                    stats = process_diff_event_detailed(grid_id, event, pol, output_base_dir)
                    if stats:
                        all_stats.append(stats)
                except Exception as e:
                    logger.error(f"Error processing {grid_id}/{event}: {e}")
            
            if (i + 1) % 10 == 0 or (i + 1) == total_grids:
                logger.info(f"Progress: {i+1}/{total_grids} grids processed")
        
        # Save aggregated stats
        df = pd.DataFrame(all_stats)
        if not df.empty:
            df.to_csv(output_base_dir / f"all_events_diff_{pol}.csv", index=False)
            logger.info(f"Saved aggregated diff stats ({len(df)} events)")
            
            # Generate global analysis
            generate_global_analysis(df, output_base_dir, pol)
        else:
            logger.warning("No data found.")

if __name__ == "__main__":
    main()
