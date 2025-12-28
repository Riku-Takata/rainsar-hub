import argparse
import sys
from pathlib import Path

# Add current directory to sys.path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from common_utils import setup_logger, RESULT_DIR
import process_masks
import analyze_rasters
import analyze_stats
import manage_reports

logger = setup_logger("pipeline_runner")

def run_masks(grid_id=None):
    logger.info(">>> STEP 1: Mask Generation")
    target_grids = [grid_id] if grid_id else None
    process_masks.main(target_grids=target_grids)

def run_raster_stats(grid_id=None):
    logger.info(">>> STEP 2: Raster Analysis")
    target_grids = [grid_id] if grid_id else None
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_negative_pixels(target_grids=target_grids)
    analyzer.analyze_sigma_clipped(target_grids=target_grids)

def run_csv_stats(grid_id=None):
    logger.info(">>> STEP 3: CSV Statistics Analysis")
    csv_path = RESULT_DIR / "distributions" / "aggregated_stats_difference.csv"
    base_stats_dir = RESULT_DIR / "distributions"
    
    analyzer = analyze_stats.StatsAnalyzer(csv_path)
    
    if grid_id:
        # For single grid, we might need to aggregate just that grid if not present,
        # or load the full CSV and filter.
        # If full CSV exists, load it.
        if not csv_path.exists():
             logger.info("Aggregated CSV not found. Aggregating...")
             analyzer.aggregate_data(base_stats_dir) # Aggregate all to be safe? Or just one?
             # If we aggregate just one, we don't save to the main file (as per my change in analyze_stats).
             # So let's try to aggregate just one to memory if main doesn't exist.
             if analyzer.df is None:
                 analyzer.aggregate_data(base_stats_dir, target_grids=[grid_id])
        
        if analyzer.df is not None:
            analyzer.analyze_single_grid(grid_id)
        else:
            logger.warning("No data available for analysis.")
            
    else:
        if not csv_path.exists():
            logger.info("Aggregated CSV not found. Running aggregation...")
            analyzer.aggregate_data(base_stats_dir)
            
        if analyzer.df is not None:
            analyzer.analyze_noise()
            analyzer.plot_distributions()
        else:
            logger.warning("Failed to load or generate data.")

def run_report():
    logger.info(">>> STEP 4: Report Generation")
    manager = manage_reports.ReportManager()
    manager.write_initial_report()
    manager.append_negative_analysis_section()
    # manager.add_csv_table_to_summary(...)

def run_detailed_analysis(grid_id):
    if not grid_id:
        logger.error("Grid ID is required for detailed analysis.")
        return
    logger.info(f">>> STEP: Detailed Pixel Analysis for {grid_id}")
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_pixel_variations(grid_id)
    analyzer.analyze_time_evolution(grid_id)

def run_comprehensive_analysis(grid_id):
    if not grid_id:
        logger.error("Grid ID is required for comprehensive analysis.")
        return
    logger.info(f">>> STEP: Comprehensive Analysis for {grid_id}")
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_comprehensive_statistics(grid_id)

def run_global_trends():
    logger.info(">>> STEP: Global Trend Analysis")
    import analyze_global_trends
    # Assuming analysis_result.csv is in the root or we pass the path
    csv_path = Path("analysis_result.csv").resolve()
    analyzer = analyze_global_trends.GlobalTrendAnalyzer(csv_path)
    analyzer.plot_trends()

def run_outlier_evolution(grid_id):
    if not grid_id:
        logger.error("Grid ID is required for outlier evolution analysis.")
        return
    logger.info(f">>> STEP: Outlier Evolution Analysis for {grid_id}")
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_outlier_evolution(grid_id)
from pathlib import Path

# Add current directory to sys.path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir))

from common_utils import setup_logger, RESULT_DIR
import process_masks
import analyze_rasters
import analyze_stats
import manage_reports

logger = setup_logger("pipeline_runner")

def run_masks(grid_id=None):
    logger.info(">>> STEP 1: Mask Generation")
    target_grids = [grid_id] if grid_id else None
    process_masks.main(target_grids=target_grids)

def run_raster_stats(grid_id=None):
    logger.info(">>> STEP 2: Raster Analysis")
    target_grids = [grid_id] if grid_id else None
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_negative_pixels(target_grids=target_grids)
    analyzer.analyze_sigma_clipped(target_grids=target_grids)

def run_csv_stats(grid_id=None):
    logger.info(">>> STEP 3: CSV Statistics Analysis")
    csv_path = RESULT_DIR / "distributions" / "aggregated_stats_difference.csv"
    base_stats_dir = RESULT_DIR / "distributions"
    
    analyzer = analyze_stats.StatsAnalyzer(csv_path)
    
    if grid_id:
        # For single grid, we might need to aggregate just that grid if not present,
        # or load the full CSV and filter.
        # If full CSV exists, load it.
        if not csv_path.exists():
             logger.info("Aggregated CSV not found. Aggregating...")
             analyzer.aggregate_data(base_stats_dir) # Aggregate all to be safe? Or just one?
             # If we aggregate just one, we don't save to the main file (as per my change in analyze_stats).
             # So let's try to aggregate just one to memory if main doesn't exist.
             if analyzer.df is None:
                 analyzer.aggregate_data(base_stats_dir, target_grids=[grid_id])
        
        if analyzer.df is not None:
            analyzer.analyze_single_grid(grid_id)
        else:
            logger.warning("No data available for analysis.")
            
    else:
        if not csv_path.exists():
            logger.info("Aggregated CSV not found. Running aggregation...")
            analyzer.aggregate_data(base_stats_dir)
            
        if analyzer.df is not None:
            analyzer.analyze_noise()
            analyzer.plot_distributions()
        else:
            logger.warning("Failed to load or generate data.")

def run_report():
    logger.info(">>> STEP 4: Report Generation")
    manager = manage_reports.ReportManager()
    manager.write_initial_report()
    manager.append_negative_analysis_section()
    # manager.add_csv_table_to_summary(...)

def run_detailed_analysis(grid_id):
    if not grid_id:
        logger.error("Grid ID is required for detailed analysis.")
        return
    logger.info(f">>> STEP: Detailed Pixel Analysis for {grid_id}")
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_pixel_variations(grid_id)
    analyzer.analyze_time_evolution(grid_id)

def run_comprehensive_analysis(grid_id):
    if not grid_id:
        logger.error("Grid ID is required for comprehensive analysis.")
        return
    logger.info(f">>> STEP: Comprehensive Analysis for {grid_id}")
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_comprehensive_statistics(grid_id)

def run_global_trends():
    logger.info(">>> STEP: Global Trend Analysis")
    import analyze_global_trends
    # Assuming analysis_result.csv is in the root or we pass the path
    csv_path = Path("analysis_result.csv").resolve()
    analyzer = analyze_global_trends.GlobalTrendAnalyzer(csv_path)
    analyzer.plot_trends()

def run_outlier_evolution(grid_id):
    if not grid_id:
        logger.error("Grid ID is required for outlier evolution analysis.")
        return
    logger.info(f">>> STEP: Outlier Evolution Analysis for {grid_id}")
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_outlier_evolution(grid_id)

def run_intensity_dist(grid_id):
    """Run Intensity Distribution Analysis"""
    analyzer = analyze_rasters.RasterAnalyzer()
    analyzer.analyze_intensity_distribution(grid_id)

def main():
    parser = argparse.ArgumentParser(description="RainSAR Analysis Pipeline")
    parser.add_argument("step", choices=[
        "masks", "raster-stats", "csv-stats", "report", "all",
        "detailed", "comprehensive", "global-trends", "outlier-evolution",
        "intensity-dist"
    ], help="Analysis step to run")
    parser.add_argument("--grid-id", type=str, help="Target Grid ID (optional for some steps)")
    
    args = parser.parse_args()
    
    if args.step == "masks":
        run_masks(args.grid_id)
    elif args.step == "raster-stats":
        run_raster_stats(args.grid_id)
    elif args.step == "csv-stats":
        run_csv_stats(args.grid_id) # Changed to pass grid_id as per function signature
    elif args.step == "report":
        run_report() # Changed to not pass grid_id as per function signature
    elif args.step == "detailed":
        run_detailed_analysis(args.grid_id)
    elif args.step == "comprehensive":
        run_comprehensive_analysis(args.grid_id)
    elif args.step == "global-trends":
        run_global_trends()
    elif args.step == "outlier-evolution":
        run_outlier_evolution(args.grid_id)
    elif args.step == "intensity-dist":
        run_intensity_dist(args.grid_id)
    elif args.step == "all":
        # Run standard pipeline
        run_masks(args.grid_id)
        run_raster_stats(args.grid_id)
        run_csv_stats(args.grid_id) # Changed to pass grid_id as per function signature
        if args.grid_id:
            run_report() # Changed to not pass grid_id as per function signature

if __name__ == "__main__":
    main()
