# Scripts Directory Organization

This directory has been reorganized to consolidate similar functionalities into modular scripts.

## New Structure

### Core Modules
*   **`common_utils.py`**: Contains shared functions (path settings, grid ID parsing, summary text parsing, etc.) used by other scripts.
*   **`process_masks.py`**: Consolidates mask generation logic.
    *   Generates highway masks from Shapefiles.
    *   Generates paddy masks from JAXA LULC data.
    *   Replaces: `generate_masked_images.py`, `generate_paddy_masks.py`.
*   **`analyze_rasters.py`**: Consolidates raster-based analysis.
    *   Performs negative pixel analysis and sigma-clipped statistics.
    *   Replaces: `analyze_negative_pixels.py`, `analyze_sigma_clipped_stats.py`, `analyze_single_grid_negative.py`.
*   **`analyze_stats.py`**: Consolidates CSV-based statistical analysis.
    *   Aggregates stats, detects noise, analyzes trends, and plots distributions.
    *   Replaces: `analyze_aggregated_stats.py`, `analyze_filtered_stats.py`, `analyze_stats_distribution.py`, `aggregate_stats.py`.
*   **`manage_reports.py`**: Consolidates Markdown report generation.
    *   Writes `result.md`, appends tables to `summary.md`, etc.
    *   Replaces: `write_result_md.py`, `update_result_md.py`, `add_single_grid_table.py`.

### Main Runner
*   **`run_pipeline.py`**: The main entry point to run the analysis pipeline.
    *   Usage: `python scripts/run_pipeline.py [step]`
    *   Steps: `masks`, `raster-stats`, `csv-stats`, `report`, `all`

## Legacy Files
The original files have been kept for reference but can be removed once the new workflow is verified.
