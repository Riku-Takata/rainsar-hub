import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
import rasterio
from shapely.geometry import box
import textwrap

DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
EXPANDED_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
PIXEL_CSV = DATA_DIR / "monthly_delay_pixel_counts_detailed.csv"
RIVER_FILES = [
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320001.geojson"),
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320061.geojson")
]
JAPAN_GEOJSON = Path(r"D:\sotsuron\rainsar-hub\backend\app\utils\data\japan.geojson")

PIXEL_RESULTS_CSV = DATA_DIR / "aug_oct_pixel_counts_with_river.csv"

def main():
    print("Mapping Low River Pixel Grids...")
    
    # 1. Identify Low River Delays (< 100 pixels)
    if not PIXEL_RESULTS_CSV.exists():
        print(f"Error: {PIXEL_RESULTS_CSV} not found.")
        return
        
    df_res = pd.read_csv(PIXEL_RESULTS_CSV)
    low_cases = df_res[df_res['river_pixels'] < 200] # Threshold < 200 to capture 115, 98 etc roughly
    
    print("Low River Pixel Cases (< 200):")
    print(low_cases[['month', 'delay_int', 'river_pixels']])
    
    # 2. For each case, find the contributing grids
    # We need the DETAILED breakdown again to know WHICH grids had these delays.
    # The detailed CSV `monthly_delay_pixel_counts_detailed.csv` lists all checks.
    # Wait, `monthly_delay_pixel_counts_detailed.csv` DOES NOT have river pixel info.
    # We calculated river pixels in `count_pixels_with_rivers.py` but we only saved the AGGREGATE to `aug_oct_pixel_counts_with_river.csv`.
    # AND we didn't save a detailed version of river counts?
    # Ah, `count_pixels_with_rivers.py` main() had `results` list but only saved AGGREGATE `agg_df`.
    # It printed "Processed X events" but didn't save detailed CSV.
    #
    # However, since we know which Month/Delay has low counts, we can filter `monthly_delay_pixel_counts_detailed.csv`
    # (which has grid_id) by that Month/Delay to find candidate grids.
    # Since these are aggregated statistics, ALL grids contributing to that (Month, Delay) bin are "target grids".
    
    df_detailed = pd.read_csv(PIXEL_CSV)
    
    # Load Geometries (Backgrounds)
    gdf_japan = None
    if JAPAN_GEOJSON.exists():
        gdf_japan = gpd.read_file(JAPAN_GEOJSON)
        
    river_gdfs = []
    for p in RIVER_FILES:
        if p.exists():
            river_gdfs.append(gpd.read_file(p))
    gdf_river = pd.concat(river_gdfs, ignore_index=True) if river_gdfs else None
    
    plt.rcParams['font.family'] = 'Meiryo'
    
    # Iterate key cases
    # Specifically targeting the ones distinct from 1h if possible, or all low ones.
    # Let's iterate unique (Month, Delay) in low_cases
    
    for _, row in low_cases.iterrows():
        m = int(row['month'])
        d = int(row['delay_int'])
        count = int(row['river_pixels'])
        
        print(f"\nProcessing {m}月 Delay {d}h (Pixels: {count})...")
        
        # Find grids
        target_df = df_detailed[(df_detailed['month'] == m) & (df_detailed['delay_int'] == d)]
        grids = target_df['grid_id'].unique()
        
        print(f"  Found {len(grids)} grids: {grids}")
        
        if len(grids) == 0:
            continue
            
        # Get Geoms
        grid_geoms = []
        for grid_id in grids:
            grid_dir = EXPANDED_DIR / grid_id
            event_dir = next(grid_dir.iterdir(), None)
            if event_dir:
                s1_path = event_dir / "after_vv.tif"
                if s1_path.exists():
                    try:
                        with rasterio.open(s1_path) as src:
                            b = src.bounds
                            geom = box(b.left, b.bottom, b.right, b.top)
                            grid_geoms.append({'grid_id': grid_id, 'geometry': geom})
                    except:
                        pass
                        
        gdf_grads_case = gpd.GeoDataFrame(grid_geoms, crs="EPSG:4326")
        
        # Plot
        fig, ax = plt.subplots(figsize=(10, 10))
        
        if gdf_japan is not None:
            gdf_japan.plot(ax=ax, color='#f0f0f0', edgecolor='#cccccc')
        if gdf_river is not None:
            gdf_river.plot(ax=ax, color='lightblue', edgecolor='blue', alpha=0.5)
            
        if not gdf_grads_case.empty:
            gdf_grads_case.plot(ax=ax, color='none', edgecolor='red', linewidth=2)
            
            # Auto zoom
            minx, miny, maxx, maxy = gdf_grads_case.total_bounds
            ax.set_xlim(minx - 2.0, maxx + 2.0)
            ax.set_ylim(miny - 2.0, maxy + 2.0)
            
            # Labels
            # If too many grids, maybe skip labels or just label a few
            if len(gdf_grads_case) < 30:
                for idx, r in gdf_grads_case.iterrows():
                    ax.text(r.geometry.centroid.x, r.geometry.centroid.y, r['grid_id'], fontsize=8, color='darkred')
                    
        plt.title(f"{m}月 Delay {d}h 対象グリッド (River Pixels: {count})", fontsize=16)
        plt.xlabel("経度")
        plt.ylabel("緯度")
        
        out_path = DATA_DIR / f"map_low_river_{m}m_{d}h.png"
        plt.savefig(out_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  Saved map to {out_path}")

if __name__ == "__main__":
    main()
