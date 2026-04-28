import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
import rasterio
from shapely.geometry import box

DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
EXPANDED_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
PIXEL_CSV = DATA_DIR / "monthly_delay_pixel_counts_detailed.csv"
RIVER_FILES = [
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320001.geojson"),
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320061.geojson")
]
JAPAN_GEOJSON = Path(r"D:\sotsuron\rainsar-hub\backend\app\utils\data\japan.geojson")

OUTPUT_LIST = DATA_DIR / "grid_list_0_3h.txt"

def main():
    print("Mapping 0-3h Delay Grids...")
    
    if not PIXEL_CSV.exists():
        print(f"Error: {PIXEL_CSV} not found.")
        return
        
    df = pd.read_csv(PIXEL_CSV)
    
    # Target: Months 8, 10 AND Delays 0, 1, 2, 3
    targets = [
        (8, 0), (8, 1), (8, 2), (8, 3),
        (10, 0), (10, 1), (10, 2), (10, 3)
    ]
    
    # Load Geometries (Backgrounds)
    gdf_japan = None
    if JAPAN_GEOJSON.exists():
        try:
            gdf_japan = gpd.read_file(JAPAN_GEOJSON)
        except: pass
        
    river_gdfs = []
    for p in RIVER_FILES:
        if p.exists():
            river_gdfs.append(gpd.read_file(p))
    gdf_river = pd.concat(river_gdfs, ignore_index=True) if river_gdfs else None
    
    plt.rcParams['font.family'] = 'Meiryo'
    
    with open(OUTPUT_LIST, 'w', encoding='utf-8') as f:
        f.write("Target Grids for Delays 0-3h (Aug/Oct)\n")
        f.write("=======================================\n\n")
        
        for m, d in targets:
            print(f"\nProcessing {m}月 Delay {d}h...")
            f.write(f"--- {m}月 Delay {d}h ---\n")
            
            # Find grids
            target_df = df[(df['month'] == m) & (df['delay_int'] == d)]
            grids = target_df['grid_id'].unique()
            grids.sort()
            
            print(f"  Found {len(grids)} grids.")
            f.write(f"Count: {len(grids)}\n")
            f.write(f"Grids: {', '.join(grids)}\n\n")
            
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
                try:
                    minx, miny, maxx, maxy = gdf_grads_case.total_bounds
                    ax.set_xlim(minx - 1.0, maxx + 1.0)
                    ax.set_ylim(miny - 1.0, maxy + 1.0)
                except:
                    pass
                
                # Labels - only if not too clustered, usually OK for small N
                # If N is large (>50), maybe skip text to avoid mess
                if len(gdf_grads_case) < 50:
                    for idx, r in gdf_grads_case.iterrows():
                        ax.text(r.geometry.centroid.x, r.geometry.centroid.y, r['grid_id'], 
                                fontsize=8, color='darkred', ha='center')
                        
            plt.title(f"{m}月 Delay {d}h 対象グリッド ({len(grids)}箇所)", fontsize=16)
            plt.xlabel("経度")
            plt.ylabel("緯度")
            
            out_path = DATA_DIR / f"map_target_{m}m_{d}h.png"
            plt.savefig(out_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"  Saved map to {out_path}")

    print(f"\nSaved grid list to {OUTPUT_LIST}")

if __name__ == "__main__":
    main()
