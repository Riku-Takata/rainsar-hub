import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
import rasterio
from shapely.geometry import box

DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
EXPANDED_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
PIXEL_CSV = DATA_DIR / "monthly_delay_pixel_counts_detailed.csv" # Using detailed to find which grids contributed
RIVER_FILES = [
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320001.geojson"),
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320061.geojson")
]
OUTPUT_MAP = DATA_DIR / "map_delay_1h_locations.png"

def main():
    print("Mapping 1h Delay Grids...")
    
    # 1. Load Data/Filter
    df = pd.read_csv(PIXEL_CSV)
    # Filter: Delay=1, Month=8 or 10
    target_df = df[(df['delay_int'] == 1) & (df['month'].isin([8, 10]))]
    
    target_grids = target_df['grid_id'].unique()
    print(f"Found {len(target_grids)} unique grids for Delay 1h (Aug/Oct).")
    print("\nTarget Grids:", target_grids)
    
    # 2. Extract Geometry for these grids
    grid_geoms = []
    
    for grid_id in target_grids:
        grid_dir = EXPANDED_DIR / grid_id
        # Just grab the first available S1 to get bounds
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
    
    gdf_grids = gpd.GeoDataFrame(grid_geoms, crs="EPSG:4326")
    
    # 3. Load Rivers for context
    river_gdfs = []
    for p in RIVER_FILES:
        if p.exists():
            river_gdfs.append(gpd.read_file(p))
            
    if river_gdfs:
        gdf_river = pd.concat(river_gdfs, ignore_index=True)
    else:
        gdf_river = None
        
    # Load Japan Map
    JAPAN_GEOJSON = Path(r"D:\sotsuron\rainsar-hub\backend\app\utils\data\japan.geojson")
    gdf_japan = None
    if JAPAN_GEOJSON.exists():
        try:
            gdf_japan = gpd.read_file(JAPAN_GEOJSON)
        except Exception as e:
            print(f"Error loading Japan map: {e}")

    # 4. Plot
    # Set Japanese Font
    plt.rcParams['font.family'] = 'Meiryo'
    
    fig, ax = plt.subplots(figsize=(10, 10))
    
    # Plot Japan (Base)
    if gdf_japan is not None:
        gdf_japan.plot(ax=ax, color='#f0f0f0', edgecolor='#cccccc', label='Japan')

    # Plot Rivers (Background)
    if gdf_river is not None:
        gdf_river.plot(ax=ax, color='lightblue', edgecolor='blue', alpha=0.5, label='River')
        
    # Plot Grids
    if not gdf_grids.empty:
        gdf_grids.plot(ax=ax, color='none', edgecolor='red', linewidth=2, label='Target Grids (1h)')
        
        # Add labels
        for idx, row in gdf_grids.iterrows():
            # Simple offset for label
            ax.text(row.geometry.centroid.x, row.geometry.centroid.y, row['grid_id'], 
                    fontsize=8, ha='center', color='darkred')
    
    # Zoom to grids area with some padding
    if not gdf_grids.empty:
        minx, miny, maxx, maxy = gdf_grids.total_bounds
        # Add some padding to see context
        pad_x = (maxx - minx) * 2.0
        pad_y = (maxy - miny) * 2.0
        # Ensure we don't zoom out too much if grids are very far apart, but here they seem close.
        # Actually, let's just let it auto-scale or set a reasonable limit.
        # If we zoom to Japan, it will be too small.
        # Let's zoom to the union of grids and rivers if rivers are relevant.
        # Given rivers are specific polygons, let's focus on the area around the target grids.
        
        ax.set_xlim(minx - 1.0, maxx + 1.0)
        ax.set_ylim(miny - 1.0, maxy + 1.0)
    
    plt.title("Delay 1h 対象グリッド位置 (8月・10月)", fontsize=16)
    plt.xlabel("経度", fontsize=12)
    plt.ylabel("緯度", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.5)
    
    # Handle Legend manually since GPD plot doesn't always play nice with auto-legend
    import matplotlib.patches as mpatches
    patches = []
    if gdf_river is not None:
        patches.append(mpatches.Patch(color='lightblue', label='河川 (River)'))
    if not gdf_grids.empty:
        patches.append(mpatches.Patch(facecolor='none', edgecolor='red', label='対象グリッド (1h)'))
        
    plt.legend(handles=patches)
    
    plt.savefig(OUTPUT_MAP, dpi=300, bbox_inches='tight')
    print(f"Saved map to {OUTPUT_MAP}")

if __name__ == "__main__":
    main()
