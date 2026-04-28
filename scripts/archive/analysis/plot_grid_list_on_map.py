import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
import rasterio
from shapely.geometry import box
import re

DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\data\analysis")
EXPANDED_DIR = Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples")
GRID_LIST_TXT = DATA_DIR / "grid_list_0_3h.txt"
OUTPUT_MAP = DATA_DIR / "map_all_0_3h_grids.png"

RIVER_FILES = [
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320001.geojson"),
    Path(r"D:\sotsuron\rainsar-hub\mask-data\river_polygon_2320061.geojson")
]
JAPAN_GEOJSON = Path(r"D:\sotsuron\rainsar-hub\backend\app\utils\data\japan.geojson")

def main():
    print("Plotting consolidated grid list...")
    
    if not GRID_LIST_TXT.exists():
        print(f"Error: {GRID_LIST_TXT} not found.")
        return
        
    # 1. Parse Text File (August Only)
    unique_grids = set()
    with open(GRID_LIST_TXT, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    current_month = None
    
    # Simple state machine to parse "--- X月 Delay Yh ---"
    for line in lines:
        line = line.strip()
        header_match = re.match(r"--- (\d+)月 Delay (\d+)h ---", line)
        if header_match:
            current_month = int(header_match.group(1))
            continue
            
        if current_month == 8 and line.startswith("Grids: "):
            # Extract grids
            content = line.replace("Grids: ", "")
            grids = content.split(", ")
            unique_grids.update(grids)
        
    sorted_grids = sorted(list(unique_grids))
    print(f"Found {len(sorted_grids)} unique grids for August (0-3h).")
    
    if not sorted_grids:
        return

    # 2. Get Geometries
    grid_geoms = []
    # ... (same)
    
    for grid_id in sorted_grids:
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
                    
    gdf_grids = gpd.GeoDataFrame(grid_geoms, crs="EPSG:4326")
    
    # 3. Backgrounds
    gdf_japan = None
    if JAPAN_GEOJSON.exists():
        try: gdf_japan = gpd.read_file(JAPAN_GEOJSON)
        except: pass

    river_gdfs = []
    for p in RIVER_FILES:
        if p.exists():
            river_gdfs.append(gpd.read_file(p))
    gdf_river = pd.concat(river_gdfs, ignore_index=True) if river_gdfs else None
    
    # 4. Plot
    plt.rcParams['font.family'] = 'Meiryo'
    fig, ax = plt.subplots(figsize=(12, 12))
    
    if gdf_japan is not None:
        gdf_japan.plot(ax=ax, color='#f0f0f0', edgecolor='#cccccc', label='Japan')
    if gdf_river is not None:
        gdf_river.plot(ax=ax, color='lightblue', edgecolor='blue', alpha=0.5, label='River')
        
    if not gdf_grids.empty:
        gdf_grids.plot(ax=ax, color='none', edgecolor='red', linewidth=1.5, label='Target Grids (Aug 0-3h)')
        
        # Zoom logic
        minx, miny, maxx, maxy = gdf_grids.total_bounds
        ax.set_xlim(minx - 1.0, maxx + 1.0)
        ax.set_ylim(miny - 1.0, maxy + 1.0)
        
        # Labels if feasible
        if len(gdf_grids) < 100:
             for idx, r in gdf_grids.iterrows():
                ax.text(r.geometry.centroid.x, r.geometry.centroid.y, r['grid_id'], 
                        fontsize=6, color='darkred', ha='center')

    plt.title(f"8月 0-3h Delay 対象グリッド一覧 (全{len(sorted_grids)}箇所)", fontsize=16)
    plt.xlabel("経度")
    plt.ylabel("緯度")
    
    import matplotlib.patches as mpatches
    patches = []
    patches.append(mpatches.Patch(facecolor='none', edgecolor='red', label='対象グリッド (8月)'))
    if gdf_river is not None:
        patches.append(mpatches.Patch(color='lightblue', label='河川'))
    plt.legend(handles=patches, loc='upper right')

    OUTPUT_MAP_AUG = DATA_DIR / "map_aug_0_3h_grids.png"
    plt.savefig(OUTPUT_MAP_AUG, dpi=300, bbox_inches='tight')
    print(f"Saved consolidated map to {OUTPUT_MAP_AUG}")

if __name__ == "__main__":
    main()
