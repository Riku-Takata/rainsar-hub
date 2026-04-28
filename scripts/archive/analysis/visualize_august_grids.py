
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
import re
import warnings

# Try importing contextily
try:
    import contextily as ctx
    HAS_CTX = True
except ImportError:
    HAS_CTX = False

warnings.filterwarnings('ignore')
plt.rcParams['font.family'] = 'MS Gothic'

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
CSV_PATH = BASE_DIR / "data/result/vv/diff/all_events_diff_vv.csv"
OUTPUT_DIR = BASE_DIR / "data/result/visualization"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def parse_grid_id(grid_id):
    # Format: N02615E12775 -> 26.15, 127.75
    # Assumes N...E... format with fixed decimal places implied or generic number capturing
    match = re.match(r'N(\d+)E(\d+)', grid_id)
    if match:
        lat_str = match.group(1)
        lon_str = match.group(2)
        
        # Based on N02615 -> 26.15, we divide by 100
        lat = float(lat_str) / 100.0
        lon = float(lon_str) / 100.0
        return lat, lon
    return None, None

def main():
    print("Loading event data...")
    if not CSV_PATH.exists():
        print("Error: Data file not found.")
        return

    df = pd.read_csv(CSV_PATH)
    
    # Filter August using the 'month' column
    # Ensure month column exists and handle potential types
    if 'month' in df.columns:
        df_aug = df[df['month'] == 8].copy()
    else:
        print("Error: 'month' column not found in CSV.")
        # Fallback to parsing event_name if necessary, but 'month' should be there based on inspection
        return

    print(f"Total August events found: {len(df_aug)}")
    
    unique_grids = df_aug['grid_id'].unique()
    print(f"Found {len(unique_grids)} unique grids for August.")
    
    # Parse Coordinates
    grid_coords = []
    for gid in unique_grids:
        lat, lon = parse_grid_id(gid)
        if lat is not None:
            grid_coords.append({'grid_id': gid, 'lat': lat, 'lon': lon})
            
    df_coords = pd.DataFrame(grid_coords)
    
    if df_coords.empty:
        print("No valid coordinates parsed.")
        return
        
    print(df_coords.head())


    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df_coords, geometry=gpd.points_from_xy(df_coords.lon, df_coords.lat), crs="EPSG:4326"
    )
    
    # Plot
    fig, ax = plt.subplots(figsize=(12, 12))
    
    if HAS_CTX:
        print("Using Contextily for basemap...")
        # Reproject to Web Mercator for contextily
        gdf_web = gdf.to_crs(epsg=3857)
        gdf_web.plot(ax=ax, color='red', markersize=30, alpha=0.8, edgecolor='white', linewidth=0.5, label='Target Grids (Aug)')
        
        # Add basemap
        try:
            ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
        except Exception as e:
            print(f"Contextily failed: {e}")
            # Fallback to simple plot if tile fetch fails
            gdf.plot(ax=ax, color='red')
    else:
        print("Contextily not found. Trying to load vector basemap...")
        # Fallback to Natural Earth or generic World
        try:
            # Try loading simplified world map from URL if local fails
            url = "https://raw.githubusercontent.com/holtzy/D3-graph-gallery/master/DATA/world.geojson"
            world = gpd.read_file(url)
            japan = world[world.name == "Japan"]
            if japan.empty:
                 # Fallback to filter by coordinates if name filtering fails
                 japan = world.cx[120:150, 30:46]
            
            japan.plot(ax=ax, color='#f0f0f0', edgecolor='gray')
            gdf.plot(ax=ax, color='red', markersize=20, alpha=0.7, label='Target Grids (Aug)')
        except Exception as e:
            print(f"Could not load vector basemap: {e}")
            # Just plot points
            gdf.plot(ax=ax, color='red', markersize=20, label='Target Grids (Aug)')

    # Labels and Grid
    plt.title("8月の分析対象主要Grid分布")
    
    if not HAS_CTX:
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
    else:
        ax.set_axis_off() # Hide axis for tile map usually looks better, or keep for reference
        # Keep axis for scientific context
        plt.xlabel("X (Web Mercator)")
        plt.ylabel("Y (Web Mercator)")

    plt.legend()
    # plt.grid(True, linestyle='--', alpha=0.5) # Grid lines might interfere with map tiles
    
    out_path = OUTPUT_DIR / "august_grids_map.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Saved map to {out_path}")


if __name__ == "__main__":
    main()
