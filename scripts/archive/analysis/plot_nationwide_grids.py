import pandas as pd
import folium
import re
from pathlib import Path
import logging
from branca.colormap import LinearColormap

# Setup paths
BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_grid_summary.csv"
OUTPUT_HTML = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "search_results_map.html"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("plot_map")

def decode_grid_id(grid_id: str):
    """
    N03675E13685 -> (36.75, 136.85)
    """
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, grid_id)
    if not m:
        return None
    ns, lat_str, ew, lon_str = m.groups()
    lat = float(lat_str) / 100.0
    if ns == 'S': lat = -lat
    lon = float(lon_str) / 100.0
    if ew == 'W': lon = -lon
    return lat, lon

def main():
    if not INPUT_CSV.exists():
        logger.error(f"Input file not found: {INPUT_CSV}")
        return

    logger.info("Reading grid summary...")
    df = pd.read_csv(INPUT_CSV)
    
    # Calculate coords
    logger.info("Decoding grid coordinates...")
    coords = df['grid_id'].apply(decode_grid_id)
    df['lat'] = [c[0] if c else None for c in coords]
    df['lon'] = [c[1] if c else None for c in coords]
    df = df.dropna(subset=['lat', 'lon'])
    
    # Create Filter for "High Count" vs "Low Count" if needed
    # But for now, just plot all with color scale
    
    # Base Map (Japan Center approx)
    m = folium.Map(location=[38.0, 137.0], zoom_start=5, tiles="CartoDB positron")
    
    # Colormap
    max_count = df['pair_count'].max()
    # Log-ish scale might be better, but let's try linear first
    cmap = LinearColormap(colors=['blue', 'green', 'yellow', 'red'], vmin=1, vmax=min(max_count, 20))
    cmap.caption = "Number of Pairs Found"
    cmap.add_to(m)
    
    logger.info(f"Plotting {len(df)} grids...")
    
    for _, row in df.iterrows():
        lat, lon = row['lat'], row['lon']
        count = row['pair_count']
        
        # Grid Size (approx 0.1 deg)
        # Create a rectangle
        half = 0.05
        bounds = [[lat - half, lon - half], [lat + half, lon + half]]
        
        folium.Rectangle(
            bounds=bounds,
            color=cmap(count),
            fill=True,
            fill_opacity=0.6,
            weight=1,
            popup=f"Grid: {row['grid_id']}<br>Pairs: {count}<br>MinDelay: {row['min_delay']:.1f}h",
            tooltip=f"{row['grid_id']} ({count})"
        ).add_to(m)

    # Save
    m.save(OUTPUT_HTML)
    logger.info(f"Saved map to {OUTPUT_HTML}")

if __name__ == "__main__":
    main()
