import pandas as pd
import numpy as np
from pathlib import Path
import rasterio
from rasterio.features import geometry_mask
import geopandas as gpd
import warnings

warnings.filterwarnings('ignore')

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
SAMPLES_DIR = BASE_DIR / "data" / "expanded" / "samples"
MASKS_DIR = BASE_DIR / "data" / "expanded" / "masks"
PIXEL_COUNTS_CSV = BASE_DIR / "data" / "analysis" / "monthly_delay_pixel_counts_detailed.csv"

def get_extensive_october_pairs():
    df_pixels = pd.read_csv(PIXEL_COUNTS_CSV)
    oct_events = df_pixels[df_pixels['month'] == 10]
    def extract_date(s):
        import re
        m = re.search(r'(\d{8})', s)
        return m.group(1) if m else s
    oct_events['date'] = oct_events['event_dir'].apply(extract_date)
    analysis_links = []
    for grid_id, group in oct_events.groupby('grid_id'):
        unique_dirs = group.sort_values('date').drop_duplicates(subset=['event_dir'])
        if len(unique_dirs) < 2: continue
        for i in range(len(unique_dirs) - 1):
            analysis_links.append({
                'grid_id': grid_id, 'dir_1': unique_dirs.iloc[i]['event_dir'], 'dir_2': unique_dirs.iloc[i+1]['event_dir']
            })
    return analysis_links

def debug_counts():
    links = get_extensive_october_pairs()
    road_counts, paddy_counts = [], []
    print(f"Investigating {len(links)} pairs...")
    
    for i, link in enumerate(links):
        grid_id = link['grid_id']
        p1_vv = SAMPLES_DIR / grid_id / link['dir_1'] / "before_vv.tif"
        if not p1_vv.exists(): continue
        
        road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
        if not road_mask_path.exists(): road_mask_path = MASKS_DIR / grid_id / f"{grid_id}_road.geojson"
        paddy_mask_path = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
        
        try:
            with rasterio.open(p1_vv) as src:
                h, w = src.height, src.width
                valid_mask = src.read(1) > 0 # Simple check
                
                if road_mask_path.exists():
                    gdf = gpd.read_file(road_mask_path)
                    if not gdf.empty:
                        mask = geometry_mask(gdf.to_crs(src.crs).geometry, (h, w), src.transform, invert=True)
                        road_counts.append(np.sum(mask & valid_mask))
                
                if paddy_mask_path.exists():
                    gdf = gpd.read_file(paddy_mask_path)
                    if not gdf.empty:
                        mask = geometry_mask(gdf.to_crs(src.crs).geometry, (h, w), src.transform, invert=True)
                        paddy_counts.append(np.sum(mask & valid_mask))
        except: continue
        
    print("\n[Road Availability]")
    df_r = pd.Series(road_counts)
    print(df_r.describe())
    print(f"Pairs with < 20 pixels: {np.sum(df_r < 20)} / {len(df_r)}")
    
    print("\n[Paddy Availability]")
    df_p = pd.Series(paddy_counts)
    print(df_p.describe())
    print(f"Pairs with < 20 pixels: {np.sum(df_p < 20)} / {len(df_p)}")

if __name__ == "__main__":
    debug_counts()
