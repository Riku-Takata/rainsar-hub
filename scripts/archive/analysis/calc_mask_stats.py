"""
Calculate statistics of mask areas (Paddy/Road) for expansion grids.
Reads GeoJSONs in data/expanded/masks and computes area statistics.
"""
import pandas as pd
import geopandas as gpd
import geopandas
from pathlib import Path
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
MASKS_DIR = BASE_DIR / "data/expanded/masks"
TARGET_CSV = BASE_DIR / "data/analysis/suggested_grids_quality.csv"

def main():
    if not TARGET_CSV.exists():
        print("Target CSV not found.")
        return

    df = pd.read_csv(TARGET_CSV)
    grids = df['grid_id'].tolist()
    
    print(f"Calculating stats for {len(grids)} grids...")
    
    stats = []
    
    for i, grid_id in enumerate(grids):
        paddy_path = MASKS_DIR / grid_id / f"{grid_id}_paddy.geojson"
        road_path = MASKS_DIR / grid_id / f"{grid_id}_motorway.geojson"
        
        paddy_area = 0.0
        road_area = 0.0
        
        # Calculate Paddy Area
        if paddy_path.exists():
            try:
                gdf = gpd.read_file(paddy_path)
                if not gdf.empty:
                    # Generic Japan UTM (Zone 54N) for area approx
                    gdf = gdf.to_crs(epsg=32654)
                    paddy_area = gdf.geometry.area.sum()
            except Exception as e:
                pass
                
        # Calculate Road Area
        if road_path.exists():
            try:
                gdf = gpd.read_file(road_path)
                if not gdf.empty:
                    gdf = gdf.to_crs(epsg=32654)
                    # Check geometry type
                    if gdf.geometry.type.iloc[0] in ['LineString', 'MultiLineString']:
                         road_area = gdf.geometry.length.sum() # Actually length in meters
                    else:
                         road_area = gdf.geometry.area.sum()
            except Exception as e:
                pass
        
        stats.append({
            'grid_id': grid_id,
            'paddy_area_m2': paddy_area,
            'road_metric_m': road_area, # Rename key conceptually
            'paddy_ratio': paddy_area / (10000 * 10000), 
            'road_ratio': 0 # Not applicable for length
        })
        
        if (i+1) % 10 == 0:
            print(f"Processed {i+1} grids")
        
        if i >= 49: # Limit to 50 for quick result
            break
            
    stats_df = pd.DataFrame(stats)
    
    # Summary
    print("\n--- Summary Statistics (First 50 Grids) ---")
    print(stats_df[['paddy_area_m2', 'road_metric_m']].describe().map(lambda x: f"{x:,.0f}"))
    
    # Save detailed
    out_path = BASE_DIR / "data/analysis/mask_stats_sample.csv"
    stats_df.to_csv(out_path, index=False)
    print(f"\nDetailed stats saved to {out_path}")

if __name__ == "__main__":
    main()
