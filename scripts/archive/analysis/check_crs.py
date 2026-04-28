import rasterio
import json
import geopandas as gpd
from pathlib import Path

# Sample paths (from previous diagnosis)
# Grid: N03385E13065 (Only GeoJSON)
GEOJSON_PATH = Path(r"D:\sotsuron\rainsar-hub\data\expanded\masks\N03385E13065\N03385E13065_paddy.geojson")

# Find an event for this grid
EVENT_DIR = next(Path(r"D:\sotsuron\rainsar-hub\data\expanded\samples\N03385E13065").iterdir())
S1_PATH = EVENT_DIR / "after_vv.tif"

def main():
    print(f"Checking CRS for Grid: N03385E13065")
    
    # Check S1
    if S1_PATH.exists():
        with rasterio.open(S1_PATH) as src:
            print(f"\n[S1 Image]")
            print(f"Path: {S1_PATH}")
            print(f"CRS: {src.crs}")
            print(f"Bounds: {src.bounds}")
            print(f"Transform: {src.transform}")
            print(f"Shape: {src.shape}")
    else:
        print("S1 Image not found.")

    # Check GeoJSON
    if GEOJSON_PATH.exists():
        print(f"\n[GeoJSON]")
        print(f"Path: {GEOJSON_PATH}")
        try:
            gdf = gpd.read_file(GEOJSON_PATH)
            print(f"CRS: {gdf.crs}")
            print(f"Total Features: {len(gdf)}")
            print(f"Bounds: {gdf.total_bounds}")
        except Exception as e:
            print(f"Error reading GeoJSON: {e}")
            import json
            with open(GEOJSON_PATH) as f:
                data = json.load(f)
                print("Raw JSON top level keys:", data.keys())
                if 'crs' in data:
                    print("JSON CRS:", data['crs'])
    else:
        print("GeoJSON not found.")

if __name__ == "__main__":
    main()
