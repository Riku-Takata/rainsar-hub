import geopandas as gpd
from pathlib import Path

MASK_DATA_DIR = Path(r"D:\sotsuron\rainsar-hub\mask-data")
RIVER_FILES = [
    MASK_DATA_DIR / "river_polygon_2320001.geojson",
    MASK_DATA_DIR / "river_polygon_2320061.geojson"
]

def main():
    print("Inspecting River GeoJSONs...")
    
    for p in RIVER_FILES:
        if not p.exists():
            print(f"[MISSING] {p}")
            continue
            
        print(f"\n--- {p.name} ---")
        try:
            gdf = gpd.read_file(p)
            print(f"CRS: {gdf.crs}")
            print(f"Features: {len(gdf)}")
            print(f"Bounds: {gdf.total_bounds}")
            print(f"Columns: {gdf.columns.tolist()}")
            print("First row geometry type:", gdf.geometry.iloc[0].geom_type)
        except Exception as e:
            print(f"[ERROR] {e}")

if __name__ == "__main__":
    main()
