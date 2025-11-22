import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Polygon
import io

def fetch_osm_data():
    # Query for a small area in Tokyo
    # Rice paddies: landuse=farmland or landuse=paddy
    # Major roads: highway=motorway/trunk or lanes>=4
    overpass_url = "http://overpass-api.de/api/interpreter"
    overpass_query = """
    [out:json][timeout:25];
    (
      way["landuse"="farmland"](35.8,139.6,35.9,139.8);
      way["landuse"="paddy"](35.8,139.6,35.9,139.8);
      way["highway"~"motorway|trunk"](35.8,139.6,35.9,139.8);
      way["lanes"~"^[4-9]"](35.8,139.6,35.9,139.8);
    );
    out geom;
    """
    
    print("Fetching data from Overpass API...")
    response = requests.get(overpass_url, params={'data': overpass_query})
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        print(response.text)
        return

    data = response.json()
    print(f"Received {len(data.get('elements', []))} elements")

    # Convert to GeoDataFrame
    features = []
    for element in data['elements']:
        if element['type'] == 'way':
            tags = element.get('tags', {})
            geom_points = element.get('geometry', [])
            if not geom_points:
                continue
            
            coords = [(p['lon'], p['lat']) for p in geom_points]
            
            # Determine type
            feature_type = 'unknown'
            if tags.get('landuse') in ['farmland', 'paddy']:
                feature_type = 'rice_paddy' # Potential
                geom = Polygon(coords) if len(coords) >= 3 else LineString(coords)
            elif tags.get('highway') in ['motorway', 'trunk'] or int(tags.get('lanes', 0)) >= 4:
                feature_type = 'major_road'
                geom = LineString(coords)
            else:
                continue

            features.append({
                'id': element['id'],
                'type': feature_type,
                'tags': str(tags),
                'geometry': geom
            })

    if not features:
        print("No features found")
        return

    gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
    print(gdf.head())
    print(gdf['type'].value_counts())
    
    # Save to check
    gdf.to_file("test_osm_data.geojson", driver="GeoJSON")
    print("Saved to test_osm_data.geojson")

if __name__ == "__main__":
    fetch_osm_data()
