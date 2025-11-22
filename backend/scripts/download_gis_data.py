#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/download_gis_data.py

OpenStreetMap (Overpass API) から「田んぼ」と「大型道路」のデータを取得し、
GeoJSON または GeoPackage 形式で保存するスクリプト。

取得対象:
- 田んぼ (Rice Paddies):
    - landuse = farmland
    - landuse = paddy
- 大型道路 (Major Roads):
    - highway = motorway (高速道路)
    - highway = trunk (幹線道路)
    - lanes >= 4 (4車線以上)

使い方:
  # 日本付近のデータを取得して保存 (デフォルト: GeoJSON)
  python -m scripts.download_gis_data --output japan_gis_data.geojson --bbox 30 128 46 146

  # GeoPackage で保存
  python -m scripts.download_gis_data --output japan_gis_data.gpkg --format gpkg

  # 小さい範囲でテスト
  python -m scripts.download_gis_data --output test.geojson --bbox 35.6 139.6 35.7 139.8
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

import requests
import geopandas as gpd
from shapely.geometry import LineString, Polygon

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

OVERPASS_URL = "http://overpass-api.de/api/interpreter"

def fetch_osm_data(min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> List[Dict[str, Any]]:
    """
    Overpass API からデータを取得する。
    """
    # Overpass QL
    # timeout: タイムアウト時間(秒)
    # maxsize: メモリ制限(バイト) - 必要に応じて調整
    query = f"""
    [out:json][timeout:300];
    (
      way["landuse"="farmland"]({min_lat},{min_lon},{max_lat},{max_lon});
      way["landuse"="paddy"]({min_lat},{min_lon},{max_lat},{max_lon});
      way["highway"~"motorway|trunk"]({min_lat},{min_lon},{max_lat},{max_lon});
      way["lanes"~"^[4-9]"]({min_lat},{min_lon},{max_lat},{max_lon});
    );
    out geom;
    """
    
    logger.info("Querying Overpass API...")
    logger.debug("Query: %s", query)

    try:
        resp = requests.get(OVERPASS_URL, params={'data': query}, timeout=360)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to fetch data from Overpass API: %s", e)
        raise

    data = resp.json()
    elements = data.get("elements", [])
    logger.info("Fetched %d elements.", len(elements))
    return elements

def parse_elements(elements: List[Dict[str, Any]]) -> gpd.GeoDataFrame:
    """
    Overpass API のレスポンス (elements) を GeoDataFrame に変換する。
    """
    features = []
    
    for el in elements:
        if el["type"] != "way":
            continue
        
        tags = el.get("tags", {})
        geometry_pts = el.get("geometry", [])
        
        if not geometry_pts:
            continue
            
        coords = [(p["lon"], p["lat"]) for p in geometry_pts]
        if len(coords) < 2:
            continue

        # カテゴリ判定
        feature_type = "unknown"
        landuse = tags.get("landuse")
        highway = tags.get("highway")
        lanes = tags.get("lanes", "0")
        
        try:
            lanes_int = int(lanes)
        except ValueError:
            lanes_int = 0

        geom = None

        # 1. 田んぼ判定
        if landuse in ["farmland", "paddy"]:
            feature_type = "rice_paddy"
            # ポリゴンとして扱う (始点と終点が同じなら閉じる、そうでなくても landuse は面とみなすのが一般的だが、
            # Overpass の way は LineString で返ってくることが多い。Polygon に変換を試みる)
            if len(coords) >= 3:
                geom = Polygon(coords)
            else:
                # 3点未満なら LineString のまま (面積なし)
                geom = LineString(coords)

        # 2. 大型道路判定
        elif highway in ["motorway", "trunk"] or lanes_int >= 4:
            feature_type = "major_road"
            geom = LineString(coords)
        
        else:
            # クエリ条件には入っているが、タグの組み合わせでここに来る場合など
            continue

        if geom is not None:
            features.append({
                "osm_id": el["id"],
                "feature_type": feature_type,
                "landuse": landuse,
                "highway": highway,
                "lanes": lanes,
                "geometry": geom
            })

    if not features:
        logger.warning("No valid features found.")
        return gpd.GeoDataFrame(columns=["osm_id", "feature_type", "geometry"], crs="EPSG:4326")

    gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
    return gdf

def main():
    parser = argparse.ArgumentParser(description="Download GIS data (Rice Paddies & Major Roads) from OSM.")
    parser.add_argument("--output", type=str, required=True, help="Output file path (e.g. data.geojson, data.gpkg)")
    parser.add_argument("--format", type=str, choices=["geojson", "gpkg"], default="geojson", help="Output format (default: geojson)")
    parser.add_argument("--bbox", type=float, nargs=4, metavar=('MIN_LAT', 'MIN_LON', 'MAX_LAT', 'MAX_LON'),
                        default=[30.0, 128.0, 46.0, 146.0], # 日本全体をカバーするざっくりとした矩形
                        help="Bounding box to download (min_lat min_lon max_lat max_lon)")

    args = parser.parse_args()

    min_lat, min_lon, max_lat, max_lon = args.bbox
    logger.info("BBox: %.4f, %.4f, %.4f, %.4f", min_lat, min_lon, max_lat, max_lon)

    try:
        elements = fetch_osm_data(min_lat, min_lon, max_lat, max_lon)
        gdf = parse_elements(elements)
        
        logger.info("Feature counts:\n%s", gdf["feature_type"].value_counts())

        output_path = Path(args.output)
        if args.format == "gpkg":
            gdf.to_file(output_path, driver="GPKG")
        else:
            gdf.to_file(output_path, driver="GeoJSON")
            
        logger.info("Saved to %s", output_path.absolute())

    except Exception as e:
        logger.exception("An error occurred:")
        sys.exit(1)

if __name__ == "__main__":
    main()
