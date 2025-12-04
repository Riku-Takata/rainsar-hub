#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/tag_japan_grids.py

japan_grids の各地点に対し、以下の基準でフラグを立てる（候補選定）。

1. is_rice_paddy (田んぼ候補):
   - JAXAデータの全年度フォルダを走査。
   - いずれかの年度で「水田 (#3)」であれば True とする。
   - 意味: "この場所は田んぼである可能性がある"

2. is_highway (道路確定):
   - OpenStreetMap (OSM) で高速道路・幹線道路を判定。
   - 道路は経年変化が少ないため、ここで確定とする。
"""

import logging
import argparse
import time
import math
from pathlib import Path
from typing import List

from sqlalchemy.orm import Session
import osmnx as ox
import rasterio

from app.db.session import SessionLocal
from app.db import models

logger = logging.getLogger(__name__)

JAXA_ROOT = Path("/data/jaxa")
JAXA_PADDY_VAL = 3

def get_all_jaxa_files(lat: float, lon: float) -> List[Path]:
    """
    全年度のフォルダから、指定座標を含むGeoTIFFファイルをすべて探す
    """
    lat_int = int(math.floor(lat))
    lon_int = int(math.floor(lon))
    pattern = f"*N{lat_int:02d}E{lon_int:03d}*.tif"
    
    found = []
    if JAXA_ROOT.exists():
        for folder in JAXA_ROOT.iterdir():
            if folder.is_dir():
                found.extend(list(folder.glob(pattern)))
    return found

def check_rice_paddy_candidate(lat: float, lon: float) -> bool:
    """
    全期間のデータをチェックし、一度でも水田なら True
    """
    files = get_all_jaxa_files(lat, lon)
    if not files:
        return False

    for tif_path in files:
        try:
            with rasterio.open(tif_path) as src:
                row, col = src.index(lon, lat)
                # 範囲チェック
                if row < 0 or col < 0 or row >= src.height or col >= src.width:
                    continue
                
                data = src.read(1, window=((row, row+1), (col, col+1)))
                if data.size > 0 and int(data[0, 0]) == JAXA_PADDY_VAL:
                    return True
        except Exception:
            continue
            
    return False

def check_highway(lat: float, lon: float, dist: int = 500) -> bool:
    """ OSMで道路判定 """
    point = (lat, lon)
    try:
        roads = ox.features_from_point(point, tags={'highway': ['motorway', 'trunk']}, dist=dist)
        return not roads.empty
    except Exception:
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ox.settings.log_console = False
    ox.settings.use_cache = True

    db = SessionLocal()
    try:
        if args.reset:
            logger.info("Resetting flags...")
            db.query(models.JapanGrid).update(
                {models.JapanGrid.is_rice_paddy: False, models.JapanGrid.is_highway: False}
            )
            db.commit()

        query = db.query(models.JapanGrid).order_by(models.JapanGrid.id)
        if args.limit > 0: query = query.limit(args.limit)
        if args.start_offset > 0: query = query.offset(args.start_offset)
            
        grids = query.all()
        total = len(grids)
        logger.info(f"Target grids: {total}")

        for i, grid in enumerate(grids, start=1):
            try:
                # 1. 田んぼ候補判定 (全期間チェック)
                is_rice = check_rice_paddy_candidate(grid.lat, grid.lon)
                
                # 2. 道路判定
                is_hway = check_highway(grid.lat, grid.lon)
                
                if is_rice != grid.is_rice_paddy or is_hway != grid.is_highway:
                    grid.is_rice_paddy = is_rice
                    grid.is_highway = is_hway
                    db.add(grid)
                    logger.info(f"[{i}/{total}] {grid.grid_id}: RiceCandidate={is_rice}, Highway={is_hway}")
                else:
                    if i % 50 == 0: logger.info(f"[{i}/{total}] No change.")

                time.sleep(0.1)
                if i % 50 == 0: db.commit()

            except Exception as e:
                logger.error(f"Error {grid.grid_id}: {e}")
                continue

        db.commit()
        logger.info("Tagging completed.")

    finally:
        db.close()

if __name__ == "__main__":
    main()