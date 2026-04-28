#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/import_and_map_road_polygons.py

- road-polygonディレクトリのGEOJSON.zipをパースする。
- 道路データを RoadPolygon テーブルに格納する。
- 同時に、全座標をGPU(CuPy)で並列処理し、道路が存在する GSMaP メッシュ (0.1度刻み) の grid_id を逆算する。
- 該当する一意な grid_id コレクションを japan_road_grids のような形式（または別のアプローチ）でDBに保存する。
"""

import sys
import copy
import logging
import argparse
import time
import zipfile
import json
from pathlib import Path
import traceback

import cupy as cp
import numpy as np
import torch


# SQLAlchemy
from sqlalchemy.orm import Session
from sqlalchemy import text, insert, BigInteger, String, Column, Float, Boolean, Table, MetaData
from app.db.session import SessionLocal
from app.db import models

logger = logging.getLogger(__name__)

# JapanRoadGrid という仮テーブルの定義(手動作成)
metadata = MetaData()
japan_road_grids_table = Table(
    'japan_road_grids', metadata,
    Column('grid_id', String(32), primary_key=True),
    Column('lat', Float, nullable=False),
    Column('lon', Float, nullable=False),
)

def setup_db(db: Session):
    # japan_road_grids が無ければ作る
    metadata.create_all(db.get_bind())

def lat_lon_to_grid_id(lat: float, lon: float) -> str:
    lat_i = int(round(lat * 100))
    lon_i = int(round(lon * 100))
    n_s = "N" if lat_i >= 0 else "S"
    e_w = "E" if lon_i >= 0 else "W"
    return f"{n_s}{abs(lat_i):05d}{e_w}{abs(lon_i):05d}"

def process_coordinates_gpu(all_coords):
    """
    all_coords: list of [lon, lat]
    1. PyTorch(CUDA) に転送
    2. 0.1度の GSMaP メッシュの中心座標を計算
       cell_lat = floor(lat * 10) / 10.0 + 0.05
       cell_lon = floor(lon * 10) / 10.0 + 0.05
    3. grid_idに必要な数値に変換
    4. 一意な (lat_i, lon_i) ペアを取り出してCPUに戻す
    """
    if not all_coords:
        return set(), []

    start = time.time()
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    logger.info(f"Using device for coordinate processing: {device}")
    
    # データをPyTorchテンソルに
    coords_tensor = torch.tensor(all_coords, dtype=torch.float32, device=device)
    lons = coords_tensor[:, 0]
    lats = coords_tensor[:, 1]
    
    # メッシュの中心へスナップさせる
    cell_lats = torch.floor(lats * 10.0) / 10.0 + 0.05
    cell_lons = torch.floor(lons * 10.0) / 10.0 + 0.05

    # GridID用整数に変換 (100倍して四捨五入)
    lat_i = torch.round(cell_lats * 100).to(torch.int32)
    lon_i = torch.round(cell_lons * 100).to(torch.int32)

    # lat_i と lon_i を結合して一意化 (e.g. upper 32bits, lower 32bits)
    packed = (lat_i.to(torch.int64) << 32) | (lon_i.to(torch.int64) & 0xFFFFFFFF)
    unique_packed = torch.unique(packed)
    
    unique_cpu = unique_packed.cpu().numpy()
    
    new_grid_ids = set()
    grid_details = []
    
    for val in unique_cpu:
        la_i = int(val >> 32)
        lo_i = int(val & 0xFFFFFFFF)
        # Sign extension manually due to 32-bit unpack
        if lo_i >= 0x80000000:
            lo_i -= 0x100000000
            
        ns = 'N' if la_i >= 0 else 'S'
        ew = 'E' if lo_i >= 0 else 'W'
        gid = f"{ns}{abs(la_i):05d}{ew}{abs(lo_i):05d}"
        
        real_lat = round(la_i / 100.0, 2)
        real_lon = round(lo_i / 100.0, 2)
        
        new_grid_ids.add(gid)
        grid_details.append((gid, real_lat, real_lon))
        
    logger.debug(f"GPU processing took {time.time() - start:.3f}s for {len(all_coords)} points.")
    return new_grid_ids, grid_details


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    road_dir = Path(r"D:\sotsuron\road-polygon")
    
    if not road_dir.exists():
        logger.error(f"Directory not found: {road_dir}")
        return

    db = SessionLocal()
    setup_db(db)

    zip_files = list(road_dir.glob("*.zip"))
    logger.info(f"Found {len(zip_files)} zip files.")

    total_global_grid_ids = set()
    all_grid_details = {}

    db_grid_stmt = insert(japan_road_grids_table).prefix_with('IGNORE')

    overall_start = time.time()
    
    for i, zpath in enumerate(zip_files, start=1):
        logger.info(f"[{i}/{len(zip_files)}] Processing {zpath.name} ...")
        
        db_records = []
        all_coords = []
        
        try:
            with zipfile.ZipFile(zpath, 'r') as z:
                for fname in z.namelist():
                    if not fname.endswith('.geojson'):
                        continue
                        
                    with z.open(fname) as f:
                        data = json.loads(f.read().decode('utf-8'))
                        features = data.get("features", [])
                        
                        for feat in features:
                            props = feat.get("properties", {})
                            geom = feat.get("geometry", {})
                            
                            if not geom:
                                continue
                                
                            # DBインサート用
                            rec = models.RoadPolygon(
                                n13_001=props.get("N13_001"),
                                n13_002=props.get("N13_002"),
                                n13_003=props.get("N13_003"),
                                n13_004=props.get("N13_004"),
                                n13_005=int(props.get("N13_005")) if props.get("N13_005") is not None else None,
                                n13_006=props.get("N13_006"),
                                n13_007=props.get("N13_007"),
                                n13_008=props.get("N13_008"),
                                geometry=geom
                            )
                            db_records.append(rec)
                            
                            # 座標抽出
                            ctype = geom.get("type")
                            coords = geom.get("coordinates", [])
                            if ctype == "LineString":
                                all_coords.extend(coords)
                            elif ctype == "MultiLineString":
                                for line in coords:
                                    all_coords.extend(line)

            # --- GPU処理 ---
            if all_coords:
                new_grid_ids, grid_details = process_coordinates_gpu(all_coords)
                total_global_grid_ids.update(new_grid_ids)
                for gid, la, lo in grid_details:
                    all_grid_details[gid] = (la, lo)

            # --- DBへSAVE ---
            if db_records:
                db.bulk_save_objects(db_records)
                db.commit()
            
            logger.info(f" -> Inserted {len(db_records)} road lines. Distinct grids so far: {len(total_global_grid_ids)}")

        except Exception as e:
            logger.error(f"Error processing {zpath.name}: {e}")
            traceback.print_exc()
            db.rollback()

    logger.info("Iterating finished. Now saving grid mappings to DB...")
    
    # 溜まった全GridIDを格納
    batch_size = 5000
    grid_list = list(all_grid_details.items())
    for chunk_idx in range(0, len(grid_list), batch_size):
        chunk = grid_list[chunk_idx:chunk_idx+batch_size]
        ins_data = [{"grid_id": gid, "lat": la, "lon": lo} for gid, (la, lo) in chunk]
        db.execute(db_grid_stmt, ins_data)
        db.commit()
        
    logger.info(f"Done! Saved {len(all_grid_details)} unique grids. Overall time: {time.time() - overall_start:.2f}s")
    db.close()

if __name__ == "__main__":
    main()
