#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/download_kyushu_pairs.py

九州地方の s1_pairs をダウンロードする。

フィルタリングロジック:
1. DBフィルタ: `is_rice_paddy` (候補) または `is_highway` が True の地点を抽出。
2. 厳密チェック:
   - 道路フラグがある場合 -> ダウンロード対象。
   - 田んぼ候補の場合 -> そのイベントの日時(event_start_ts_utc)に対応する年度の
     JAXA土地利用図を参照し、本当に「水田(#3)」であるかを確認。
"""

from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Set
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import or_
import rasterio

from app.db.session import SessionLocal
from app.db import models
from app.core.config import settings
from app.services.s1_cdse_client import S1CDSEClient

logger = logging.getLogger(__name__)

KYUSHU_BBOX = {
    "min_lat": 30.9, "max_lat": 34.5,
    "min_lon": 129.0, "max_lon": 132.5,
}

JAXA_ROOT = Path("/data/jaxa")
JAXA_PADDY_VAL = 3

def get_jaxa_folder_for_date(dt: datetime) -> Optional[Path]:
    """日付に応じたJAXAデータフォルダを返す"""
    year = dt.year
    
    # フォルダ名の定義
    # 2018-2020JPN_v21.11_10m
    # 2020JPN_v25.04
    # 2024JPN_v25.04
    
    folder_name = None
    if year <= 2019:
        folder_name = "2018-2020JPN_v21.11_10m"
    elif year <= 2023:
        # 2020~2023は 2020JPN 版を使用（適宜調整）
        folder_name = "2020JPN_v25.04"
    else:
        # 2024~
        folder_name = "2024JPN_v25.04"
        
    path = JAXA_ROOT / folder_name
    if not path.exists():
        # logger.warning(f"JAXA folder not found: {path}")
        return None
    return path

def is_actually_paddy(lat: float, lon: float, event_dt: datetime) -> bool:
    """指定日時のJAXAデータを参照して水田か判定"""
    folder = get_jaxa_folder_for_date(event_dt)
    if not folder:
        return False
        
    lat_int = int(math.floor(lat))
    lon_int = int(math.floor(lon))
    pattern = f"*N{lat_int:02d}E{lon_int:03d}*.tif"
    
    candidates = list(folder.glob(pattern))
    if not candidates:
        return False
        
    target_file = candidates[0]
    try:
        with rasterio.open(target_file) as src:
            row, col = src.index(lon, lat)
            # 範囲チェック
            if row < 0 or col < 0 or row >= src.height or col >= src.width:
                return False
            
            data = src.read(1, window=((row, row+1), (col, col+1)))
            if data.size > 0 and int(data[0, 0]) == JAXA_PADDY_VAL:
                return True
    except Exception:
        return False
        
    return False

def get_target_pairs(db: Session) -> Dict[str, List[models.S1Pair]]:
    logger.info(f"Querying DB for Kyushu region with Candidate Filter...")
    
    # 1. まずDB上のフラグ（候補）で絞り込み
    pairs = db.query(models.S1Pair, models.JapanGrid).join(
        models.JapanGrid, models.S1Pair.grid_id == models.JapanGrid.grid_id
    ).filter(
        models.S1Pair.lat >= KYUSHU_BBOX["min_lat"],
        models.S1Pair.lat <= KYUSHU_BBOX["max_lat"],
        models.S1Pair.lon >= KYUSHU_BBOX["min_lon"],
        models.S1Pair.lon <= KYUSHU_BBOX["max_lon"],
        or_(
            models.JapanGrid.is_rice_paddy.is_(True),
            models.JapanGrid.is_highway.is_(True)
        )
    ).all()
    
    grid_map = defaultdict(list)
    verified_count = 0
    skipped_count = 0
    
    logger.info(f"Processing {len(pairs)} candidate pairs for strict validation...")

    # 2. 各ペアについて厳密チェック
    for pair, grid_info in pairs:
        # 道路なら即採用
        if grid_info.is_highway:
            grid_map[pair.grid_id].append(pair)
            verified_count += 1
            continue
            
        # 田んぼ候補なら、日付チェック
        if grid_info.is_rice_paddy:
            # DBの日時が naive なら UTC にするなどの配慮
            evt_time = pair.event_start_ts_utc
            
            if is_actually_paddy(pair.lat, pair.lon, evt_time):
                grid_map[pair.grid_id].append(pair)
                verified_count += 1
            else:
                skipped_count += 1
    
    logger.info(f"Validation Result: Accepted={verified_count}, Skipped={skipped_count} (Not paddy at that time)")
    return grid_map

def select_best_pairs(grid_pairs: List[models.S1Pair]) -> List[models.S1Pair]:
    immediate = []
    delayed = []
    for p in grid_pairs:
        if p.delay_h is None: continue
        if 0 <= p.delay_h < 2.0: immediate.append(p)
        elif 5.0 <= p.delay_h <= 12.0: delayed.append(p)
    
    results = []
    if immediate: results.append(min(immediate, key=lambda x: x.delay_h))
    if delayed: results.append(min(delayed, key=lambda x: x.delay_h))
    return results

def download_scenes(grid_id: str, scene_ids: Set[str], client: S1CDSEClient) -> None:
    save_dir = settings.s1_safe_path / grid_id
    save_dir.mkdir(parents=True, exist_ok=True)
    for sid in scene_ids:
        if not sid: continue
        try:
            client.download_product(sid, save_dir)
        except Exception as e:
            logger.error(f"[{grid_id}] Download failed for {sid}: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    db = SessionLocal()
    client = S1CDSEClient()

    try:
        # 厳密フィルタリング済みのペアを取得
        grid_map = get_target_pairs(db)
        logger.info(f"Target Grids after strict check: {len(grid_map)}")

        download_tasks = []
        total_imm = 0
        total_del = 0

        for grid_id, pairs in grid_map.items():
            selected = select_best_pairs(pairs)
            if not selected: continue
            
            scenes = set()
            for p in selected:
                if p.delay_h < 2.0: total_imm += 1
                else: total_del += 1
                if p.after_scene_id: scenes.add(p.after_scene_id)
                if p.before_scene_id: scenes.add(p.before_scene_id)
            
            if scenes:
                download_tasks.append((grid_id, scenes))

        logger.info(f"--- Download Plan ---")
        logger.info(f"Target Grids : {len(download_tasks)}")
        logger.info(f"Pairs (0-2h): {total_imm}, (5-12h): {total_del}")
        
        if args.dry_run:
            return

        logger.info(f"Starting download with {args.workers} workers...")
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_grid = {
                executor.submit(download_scenes, gid, sids, client): gid 
                for gid, sids in download_tasks
            }
            for future in as_completed(future_to_grid):
                try: future.result()
                except Exception as e: logger.error(f"Error: {e}")

        logger.info("All downloads completed.")

    finally:
        db.close()

if __name__ == "__main__":
    main()