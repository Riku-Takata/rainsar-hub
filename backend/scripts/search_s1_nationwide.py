#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/search_s1_nationwide.py

gsmap_events を「地点（Grid）」単位でグルーピングして Sentinel-1 画像を検索する。

判定ロジック:
1. ある Grid で発生した全ての降雨イベントについて、衛星画像を検索する（検索ウィンドウは広め: default 24h）。
2. その結果、1つでも「降雨後 N時間以内（default 2.0h）の画像」が見つかれば、その Grid は「観測好適地」とみなす。
3. 観測好適地と判定された場合、見つかった全てのペア（2時間を超えるものも含む）を s1_pairs に保存する。

特徴:
- ThreadPoolExecutor による並列検索
- 読み込み/書き込みセッションの分離
- Grid単位のトランザクション管理
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from app.db.session import SessionLocal
from app.db import models
from app.services.s1_cdse_client import S1CDSEClient

logger = logging.getLogger(__name__)


def normalize_mission(platform: Optional[str]) -> Optional[str]:
    if not platform:
        return None
    p = platform.lower()
    if "sentinel-1a" in p or p.endswith("1a") or p == "s1a":
        return "S1A"
    if "sentinel-1b" in p or p.endswith("1b") or p == "s1b":
        return "S1B"
    if "sentinel-1" in p:
        return "S1"
    return platform[:8].upper()


def normalize_pass_direction(direction: Optional[str]) -> Optional[str]:
    if not direction:
        return None
    d = direction.lower()
    if d.startswith("asc"):
        return "ASC"
    if d.startswith("des"):
        return "DSC"
    return direction[:3].upper()


def search_event_pair(
    event_data: dict,
    search_window_hours: float,
    client: S1CDSEClient,
) -> Optional[dict]:
    """
    1つのイベントに対して画像を検索し、見つかればペア情報を辞書で返す。
    見つからなければ None。
    """
    grid_id = event_data["grid_id"]
    lat = event_data["lat"]
    lon = event_data["lon"]
    end_utc = event_data["end_ts_utc"]

    # 1. AFTER シーン検索 (広いウィンドウで検索)
    try:
        after_scene = client.find_after_scene(
            lat=lat, lon=lon, event_end_utc=end_utc, after_hours=search_window_hours
        )
    except Exception as e:
        logger.warning(f"Search error grid={grid_id}: {e}")
        return None

    if not after_scene:
        return None

    # 2. BEFORE シーン検索 (Afterが見つかった場合のみ)
    before_scene = None
    try:
        before_scene = client.find_before_scene_unbounded(
            lat=lat,
            lon=lon,
            ref_time_utc=after_scene.acquisition_time,
            platform=after_scene.platform,
            orbit_direction=after_scene.orbit_direction,
            relative_orbit=after_scene.relative_orbit,
        )
    except Exception:
        pass  # Beforeは必須ではないので無視

    # 3. データ整形
    delay_h = (after_scene.acquisition_time - end_utc).total_seconds() / 3600.0
    
    after_id = after_scene.product_identifier or after_scene.stac_id
    before_id = (
        (before_scene.product_identifier or before_scene.stac_id)
        if before_scene
        else None
    )

    return {
        "grid_id": grid_id,
        "lat": lat,
        "lon": lon,
        "event_start_ts_utc": event_data["start_ts_utc"],
        "event_end_ts_utc": end_utc,
        "threshold_mm_h": event_data["threshold_mm_h"],
        "hit_hours": event_data["hit_hours"],
        "max_gauge_mm_h": event_data["max_gauge_mm_h"],
        
        "after_scene_id": after_id,
        "after_platform": after_scene.platform,
        "after_mission": normalize_mission(after_scene.platform),
        "after_pass_direction": normalize_pass_direction(after_scene.orbit_direction),
        "after_relative_orbit": after_scene.relative_orbit,
        "after_start_ts_utc": after_scene.acquisition_time,
        "after_end_ts_utc": after_scene.acquisition_time,
        
        "before_scene_id": before_id,
        "before_start_ts_utc": before_scene.acquisition_time if before_scene else None,
        "before_end_ts_utc": before_scene.acquisition_time if before_scene else None,
        "before_relative_orbit": before_scene.relative_orbit if before_scene else None,
        
        "delay_h": delay_h,
        "source": "cdse_nationwide_search",
    }


def process_grid_batch(
    grid_id: str,
    events: List[dict],
    trigger_hours: float,
    search_window_hours: float,
) -> List[dict]:
    """
    1つのGridに含まれる全イベントを処理するワーカー関数。
    
    Returns:
        保存すべきペアデータのリスト（条件を満たさない場合は空リスト）
    """
    # 各スレッドでクライアントをインスタンス化（セッション混同防止）
    client = S1CDSEClient()
    
    found_pairs = []
    is_qualified_grid = False

    # 全イベントについて検索を実行
    for ev in events:
        pair = search_event_pair(ev, search_window_hours, client)
        if pair:
            found_pairs.append(pair)
            # 判定条件: 1つでも trigger_hours 以内のデータがあれば合格
            if pair["delay_h"] <= trigger_hours:
                is_qualified_grid = True
    
    # 合格したGridなら、見つかった全ペアを返す
    if is_qualified_grid:
        return found_pairs
    
    # 不合格なら何も返さない
    return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trigger-hours", type=float, default=2.0, help="Gridを採用する判定基準となる経過時間 (default: 2.0h)")
    parser.add_argument("--search-window", type=float, default=24.0, help="検索を行う最大時間幅 (default: 24.0h)")
    parser.add_argument("--workers", type=int, default=8, help="並列実行数 (default: 8)")
    parser.add_argument("--min-rain", type=float, default=4.0, help="対象とする最小最大雨量 (default: 4.0)")
    parser.add_argument("--limit-grids", type=int, default=0, help="処理するGrid数の上限 (テスト用, 0=無制限)")
    
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    
    db_read = SessionLocal()
    db_write = SessionLocal()
    
    try:
        # 1. 全イベントを取得してメモリ上でグルーピング
        logger.info(f"Fetching events (rain >= {args.min_rain} mm/h)...")
        query = db_read.query(models.GsmapEvent).filter(
            models.GsmapEvent.max_gauge_mm_h >= args.min_rain
        )
        all_events = query.all()
        
        # Gridごとに整理
        events_by_grid: Dict[str, List[dict]] = defaultdict(list)
        for ev in all_events:
            # ★修正箇所: DBから取得した日時が naive の場合、UTCとして扱う
            start_utc = ev.start_ts_utc.replace(tzinfo=timezone.utc) if ev.start_ts_utc.tzinfo is None else ev.start_ts_utc
            end_utc = ev.end_ts_utc.replace(tzinfo=timezone.utc) if ev.end_ts_utc.tzinfo is None else ev.end_ts_utc

            events_by_grid[ev.grid_id].append({
                "grid_id": ev.grid_id,
                "lat": ev.lat,
                "lon": ev.lon,
                "start_ts_utc": start_utc,
                "end_ts_utc": end_utc,
                "threshold_mm_h": ev.threshold_mm_h,
                "hit_hours": ev.hit_hours,
                "max_gauge_mm_h": ev.max_gauge_mm_h,
            })
        
        grid_ids = list(events_by_grid.keys())
        if args.limit_grids > 0:
            grid_ids = grid_ids[:args.limit_grids]
            
        logger.info(f"Processing {len(grid_ids)} grids (Total events: {len(all_events)}). Workers: {args.workers}")

        # 2. 並列処理
        processed_grids = 0
        qualified_grids = 0
        total_pairs_found = 0
        results_buffer = []

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_grid = {
                executor.submit(
                    process_grid_batch, 
                    gid, 
                    events_by_grid[gid], 
                    args.trigger_hours, 
                    args.search_window
                ): gid 
                for gid in grid_ids
            }
            
            for future in as_completed(future_to_grid):
                processed_grids += 1
                pairs = future.result()
                
                if pairs:
                    qualified_grids += 1
                    total_pairs_found += len(pairs)
                    results_buffer.extend(pairs)
                    logger.info(f"[QUALIFIED] Grid {pairs[0]['grid_id']}: Found {len(pairs)} pairs")

                if processed_grids % 10 == 0:
                    logger.info(f"Progress: {processed_grids}/{len(grid_ids)} grids. (Qualified: {qualified_grids}, Pairs: {total_pairs_found})")

                # 定期的にDB保存 (メモリ節約)
                if len(results_buffer) >= 100:
                    _bulk_save(db_write, results_buffer)
                    results_buffer = []

        # 残りを保存
        if results_buffer:
            _bulk_save(db_write, results_buffer)

        logger.info(f"Done. Processed Grids: {processed_grids}")
        logger.info(f"Qualified Grids: {qualified_grids}")
        logger.info(f"Total Pairs Inserted: {total_pairs_found}")

    finally:
        db_read.close()
        db_write.close()


def _bulk_save(db: Session, data_list: List[dict]):
    """重複をチェックしながら保存"""
    if not data_list:
        return

    # 1件ずつチェック
    count = 0
    for data in data_list:
        exists = db.query(models.S1Pair).filter(
            models.S1Pair.grid_id == data["grid_id"],
            models.S1Pair.event_start_ts_utc == data["event_start_ts_utc"],
            models.S1Pair.source == data["source"]
        ).first()
        
        if not exists:
            obj = models.S1Pair(**data)
            db.add(obj)
            count += 1
    
    if count > 0:
        try:
            db.commit()
        except Exception as e:
            logger.error(f"DB Commit Error: {e}")
            db.rollback()


if __name__ == "__main__":
    main()