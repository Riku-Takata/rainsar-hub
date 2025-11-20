#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/download_s1_events_by_grid.py

指定した grid_id の gsmap_points データから「連続降雨イベント」を動的に抽出し、
そのイベントに対応する Sentinel-1 画像ペア（After / Before）を検索・保存・ダウンロードする。
"""

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

from app.db.session import SessionLocal
from app.db import models
from app.services.s1_cdse_client import S1CDSEClient
from app.core.config import settings

logger = logging.getLogger(__name__)

# --- Helper Functions ---
def normalize_mission(platform: Optional[str]) -> Optional[str]:
    if not platform: return None
    p = platform.lower()
    if "sentinel-1a" in p or p.endswith("1a") or p == "s1a": return "S1A"
    if "sentinel-1b" in p or p.endswith("1b") or p == "s1b": return "S1B"
    if "sentinel-1" in p: return "S1"
    return platform[:8].upper()

def normalize_pass_direction(direction: Optional[str]) -> Optional[str]:
    if not direction: return None
    d = direction.lower()
    if d.startswith("asc"): return "ASC"
    if d.startswith("des"): return "DSC"
    return direction[:3].upper()

def get_scene_id(scene) -> str:
    """ product_identifier があればそれを、なければ stac_id を返す """
    if scene.product_identifier:
        return scene.product_identifier
    return scene.stac_id
# ------------------------

def main():
    parser = argparse.ArgumentParser(description="Download S1 pairs and save to DB for rain events.")
    parser.add_argument("grid_id", type=str, help="Target Grid ID")
    parser.add_argument("--threshold", type=float, default=10.0, help="Rain threshold mm/h (default: 10.0)")
    parser.add_argument("--output-dir", type=str, default=None, help="Download directory")
    parser.add_argument("--after-hours", type=float, default=24.0, help="Search window for After image (hours)")
    parser.add_argument("--dry-run", action="store_true", help="Search & DB check only")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.output_dir:
        save_dir = Path(args.output_dir)
    else:
        save_dir = settings.s1_safe_path
    
    if not args.dry_run:
        save_dir.mkdir(parents=True, exist_ok=True)

    db = SessionLocal()
    client = S1CDSEClient()
    
    download_count = 0
    inserted_count = 0

    try:
        logger.info(f"Fetching points for {args.grid_id} (threshold >= {args.threshold} mm/h)...")
        points = db.query(models.GsmapPoint).filter(
            models.GsmapPoint.grid_id == args.grid_id,
            models.GsmapPoint.gauge_mm_h >= args.threshold
        ).order_by(models.GsmapPoint.ts_utc.asc()).all()

        if not points:
            logger.warning(f"No rain points found for grid {args.grid_id}")
            return
        
        lat, lon = points[0].lat, points[0].lon
        logger.info(f"Grid Location: Lat={lat}, Lon={lon}")

        # イベント集約
        events: List[Dict[str, Any]] = []
        if points:
            current_evt = {
                "start": points[0].ts_utc,
                "end": points[0].ts_utc,
                "max_rain": points[0].gauge_mm_h,
                "count": 1
            }
            for p in points[1:]:
                if (p.ts_utc - current_evt["end"]) <= timedelta(hours=1.1):
                    current_evt["end"] = p.ts_utc
                    current_evt["max_rain"] = max(current_evt["max_rain"], p.gauge_mm_h)
                    current_evt["count"] += 1
                else:
                    events.append(current_evt)
                    current_evt = {"start": p.ts_utc, "end": p.ts_utc, "max_rain": p.gauge_mm_h, "count": 1}
            events.append(current_evt)

        logger.info(f"Extracted {len(events)} rain events.")
        
        for i, ev in enumerate(reversed(events), start=1):
            start_ts = ev["start"].replace(tzinfo=timezone.utc)
            end_ts = ev["end"].replace(tzinfo=timezone.utc)
            max_r = ev["max_rain"]
            hit_hours = ev["count"]
            
            logger.info(f"--- Event {i}/{len(events)} ---")
            logger.info(f"  Rain Period: {start_ts} ~ {end_ts} (Duration: {hit_hours}h, Max: {max_r:.1f}mm/h)")
            
            after_scene = client.find_after_scene(lat=lat, lon=lon, event_end_utc=end_ts, after_hours=args.after_hours)
            if not after_scene:
                logger.info("  [Skip] No 'After' scene found within range.")
                continue
            
            before_scene = client.find_before_scene_unbounded(lat=lat, lon=lon, ref_time_utc=after_scene.acquisition_time)
            if not before_scene:
                logger.info("  [Skip] 'After' scene found, but no 'Before' scene found.")
                continue

            # ID決定（ここを修正: product_identifierがない場合はstac_idを使う）
            after_id = get_scene_id(after_scene)
            before_id = get_scene_id(before_scene)

            delay_h = (after_scene.acquisition_time - end_ts).total_seconds() / 3600.0
            
            logger.info(f"  [Pair Found]")
            logger.info(f"    Rain End  : {end_ts}")
            logger.info(f"    Satellite : {after_scene.acquisition_time}")
            logger.info(f"    Delay     : {delay_h:.2f} hours")
            logger.info(f"    After ID  : {after_id}")
            logger.info(f"    Before ID : {before_id}")

            if args.dry_run:
                continue

            # DB 保存
            existing = db.query(models.S1Pair).filter(
                models.S1Pair.grid_id == args.grid_id,
                models.S1Pair.event_start_ts_utc == start_ts,
                models.S1Pair.after_scene_id == after_id
            ).first()

            if existing:
                logger.info("  [DB] Record already exists, skipping insert.")
            else:
                new_pair = models.S1Pair(
                    grid_id=args.grid_id,
                    lat=lat,
                    lon=lon,
                    event_start_ts_utc=start_ts,
                    event_end_ts_utc=end_ts,
                    threshold_mm_h=args.threshold,
                    hit_hours=hit_hours,
                    max_gauge_mm_h=max_r,
                    
                    after_scene_id=after_id,
                    after_platform=after_scene.platform,
                    after_mission=normalize_mission(after_scene.platform),
                    after_pass_direction=normalize_pass_direction(after_scene.orbit_direction),
                    after_relative_orbit=after_scene.relative_orbit,
                    after_start_ts_utc=after_scene.acquisition_time,
                    after_end_ts_utc=after_scene.acquisition_time,
                    
                    before_scene_id=before_id,
                    before_start_ts_utc=before_scene.acquisition_time,
                    before_end_ts_utc=before_scene.acquisition_time,
                    before_relative_orbit=before_scene.relative_orbit,
                    
                    delay_h=delay_h,
                    source="cdse_dynamic"
                )
                db.add(new_pair)
                db.commit()
                inserted_count += 1
                logger.info("  [DB] Inserted new record to s1_pairs.")

            # ダウンロード実行 (IDとして after_id/before_id を渡す)
            logger.info(f"  [Download] Saving to {save_dir} ...")
            
            path_b = client.download_product(before_id, save_dir)
            if path_b: logger.info(f"    -> Downloaded Before: {path_b.name}")
            
            path_a = client.download_product(after_id, save_dir)
            if path_a: 
                logger.info(f"    -> Downloaded After : {path_a.name}")
                download_count += 1

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()
        logger.info(f"Done. DB Inserted: {inserted_count}, Downloaded Pairs: {download_count}")

if __name__ == "__main__":
    main()