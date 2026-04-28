#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/update_gsmap_region_by_roads.py

- japan_road_grids テーブルにマッピングされた道路グリッドリストを使用して、
  gsmap_points の region カラムを一括で更新するスクリプト。
- gsmap_points テーブルは10億レコード超を想定しているため、JOINによるバルクアップデートを行う。
- 道路グリッドに含まれる座標 -> 'Japan'
- 含まれない座標 -> 'Other'
"""

import logging
import time
import argparse
from sqlalchemy import text
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

def update_regions():
    db = SessionLocal()
    
    logger.info("Initializing... Starting the bulk UPDATE via JOIN.")
    
    # 既存のJapanとOtherラベルを最初に全てOtherなどにリセットするか、一発でCASEで書き換えるか。
    # LEFT JOIN が一番正確で1パスで終わる。
    # gsmap_points.grid_id が現状全てNULLなので、浮動小数点の lat, lon で結合する。
    # 誤差吸収のためキャストを使用: CAST(p.lat*100 AS SIGNED)
    
    # === 注意 ===
    # 10億件の場合、クエリが長引くことがあるため、
    # DBの設定(innodb_lock_wait_timeout 等)に注意が必要。
    
    sql = text("""
        UPDATE gsmap_points p
        LEFT JOIN japan_road_grids r 
               ON CAST(p.lat * 100 AS SIGNED) = CAST(r.lat * 100 AS SIGNED)
              AND CAST(p.lon * 100 AS SIGNED) = CAST(r.lon * 100 AS SIGNED)
        SET p.region = CASE 
            WHEN r.grid_id IS NOT NULL THEN 'Japan' 
            ELSE 'Other' 
        END;
    """)
    
    start_time = time.time()
    try:
        logger.info("Executing UPDATE query... (This may take several minutes to hours depending on disk speed)")
        result = db.execute(sql)
        db.commit()
        
        logger.info(f"Update completed successfully!")
        logger.info(f"Rows affected: {result.rowcount}")
        logger.info(f"Time taken: {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error during bulk update: {e}")
        db.rollback()
    finally:
        db.close()

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    update_regions()

if __name__ == "__main__":
    main()
