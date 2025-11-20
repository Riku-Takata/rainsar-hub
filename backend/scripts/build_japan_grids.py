#!/usr/bin/env python3
import logging
import argparse
from shapely.geometry import Point
from sqlalchemy import distinct
from app.db.session import SessionLocal
from app.db import models
# 既存のスクリプトからポリゴン取得関数をインポート（またはコピー）
# ここでは簡易化のため、build_gsmap_events.py と同様のロジックを使用前提とします
from scripts.build_gsmap_events import get_japan_polygon

logger = logging.getLogger(__name__)

def main():
    logging.basicConfig(level=logging.INFO)
    logger.info("Building Japan Grids table...")

    # 1. 日本ポリゴンの取得
    try:
        jp_geom = get_japan_polygon() # build_gsmap_events.py の関数を利用
    except Exception as e:
        logger.error(f"Failed to load Japan polygon: {e}")
        return

    db = SessionLocal()
    try:
        # 2. gsmap_points からユニークな (grid_id, lat, lon) を取得
        # grid_id が付与されているものだけを対象とする
        logger.info("Fetching distinct grids from gsmap_points...")
        
        # 注: データ量が多い場合、このクエリは重くなる可能性があります。
        # その場合、LIMIT/OFFSETでバッチ処理するか、lat/lon範囲で先に絞り込む必要があります。
        query = db.query(
            models.GsmapPoint.grid_id,
            models.GsmapPoint.lat,
            models.GsmapPoint.lon
        ).filter(
            models.GsmapPoint.grid_id.isnot(None)
        ).distinct()

        grids = query.all()
        logger.info(f"Found {len(grids)} distinct grids. Filtering by Japan mask...")

        new_grids = []
        for row in grids:
            g_id, lat, lon = row
            
            # 3. ポリゴン判定 (Shapelyは x=lon, y=lat)
            if jp_geom.contains(Point(float(lon), float(lat))):
                new_grids.append({
                    "grid_id": g_id,
                    "lat": lat,
                    "lon": lon
                })

        logger.info(f"Filtered {len(new_grids)} grids within Japan.")

        # 4. DBへ保存 (既存があればスキップまたは更新)
        # ここでは全消し＆再作成、あるいは既存チェックを入れるなど運用に合わせて調整
        # 今回は「存在しないものだけ追加」する簡易実装
        count = 0
        for g in new_grids:
            exists = db.query(models.JapanGrid).filter_by(grid_id=g["grid_id"]).first()
            if not exists:
                obj = models.JapanGrid(
                    grid_id=g["grid_id"],
                    lat=g["lat"],
                    lon=g["lon"]
                )
                db.add(obj)
                count += 1
        
        db.commit()
        logger.info(f"Inserted {count} new grids to japan_grids table.")

    except Exception as e:
        logger.error(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    main()