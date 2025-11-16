#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/backfill_gsmap_grid_id.py

gsmap_points テーブルの (lat, lon) ごとに一意な grid_id を割り当てて更新する。

- grid_id は lat/lon を 0.01 度単位で整数化してエンコードした文字列。
  例:
    lat=36.75, lon=137.25
      → lat_i = round(36.75 * 100) = 3675
      → lon_i = round(137.25 * 100) = 13725
      → grid_id = "N03675E13725"
    lat=-12.35, lon=145.05
      → lat_i = -1235 → "S01235"
      → lon_i = 14505 → "E14505"
      → grid_id = "S01235E14505"

- すでに grid_id が入っている行は変更しない（再実行安全）。
"""

from __future__ import annotations

import argparse
import logging
from typing import List, Tuple

from sqlalchemy import text, update
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db import models


logger = logging.getLogger(__name__)


# ------------------------------------------------
# grid_id エンコードロジック
# ------------------------------------------------
def lat_lon_to_grid_id(lat: float, lon: float) -> str:
    """
    (lat, lon) → grid_id への決定的な変換。

    - 0.01 度単位にスケールして四捨五入 → 整数化
    - 符号で N/S, E/W を付与
    - ゼロ埋めして固定桁数の文字列にする
    """
    # 0.01deg 単位で整数化
    lat_i = int(round(lat * 100))
    lon_i = int(round(lon * 100))

    hemi_ns = "N" if lat_i >= 0 else "S"
    hemi_ew = "E" if lon_i >= 0 else "W"

    # 絶対値を 5 桁ゼロ埋め（±90.00, ±180.00 でも十分カバー）
    lat_abs = abs(lat_i)
    lon_abs = abs(lon_i)

    return f"{hemi_ns}{lat_abs:05d}{hemi_ew}{lon_abs:05d}"


# ------------------------------------------------
# メイン処理
# ------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill gsmap_points.grid_id from (lat, lon)."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="何グリッドごとに COMMIT するか（デフォルト: 1000）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB を更新せず、どれくらい対象があるかだけ確認する",
    )
    return parser.parse_args()


def fetch_distinct_lat_lon(session: Session) -> List[Tuple[float, float]]:
    """
    grid_id がまだ NULL の行について、(lat, lon) の組を DISTINCT で取得。
    """
    sql = text(
        """
        SELECT DISTINCT lat, lon
        FROM gsmap_points
        WHERE grid_id IS NULL
        """
    )
    rows = session.execute(sql).fetchall()
    return [(float(r.lat), float(r.lon)) for r in rows]  # type: ignore[attr-defined]


def backfill_grid_ids(batch_size: int = 1000, dry_run: bool = False) -> None:
    with SessionLocal() as session:
        pairs = fetch_distinct_lat_lon(session)
        total_pairs = len(pairs)
        logger.info("distinct (lat, lon) without grid_id: %d", total_pairs)

        if dry_run:
            logger.info("DRY RUN: DB 更新は行いません。")
            # 代表例を一部出す
            for i, (lat, lon) in enumerate(pairs[:10], start=1):
                gid = lat_lon_to_grid_id(lat, lon)
                logger.info("  sample %d: lat=%.2f, lon=%.2f -> grid_id=%s", i, lat, lon, gid)
            return

        if total_pairs == 0:
            logger.info("grid_id が NULL の行はありません。何もせず終了します。")
            return

        processed = 0

        for lat, lon in pairs:
            gid = lat_lon_to_grid_id(lat, lon)

            stmt = (
                update(models.GsmapPoint)
                .where(
                    models.GsmapPoint.grid_id.is_(None),
                    models.GsmapPoint.lat == lat,
                    models.GsmapPoint.lon == lon,
                )
                .values(grid_id=gid)
            )
            result = session.execute(stmt)
            processed += 1

            if processed % batch_size == 0:
                session.commit()
                logger.info(
                    "committed %d / %d grids (last gid=%s, rows=%d)",
                    processed,
                    total_pairs,
                    gid,
                    result.rowcount,
                )

        # 残りをコミット
        session.commit()
        logger.info("DONE: processed %d grids (distinct lat/lon).", processed)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = parse_args()
    backfill_grid_ids(batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
