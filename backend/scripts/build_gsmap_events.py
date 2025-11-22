#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/build_gsmap_events.py

gsmap_points から「連続降雨イベント」を抽出して gsmap_events に集約するスクリプト。
フィルタリングに japan_grids テーブルを使用することで、Web表示と整合性を取る。
【修正点】読み込みと書き込みのセッションを分離し、commitによるカーソル切断を回避。
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db import models

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# CLI 引数
# ----------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="gsmap_points から連続降雨イベントを生成して gsmap_events に保存する"
    )

    ap.add_argument(
        "--threshold-mm-h",
        type=float,
        default=4.0,
        help="この雨量(mm/h)以上をヒットとしてイベント化する (デフォルト: 4.0)",
    )
    ap.add_argument(
        "--start-date",
        type=str,
        help="対象期間の開始日 (UTC, YYYY-MM-DD)。指定しない場合は全期間。",
    )
    ap.add_argument(
        "--end-date",
        type=str,
        help="対象期間の終了日 (UTC, YYYY-MM-DD, 当日を含む)。指定しない場合は全期間。",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="DB に書き込まず、イベント件数だけをログ出力する",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="イベント insert のコミット単位 (デフォルト: 2000)",
    )
    ap.add_argument(
        "--yield-per",
        type=int,
        default=10000,
        help="gsmap_points をストリーム取得するときのバッチサイズ",
    )

    # bbox フィルタ
    ap.add_argument("--min-lat", type=float, default=None)
    ap.add_argument("--max-lat", type=float, default=None)
    ap.add_argument("--min-lon", type=float, default=None)
    ap.add_argument("--max-lon", type=float, default=None)

    # 日本フィルタ
    ap.add_argument(
        "--japan-mask",
        action="store_true",
        help="japan_grids テーブルに含まれる grid_id のみを対象にする",
    )

    return ap.parse_args()


# ----------------------------------------------------------------------
# イベント 1 件をフラッシュするヘルパ
# ----------------------------------------------------------------------
def _flush_event(
    db_write: Session,  # 書き込み用セッションを受け取る
    *,
    grid_id: str,
    lat: float,
    lon: float,
    region: Optional[str],
    start_ts: datetime,
    end_ts: datetime,
    hit_hours: int,
    sum_gauge: float,
    max_gauge: float,
    threshold: float,
    repr_source_file: Optional[str],
    dry_run: bool,
) -> int:
    """現在構築中のイベントを gsmap_events に 1 行追加する。"""
    if hit_hours <= 0:
        return 0

    mean_gauge = sum_gauge / hit_hours if hit_hours > 0 else 0.0

    if dry_run:
        logger.debug(
            "[DRY] grid=%s, start=%s, end=%s, hit_hours=%d, max=%.3f",
            grid_id, start_ts, end_ts, hit_hours, max_gauge
        )
        return 0

    ev = models.GsmapEvent(
        grid_id=grid_id,
        lat=lat,
        lon=lon,
        region=region,
        start_ts_utc=start_ts,
        end_ts_utc=end_ts,
        hit_hours=hit_hours,
        max_gauge_mm_h=max_gauge,
        sum_gauge_mm_h=sum_gauge,
        mean_gauge_mm_h=mean_gauge,
        threshold_mm_h=threshold,
        repr_source_file=repr_source_file,
    )
    db_write.add(ev)
    return 1


# ----------------------------------------------------------------------
# メイン処理
# ----------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = parse_args()

    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    if args.start_date:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    if args.end_date:
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1)

    logger.info(
        "build_gsmap_events: threshold=%.3f, start=%s, end=%s, dry_run=%s, japan_mask=%s",
        args.threshold_mm_h, start_dt, end_dt, args.dry_run, args.japan_mask,
    )

    # ★修正点: 読み込み用と書き込み用のセッションを分ける
    db_read = SessionLocal()
    db_write = SessionLocal()

    try:
        # --------------------------------------------------------------
        # 1) 既存のイベントを削除 (書き込み用セッションを使用)
        # --------------------------------------------------------------
        if not args.dry_run:
            q_del = db_write.query(models.GsmapEvent).filter(
                models.GsmapEvent.threshold_mm_h == args.threshold_mm_h
            )
            if start_dt:
                q_del = q_del.filter(models.GsmapEvent.start_ts_utc >= start_dt)
            if end_dt:
                q_del = q_del.filter(models.GsmapEvent.start_ts_utc < end_dt)
            
            if args.min_lat: q_del = q_del.filter(models.GsmapEvent.lat >= args.min_lat)
            if args.max_lat: q_del = q_del.filter(models.GsmapEvent.lat <= args.max_lat)
            if args.min_lon: q_del = q_del.filter(models.GsmapEvent.lon >= args.min_lon)
            if args.max_lon: q_del = q_del.filter(models.GsmapEvent.lon <= args.max_lon)

            deleted = q_del.delete(synchronize_session=False)
            db_write.commit()
            logger.info("deleted old events: %d rows", deleted)

        # --------------------------------------------------------------
        # 2) gsmap_points をストリーム取得 (読み込み用セッションを使用)
        # --------------------------------------------------------------
        query = db_read.query(
            models.GsmapPoint.grid_id,
            models.GsmapPoint.lat,
            models.GsmapPoint.lon,
            models.GsmapPoint.region,
            models.GsmapPoint.ts_utc,
            models.GsmapPoint.gauge_mm_h,
            models.GsmapPoint.source_file,
        )

        if args.japan_mask:
            query = query.join(
                models.JapanGrid,
                models.GsmapPoint.grid_id == models.JapanGrid.grid_id
            )

        query = query.filter(
            models.GsmapPoint.gauge_mm_h >= args.threshold_mm_h
        )

        if start_dt: query = query.filter(models.GsmapPoint.ts_utc >= start_dt)
        if end_dt: query = query.filter(models.GsmapPoint.ts_utc < end_dt)
        if args.min_lat: query = query.filter(models.GsmapPoint.lat >= args.min_lat)
        if args.max_lat: query = query.filter(models.GsmapPoint.lat <= args.max_lat)
        if args.min_lon: query = query.filter(models.GsmapPoint.lon >= args.min_lon)
        if args.max_lon: query = query.filter(models.GsmapPoint.lon <= args.max_lon)

        query = query.order_by(
            models.GsmapPoint.grid_id,
            models.GsmapPoint.ts_utc,
        )

        rows = query.yield_per(args.yield_per)
        logger.info("start building events from gsmap_points (using DB JOIN & 2 sessions)...")

        cur_grid_id: Optional[str] = None
        cur_lat: float = 0.0
        cur_lon: float = 0.0
        cur_region: Optional[str] = None
        cur_start_ts: Optional[datetime] = None
        cur_end_ts: Optional[datetime] = None
        cur_hit_hours: int = 0
        cur_sum_gauge: float = 0.0
        cur_max_gauge: float = 0.0
        cur_repr_source_file: Optional[str] = None

        total_events = 0
        inserted_events = 0
        batch_events = 0
        last_log_ts = datetime.now()

        for row in rows:
            grid_id, lat, lon, region, ts_utc, gauge_mm_h, source_file = row

            if grid_id is None:
                continue

            if cur_grid_id is None:
                cur_grid_id = grid_id
                cur_lat = float(lat)
                cur_lon = float(lon)
                cur_region = region
                cur_start_ts = ts_utc
                cur_end_ts = ts_utc
                cur_hit_hours = 1
                cur_sum_gauge = float(gauge_mm_h)
                cur_max_gauge = float(gauge_mm_h)
                cur_repr_source_file = source_file
                continue

            same_grid = (grid_id == cur_grid_id)
            dt_hours = None
            if cur_end_ts is not None:
                dt = ts_utc - cur_end_ts
                dt_hours = dt.total_seconds() / 3600.0

            if same_grid and dt_hours is not None and dt_hours <= 1.01:
                cur_end_ts = ts_utc
                cur_hit_hours += 1
                g = float(gauge_mm_h)
                cur_sum_gauge += g
                if g > cur_max_gauge:
                    cur_max_gauge = g
                    cur_repr_source_file = source_file
            else:
                if cur_start_ts is not None and cur_end_ts is not None:
                    added = _flush_event(
                        db_write,  # 書き込みセッション
                        grid_id=cur_grid_id,
                        lat=cur_lat,
                        lon=cur_lon,
                        region=cur_region,
                        start_ts=cur_start_ts,
                        end_ts=cur_end_ts,
                        hit_hours=cur_hit_hours,
                        sum_gauge=cur_sum_gauge,
                        max_gauge=cur_max_gauge,
                        threshold=args.threshold_mm_h,
                        repr_source_file=cur_repr_source_file,
                        dry_run=args.dry_run,
                    )
                    total_events += 1
                    inserted_events += added
                    batch_events += added

                    if not args.dry_run and batch_events >= args.batch_size:
                        db_write.commit()  # 書き込み側だけコミット（読み込み側には影響しない）
                        batch_events = 0

                cur_grid_id = grid_id
                cur_lat = float(lat)
                cur_lon = float(lon)
                cur_region = region
                cur_start_ts = ts_utc
                cur_end_ts = ts_utc
                cur_hit_hours = 1
                g = float(gauge_mm_h)
                cur_sum_gauge = g
                cur_max_gauge = g
                cur_repr_source_file = source_file

            now = datetime.now()
            if (now - last_log_ts).total_seconds() > 10:
                logger.info(
                    "progress: events=%d (inserted=%d), current grid=%s",
                    total_events, inserted_events, cur_grid_id
                )
                last_log_ts = now

        # ループ終了後の残り
        if cur_grid_id is not None and cur_start_ts is not None and cur_end_ts is not None:
            added = _flush_event(
                db_write,
                grid_id=cur_grid_id,
                lat=cur_lat,
                lon=cur_lon,
                region=cur_region,
                start_ts=cur_start_ts,
                end_ts=cur_end_ts,
                hit_hours=cur_hit_hours,
                sum_gauge=cur_sum_gauge,
                max_gauge=cur_max_gauge,
                threshold=args.threshold_mm_h,
                repr_source_file=cur_repr_source_file,
                dry_run=args.dry_run,
            )
            total_events += 1
            inserted_events += added
            batch_events += added

        if not args.dry_run and batch_events > 0:
            db_write.commit()

        logger.info(
            "done. total_events=%d, inserted_events=%d",
            total_events, inserted_events,
        )

    finally:
        # セッションを閉じる
        db_read.close()
        db_write.close()


if __name__ == "__main__":
    main()
