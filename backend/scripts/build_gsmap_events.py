#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/build_gsmap_events.py

gsmap_points から「連続降雨イベント」を抽出して gsmap_events に集約するスクリプト。

定義イメージ:
- 対象: gauge_mm_h >= threshold_mm_h のレコード
- グループ: 同じ grid_id (≒ 同じ格子点)
- 連続判定: 前のヒットから 1 時間以内なら同じイベント,
            それより離れていれば新しいイベント
- start_ts_utc == end_ts_utc の 1 時間イベントも普通に作る

使い方 (コンテナ内):

  root@backend:/app# python -m scripts.build_gsmap_events \
        --threshold-mm-h 4 \
        --start-date 2018-01-01 \
        --end-date 2018-12-31

  # 既存のイベントを消さずに、件数だけ確認したい場合:
  root@backend:/app# python -m scripts.build_gsmap_events \
        --threshold-mm-h 4 \
        --dry-run
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
    ap = argparse.ArgumentParser(description="gsmap_points から連続降雨イベントを生成して gsmap_events に保存する")

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
        help="gsmap_points をストリーム取得するときのバッチサイズ (ORM の yield_per, デフォルト: 10000)",
    )

    return ap.parse_args()


# ----------------------------------------------------------------------
# イベント 1 件をフラッシュするヘルパ
# ----------------------------------------------------------------------
def _flush_event(
    db: Session,
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
    """現在構築中のイベントを gsmap_events に 1 行追加する。戻り値 = 追加した行数 (0 or 1)。"""
    if hit_hours <= 0:
        return 0

    mean_gauge = sum_gauge / hit_hours if hit_hours > 0 else 0.0

    if dry_run:
        logger.debug(
            "[DRY] grid=%s, start=%s, end=%s, hit_hours=%d, max=%.3f, mean=%.3f",
            grid_id,
            start_ts,
            end_ts,
            hit_hours,
            max_gauge,
            mean_gauge,
        )
        return 0

    ev = models.GsmapEvent(
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
    db.add(ev)
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

    # 期間フィルタを datetime (UTC 想定) に変換
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    if args.start_date:
        # 00:00:00 から
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    if args.end_date:
        # 指定日の 23:59:59 まで (雑に 1 日足して < end でもよい)
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1)

    logger.info(
        "build_gsmap_events: threshold=%.3f, start=%s, end=%s, dry_run=%s",
        args.threshold_mm_h,
        start_dt,
        end_dt,
        args.dry_run,
    )

    with SessionLocal() as db:
        # まず既存のイベントを削除（同じ threshold & 期間のみ）
        if not args.dry_run:
            q_del = db.query(models.GsmapEvent).filter(
                models.GsmapEvent.threshold_mm_h == args.threshold_mm_h
            )
            if start_dt is not None:
                q_del = q_del.filter(models.GsmapEvent.start_ts_utc >= start_dt)
            if end_dt is not None:
                q_del = q_del.filter(models.GsmapEvent.start_ts_utc < end_dt)
            deleted = q_del.delete(synchronize_session=False)
            db.commit()
            logger.info("deleted old events: %d rows", deleted)

        # gsmap_points を grid_id, ts_utc 順にストリーム取得
        query = db.query(
            models.GsmapPoint.grid_id,
            models.GsmapPoint.lat,
            models.GsmapPoint.lon,
            models.GsmapPoint.region,
            models.GsmapPoint.ts_utc,
            models.GsmapPoint.gauge_mm_h,
            models.GsmapPoint.source_file,
        ).filter(
            models.GsmapPoint.gauge_mm_h >= args.threshold_mm_h
        )

        if start_dt is not None:
            query = query.filter(models.GsmapPoint.ts_utc >= start_dt)
        if end_dt is not None:
            query = query.filter(models.GsmapPoint.ts_utc < end_dt)

        query = query.order_by(
            models.GsmapPoint.grid_id,
            models.GsmapPoint.ts_utc,
        )

        # yield_per でストリーム処理
        rows = query.yield_per(args.yield_per)

        logger.info("start building events from gsmap_points ...")

        # 現在構築中のイベント情報
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
        last_log_ts = datetime.now()
        batch_events = 0

        for row in rows:
            grid_id, lat, lon, region, ts_utc, gauge_mm_h, source_file = row

            # grid_id が NULL のものはスキップ（通常はない想定）
            if grid_id is None:
                continue

            if cur_grid_id is None:
                # 最初のレコード
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

            # 前のイベントと同じグリッドか？
            same_grid = (grid_id == cur_grid_id)

            # 時間差 (秒)
            dt_hours = None
            if cur_end_ts is not None:
                dt = ts_utc - cur_end_ts
                dt_hours = dt.total_seconds() / 3600.0

            # 「同じグリッド」かつ「1時間以内」に次のヒットが来たら同じイベントに延長
            if same_grid and dt_hours is not None and dt_hours <= 1.01:
                # イベント継続
                cur_end_ts = ts_utc
                cur_hit_hours += 1
                g = float(gauge_mm_h)
                cur_sum_gauge += g
                if g > cur_max_gauge:
                    cur_max_gauge = g
                    cur_repr_source_file = source_file
            else:
                # ここまでのイベントを flush して、新しいイベントを開始
                if cur_start_ts is not None and cur_end_ts is not None:
                    added = _flush_event(
                        db,
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

                    # バッチ単位でコミット
                    if not args.dry_run and batch_events >= args.batch_size:
                        db.commit()
                        batch_events = 0

                # 新しいイベント開始
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

            # ログをときどき出す
            now = datetime.now()
            if (now - last_log_ts).total_seconds() > 10:
                logger.info(
                    "progress: events=%d (inserted=%d), current grid=%s at %s",
                    total_events,
                    inserted_events,
                    cur_grid_id,
                    cur_end_ts,
                )
                last_log_ts = now

        # ループ終了後、最後のイベントを flush
        if cur_grid_id is not None and cur_start_ts is not None and cur_end_ts is not None:
            added = _flush_event(
                db,
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
            db.commit()

        logger.info(
            "done. total_events=%d, inserted_events=%d (threshold=%.3f, period=%s..%s)",
            total_events,
            inserted_events,
            args.threshold_mm_h,
            start_dt,
            end_dt,
        )


if __name__ == "__main__":
    main()
