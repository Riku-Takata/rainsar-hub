#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/build_s1_pairs_from_events_cdse.py

gsmap_events テーブルを走査し、
各降雨イベントについて Copernicus Data Space の STAC API 経由で
Sentinel-1 の after/before シーンを検索し、s1_pairs に保存する。

使い方 (コンテナ内):

  python -m scripts.build_s1_pairs_from_events_cdse \
      --threshold-mm-h 7 \
      --start-date 2018-01-01 \
      --end-date 2018-12-31 \
      --after-hours 72 \
      --dry-run

"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db import models
from app.services.s1_cdse_client import S1CDSEClient

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold-mm-h", type=float, default=7.0)
    ap.add_argument("--start-date", type=str, required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--end-date", type=str, required=True, help="YYYY-MM-DD (UTC, exclusive)")
    ap.add_argument("--after-hours", type=int, default=72, help="イベント終了後に探索する最大時間（h）")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--log-every", type=int, default=1000, help="何イベントごとに進捗ログを出すか")
    return ap.parse_args()


def latlon_to_grid_id(lat: float, lon: float) -> str:
    ns = "N" if lat >= 0 else "S"
    ew = "E" if lon >= 0 else "W"
    return f"{ns}{int(round(abs(lat) * 100)):05d}{ew}{int(round(abs(lon) * 100)):05d}"


def build_s1_pairs(
    db: Session,
    client: S1CDSEClient,
    *,
    threshold_mm_h: float,
    start_dt: datetime,
    end_dt: datetime,
    after_hours: int,
    dry_run: bool,
    log_every: int,
) -> None:
    # 対象イベント抽出
    q = (
        select(models.GsmapEvent)
        .where(
            and_(
                models.GsmapEvent.threshold_mm_h == threshold_mm_h,
                models.GsmapEvent.start_ts_utc >= start_dt,
                models.GsmapEvent.start_ts_utc < end_dt,
            )
        )
        .order_by(models.GsmapEvent.start_ts_utc.asc())
    )

    result = db.execute(q)
    events = list(result.scalars())
    logger.info("target events: %d", len(events))

    inserted = 0
    skipped_no_after = 0

    for idx, ev in enumerate(events, start=1):
        lat = ev.lat
        lon = ev.lon

        # すでに同じイベントで s1_pairs があればスキップ
        dup_q = (
            select(models.S1Pair.id)
            .where(
                and_(
                    models.S1Pair.lat == lat,
                    models.S1Pair.lon == lon,
                    models.S1Pair.event_start_ts_utc == ev.start_ts_utc,
                    models.S1Pair.threshold_mm_h == threshold_mm_h,
                )
            )
            .limit(1)
        )
        if db.execute(dup_q).first():
            continue

        # after
        after = client.find_after_scene(
            lat=lat, lon=lon, event_end=ev.end_ts_utc, max_delay_hours=after_hours
        )
        if after is None:
            skipped_no_after += 1
            continue

        # before（時間制限は client 側で "2014-01-01 〜 after.start" 全域）
        before = client.find_before_same_track(
            lat=lat, lon=lon, after_scene=after
        )

        grid_id = latlon_to_grid_id(lat, lon)
        delay_h = (after.start_time - ev.end_ts_utc).total_seconds() / 3600.0

        pair = models.S1Pair(
            grid_id=grid_id,
            lat=lat,
            lon=lon,
            event_start_ts_utc=ev.start_ts_utc,
            event_end_ts_utc=ev.end_ts_utc,
            threshold_mm_h=ev.threshold_mm_h,
            hit_hours=ev.hit_hours,
            max_gauge_mm_h=ev.max_gauge_mm_h,
            after_scene_id=after.product_id,
            after_platform=after.platform,
            after_mission=after.mission,
            after_pass_direction=after.orbit_direction,
            after_relative_orbit=after.relative_orbit,
            after_start_ts_utc=after.start_time,
            after_end_ts_utc=after.end_time,
            before_scene_id=before.product_id if before else None,
            before_start_ts_utc=before.start_time if before else None,
            before_end_ts_utc=before.end_time if before else None,
            before_relative_orbit=before.relative_orbit if before else None,
            delay_h=delay_h,
            source="cdse",
        )

        if not dry_run:
            db.add(pair)

        inserted += 1

        if idx % log_every == 0:
            logger.info(
                "progress: %d / %d events, inserted=%d, skipped(no after)=%d",
                idx,
                len(events),
                inserted,
                skipped_no_after,
            )

    if not dry_run:
        db.commit()

    logger.info(
        "done. events=%d, inserted=%d, skipped(no after)=%d",
        len(events),
        inserted,
        skipped_no_after,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = parse_args()

    start_dt = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(args.end_date).replace(tzinfo=timezone.utc)

    client = S1CDSEClient()

    with SessionLocal() as db:
        build_s1_pairs(
            db=db,
            client=client,
            threshold_mm_h=args.threshold_mm_h,
            start_dt=start_dt,
            end_dt=end_dt,
            after_hours=args.after_hours,
            dry_run=args.dry_run,
            log_every=args.log_every,
        )


if __name__ == "__main__":
    main()
