#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

from requests.exceptions import HTTPError  # type: ignore[import-untyped]


def ensure_utc(dt: datetime) -> datetime:
    """Make a datetime timezone-aware in UTC."""
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_mission(platform: Optional[str]) -> Optional[str]:
    """
    Normalize platform string to short mission code for DB (after_mission).

    Examples:
      'sentinel-1a' -> 'S1A'
      'sentinel-1b' -> 'S1B'
      'sentinel-1'  -> 'S1'
    """
    if not platform:
        return None

    p = platform.lower()
    if "sentinel-1a" in p or p.endswith("1a") or p == "s1a":
        return "S1A"
    if "sentinel-1b" in p or p.endswith("1b") or p == "s1b":
        return "S1B"
    if "sentinel-1" in p or p.startswith("sentinel-1"):
        return "S1"
    return platform[:8].upper()


def normalize_pass_direction(direction: Optional[str]) -> Optional[str]:
    """
    Normalize orbit direction string for DB (after_pass_direction / before_pass_direction 相当).

    - 'ascending', 'ASCENDING', 'asc' -> 'ASC'
    - 'descending', 'DESCENDING', 'des' -> 'DSC'
    - otherwise first 3 chars uppercased.
    """
    if not direction:
        return None

    d = direction.lower()
    if d.startswith("asc"):
        return "ASC"
    if d.startswith("des"):
        return "DSC"
    return direction[:3].upper()


def parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()


def build_s1_pairs(
    *,
    threshold_mm_h: float,
    start_date: Optional[date],
    end_date: Optional[date],
    after_hours: float,
    dry_run: bool,
    log_every: int,
) -> None:
    """
    GSMaP イベントテーブル(gsmap_events)から「降雨イベント後の最初の S1 GRD」と
    「その直前の S1 GRD」を CDSE STAC から探して s1_pairs テーブルに書き出す。

    想定スキーマ:
      - GsmapEvent:
          lat, lon, region, start_ts_utc, end_ts_utc,
          hit_hours, max_gauge_mm_h, sum_gauge_mm_h, mean_gauge_mm_h,
          threshold_mm_h, repr_source_file
      - GsmapPoint:
          ts_utc, lat, lon, gauge_mm_h, rain_mm_h, region, grid_id, source_file
          （grid_id は 0.01° グリッド単位で共有されると想定）
      - S1Pair:
          grid_id, lat, lon,
          event_start_ts_utc, event_end_ts_utc,
          threshold_mm_h, hit_hours, max_gauge_mm_h,
          after_*, before_*, delay_h, source
    """

    from app.db.session import SessionLocal  # type: ignore[import-untyped]
    from app.db import models  # type: ignore[import-untyped]
    from app.services.s1_cdse_client import S1CDSEClient  # type: ignore[import-untyped]
    from sqlalchemy.exc import SQLAlchemyError  # type: ignore[import-untyped]

    logger = logging.getLogger(__name__)

    logger.info(
        "build_s1_pairs_from_events_cdse: threshold=%.1f, period=%s..%s, after_hours=%s, dry_run=%s",
        threshold_mm_h,
        start_date.isoformat() if start_date else None,
        end_date.isoformat() if end_date else None,
        after_hours,
        dry_run,
    )

    db = SessionLocal()
    client = S1CDSEClient()

    try:
        # --------------------------------------------------
        # 1) 対象イベント取得
        # --------------------------------------------------
        q = db.query(models.GsmapEvent).filter(
            models.GsmapEvent.threshold_mm_h >= threshold_mm_h
        )

        if start_date is not None:
            start_dt = datetime.combine(start_date, datetime.min.time())
            q = q.filter(models.GsmapEvent.start_ts_utc >= start_dt)

        if end_date is not None:
            end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
            q = q.filter(models.GsmapEvent.start_ts_utc < end_dt)

        events: List[models.GsmapEvent] = (
            q.order_by(models.GsmapEvent.start_ts_utc).all()
        )

        total = len(events)
        logger.info(
            "target events: %d (threshold=%.1f, period=%s..%s)",
            total,
            threshold_mm_h,
            start_date.isoformat() if start_date else None,
            end_date.isoformat() if end_date else None,
        )

        inserted = 0  # コミット済み件数
        skipped_no_after = 0
        skipped_dup = 0
        skipped_no_grid = 0
        stac_after_errors = 0
        stac_before_errors = 0

        commit_every = max(1, log_every)
        since_last_commit = 0  # 前回 commit 以降の未コミット件数

        # --------------------------------------------------
        # STAC ラッパ: AFTER
        # --------------------------------------------------
        def safe_find_after_scene(
            grid_id: str,
            lat: float,
            lon: float,
            event_end_utc: datetime,
            after_hours: float,
        ):
            nonlocal stac_after_errors
            RETRY_STATUSES = (429, 500, 502, 503, 504)

            for attempt in range(3):
                try:
                    return client.find_after_scene(lat, lon, event_end_utc, after_hours)
                except HTTPError as e:
                    status = getattr(e.response, "status_code", None)
                    body = e.response.text[:500] if e.response is not None else ""
                    if status in RETRY_STATUSES:
                        # Retry-After があれば優先
                        retry_after_header = (
                            e.response.headers.get("Retry-After")
                            if e.response
                            else None
                        )
                        if retry_after_header:
                            try:
                                wait = max(int(retry_after_header), 1)
                            except ValueError:
                                wait = 2**attempt
                        else:
                            wait = 2**attempt

                        logger.warning(
                            "HTTP %s when searching AFTER scene (grid=%s, lat=%.3f, lon=%.3f, event_end=%s); "
                            "retry in %ss (attempt %d/3, body=%r)",
                            status,
                            grid_id,
                            lat,
                            lon,
                            event_end_utc.isoformat(),
                            wait,
                            attempt + 1,
                            body,
                        )
                        time.sleep(wait)
                        continue

                    # リトライ対象外: このイベントは諦める
                    logger.error(
                        "Non-retriable HTTP error when searching AFTER scene "
                        "(status=%s, grid=%s, lat=%.3f, lon=%.3f, event_end=%s): %s",
                        status,
                        grid_id,
                        lat,
                        lon,
                        event_end_utc.isoformat(),
                        body,
                    )
                    stac_after_errors += 1
                    return None
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        "Unexpected error when searching AFTER scene (grid=%s): %s",
                        grid_id,
                        e,
                        exc_info=True,
                    )
                    stac_after_errors += 1
                    return None

            # リトライ尽きた
            stac_after_errors += 1
            logger.error(
                "STAC AFTER search failed after retries; skip this event (grid=%s)",
                grid_id,
            )
            return None

        # --------------------------------------------------
        # STAC ラッパ: BEFORE
        # --------------------------------------------------
        def safe_find_before_scene(
            grid_id: str,
            lat: float,
            lon: float,
            ref_time_utc: datetime,
        ):
            nonlocal stac_before_errors
            RETRY_STATUSES = (429, 500, 502, 503, 504)

            for attempt in range(3):
                try:
                    return client.find_before_scene_unbounded(
                        lat,
                        lon,
                        ref_time_utc=ref_time_utc,
                        mission_start_utc=datetime(2014, 1, 1, tzinfo=timezone.utc),
                    )
                except HTTPError as e:
                    status = getattr(e.response, "status_code", None)
                    body = e.response.text[:500] if e.response is not None else ""
                    if status in RETRY_STATUSES:
                        retry_after_header = (
                            e.response.headers.get("Retry-After")
                            if e.response
                            else None
                        )
                        if retry_after_header:
                            try:
                                wait = max(int(retry_after_header), 1)
                            except ValueError:
                                wait = 2**attempt
                        else:
                            wait = 2**attempt

                        logger.warning(
                            "HTTP %s when searching BEFORE scene (grid=%s, lat=%.3f, lon=%.3f, ref_time=%s); "
                            "retry in %ss (attempt %d/3, body=%r)",
                            status,
                            grid_id,
                            lat,
                            lon,
                            ref_time_utc.isoformat(),
                            wait,
                            attempt + 1,
                            body,
                        )
                        time.sleep(wait)
                        continue

                    logger.error(
                        "Non-retriable HTTP error when searching BEFORE scene "
                        "(status=%s, grid=%s, lat=%.3f, lon=%.3f, ref_time=%s): %s",
                        status,
                        grid_id,
                        lat,
                        lon,
                        ref_time_utc.isoformat(),
                        body,
                    )
                    stac_before_errors += 1
                    return None
                except Exception as e:  # noqa: BLE001
                    logger.error(
                        "Unexpected error when searching BEFORE scene (grid=%s): %s",
                        grid_id,
                        e,
                        exc_info=True,
                    )
                    stac_before_errors += 1
                    return None

            stac_before_errors += 1
            logger.error(
                "STAC BEFORE search failed after retries; continue without BEFORE (grid=%s)",
                grid_id,
            )
            return None

        # --------------------------------------------------
        # 2) イベントごとの処理
        # --------------------------------------------------
        for idx, ev in enumerate(events, start=1):
            try:
                lat = ev.lat
                lon = ev.lon

                # --- grid_id を取得する ---
                # 1) 将来 GsmapEvent に grid_id カラムを追加しても動くように getattr を使う
                grid_id: Optional[str] = getattr(ev, "grid_id", None)  # type: ignore[attr-defined]

                # 2) それでも無い場合は GsmapPoint から拾う（小数第2位グリッドマッチ）
                if not grid_id:
                    # イベントの lat/lon を小数第2位に揃える
                    lat2 = round(lat, 2)
                    lon2 = round(lon, 2)

                    lat_min = lat2 - 0.005
                    lat_max = lat2 + 0.005
                    lon_min = lon2 - 0.005
                    lon_max = lon2 + 0.005

                    base_q = db.query(models.GsmapPoint).filter(
                        models.GsmapPoint.lat >= lat_min,
                        models.GsmapPoint.lat < lat_max,
                        models.GsmapPoint.lon >= lon_min,
                        models.GsmapPoint.lon < lon_max,
                    )

                    # まずはイベント期間内の点を優先
                    pt = (
                        base_q.filter(
                            models.GsmapPoint.ts_utc >= ev.start_ts_utc,
                            models.GsmapPoint.ts_utc <= ev.end_ts_utc,
                        )
                        .order_by(models.GsmapPoint.ts_utc.asc())
                        .first()
                    )

                    # イベント期間内に点が無い場合は、その座標グリッドのどれか1点を使う
                    if not pt:
                        pt = base_q.order_by(models.GsmapPoint.ts_utc.asc()).first()

                    if pt and pt.grid_id:
                        grid_id = pt.grid_id
                        logger.debug(
                            (
                                "GsmapEvent id=%s: grid_id を GsmapPoint(id=%s, ts=%s, lat=%.5f, lon=%.5f) "
                                "から取得: %s (event_lat=%.5f, event_lon=%.5f, lat2=%.2f, lon2=%.2f)"
                            ),
                            ev.id,
                            pt.id,
                            pt.ts_utc,
                            pt.lat,
                            pt.lon,
                            grid_id,
                            lat,
                            lon,
                            lat2,
                            lon2,
                        )

                if not grid_id:
                    skipped_no_grid += 1
                    logger.warning(
                        "GsmapEvent id=%s has no grid_id around (lat=%.3f, lon=%.3f); skip",
                        ev.id,
                        lat,
                        lon,
                    )
                    continue

                ev_start_utc = ensure_utc(ev.start_ts_utc)
                ev_end_utc = ensure_utc(ev.end_ts_utc)

                # AFTER シーン検索
                after = safe_find_after_scene(
                    grid_id, lat, lon, ev_end_utc, after_hours
                )
                if after is None:
                    skipped_no_after += 1
                    continue

                # BEFORE シーン検索（なくても OK）
                before = safe_find_before_scene(
                    grid_id, lat, lon, ref_time_utc=after.acquisition_time
                )

                after_safe_or_stac = after.product_identifier or after.stac_id
                before_safe_or_stac = (
                    before.product_identifier if before is not None else None
                )

                after_mission = normalize_mission(after.platform)
                after_dir = normalize_pass_direction(after.orbit_direction)
                before_dir = (
                    normalize_pass_direction(before.orbit_direction)
                    if before
                    else None
                )

                delay_h = (after.acquisition_time - ev_end_utc).total_seconds() / 3600.0

                src_label = (
                    getattr(ev, "repr_source_file", None)
                    or getattr(ev, "src_relpath", None)
                    or getattr(ev, "src_path", None)
                    or getattr(ev, "src", None)
                    or "unknown"
                )

                logger.info(
                    "match: lat=%.2f lon=%.2f grid=%s event=[%s..%s] src=%s "
                    "after_safe_or_stac=%s (mission=%s, platform=%s, rel_orbit=%s, dir=%s, acq=%s) "
                    "before_safe_or_stac=%s (rel_orbit=%s, dir=%s, acq=%s)",
                    lat,
                    lon,
                    grid_id,
                    ev_start_utc.replace(tzinfo=None),
                    ev_end_utc.replace(tzinfo=None),
                    src_label,
                    after_safe_or_stac,
                    after_mission,
                    after.platform,
                    after.relative_orbit,
                    after_dir,
                    after.acquisition_time,
                    before_safe_or_stac,
                    before.relative_orbit if before is not None else None,
                    before_dir,
                    before.acquisition_time if before is not None else None,
                )

                if dry_run:
                    continue

                # 既存重複チェック
                existing = (
                    db.query(models.S1Pair)
                    .filter(
                        models.S1Pair.grid_id == grid_id,
                        models.S1Pair.event_start_ts_utc == ev.start_ts_utc,
                        models.S1Pair.event_end_ts_utc == ev.end_ts_utc,
                        models.S1Pair.threshold_mm_h == ev.threshold_mm_h,
                        models.S1Pair.after_scene_id == after_safe_or_stac,
                        models.S1Pair.source == "cdse",
                    )
                    .first()
                )
                if existing:
                    skipped_dup += 1
                    continue

                pair = models.S1Pair(
                    grid_id=grid_id,
                    lat=lat,
                    lon=lon,
                    event_start_ts_utc=ev.start_ts_utc,
                    event_end_ts_utc=ev.end_ts_utc,
                    threshold_mm_h=ev.threshold_mm_h,
                    hit_hours=ev.hit_hours,
                    max_gauge_mm_h=ev.max_gauge_mm_h,
                    after_scene_id=after_safe_or_stac,
                    after_platform=after.platform,
                    after_mission=after_mission,
                    after_pass_direction=after_dir,
                    after_relative_orbit=after.relative_orbit,
                    after_start_ts_utc=after.acquisition_time,
                    after_end_ts_utc=after.acquisition_time,
                    before_scene_id=before_safe_or_stac,
                    before_start_ts_utc=(
                        before.acquisition_time if before is not None else None
                    ),
                    before_end_ts_utc=(
                        before.acquisition_time if before is not None else None
                    ),
                    before_relative_orbit=(
                        before.relative_orbit if before is not None else None
                    ),
                    delay_h=delay_h,
                    source="cdse",
                )

                db.add(pair)
                since_last_commit += 1

                # 適宜コミット（コミットもエラー耐性あり）
                if not dry_run and since_last_commit >= commit_every:
                    try:
                        db.commit()
                        inserted += since_last_commit
                        since_last_commit = 0
                    except SQLAlchemyError as e:
                        logger.error(
                            "DB commit failed at idx=%d: %s; rollback and continue",
                            idx,
                            e,
                            exc_info=True,
                        )
                        db.rollback()
                        since_last_commit = 0  # このバッチは挿入失敗扱い

                if log_every > 0 and idx % log_every == 0:
                    logger.info(
                        "progress: %d / %d events, committed_inserted=%d, "
                        "pending=%d, skipped_dup=%d, skipped_no_after=%d, "
                        "skipped_no_grid=%d, stac_after_errors=%d, stac_before_errors=%d",
                        idx,
                        total,
                        inserted,
                        since_last_commit,
                        skipped_dup,
                        skipped_no_after,
                        skipped_no_grid,
                        stac_after_errors,
                        stac_before_errors,
                    )

            except SQLAlchemyError as e:
                logger.error(
                    "DB error at GsmapEvent id=%s (index %d): %s; rollback and skip this event",
                    getattr(ev, "id", None),
                    idx,
                    e,
                    exc_info=True,
                )
                db.rollback()
                continue
            except Exception as e:  # noqa: BLE001
                # STAC 側の想定外エラーなども含め、イベント単位で握りつぶして続行
                logger.error(
                    "Unexpected error at GsmapEvent id=%s (index %d): %s; rollback and skip this event",
                    getattr(ev, "id", None),
                    idx,
                    e,
                    exc_info=True,
                )
                db.rollback()
                continue

        # ループ終了後の残りをコミット
        if not dry_run and since_last_commit > 0:
            try:
                db.commit()
                inserted += since_last_commit
                since_last_commit = 0
            except SQLAlchemyError as e:
                logger.error(
                    "Final DB commit failed: %s; rollback",
                    e,
                    exc_info=True,
                )
                db.rollback()
                since_last_commit = 0

        logger.info(
            "done: total=%d, committed_inserted=%d, skipped_dup=%d, skipped_no_after=%d, "
            "skipped_no_grid=%d, stac_after_errors=%d, stac_before_errors=%d",
            total,
            inserted,
            skipped_dup,
            skipped_no_after,
            skipped_no_grid,
            stac_after_errors,
            stac_before_errors,
        )

    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--threshold-mm-h",
        type=float,
        default=10.0,
        help="対象とする降雨イベントの閾値 [mm/h] (default: 10.0)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="対象期間の開始日 (YYYY-MM-DD)。省略時は制限なし。",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="対象期間の終了日 (YYYY-MM-DD)。省略時は制限なし。",
    )
    parser.add_argument(
        "--after-hours",
        type=float,
        default=12.0,
        help="降雨イベント終了から何時間後までを AFTER 検索範囲とするか (default: 12)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB には書き込まず、マッチ結果をログ出力だけする",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=200,
        help="進捗ログを出すイベント数の間隔 (default: 200)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    build_s1_pairs(
        threshold_mm_h=args.threshold_mm_h,
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        after_hours=args.after_hours,
        dry_run=args.dry_run,
        log_every=args.log_every,
    )


if __name__ == "__main__":
    main()
