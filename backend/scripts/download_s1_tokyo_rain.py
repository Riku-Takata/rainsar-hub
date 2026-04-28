#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/download_s1_tokyo_rain.py

東京エリア (TOKYO_BBOX) で 10mm/h 以上の降雨が発生した日時を DB (gsmap_points) から取得し、
降雨終了後 0〜3 時間以内に撮影された Sentinel-1 GRD 画像を CDSE から検索・ダウンロードする。
見つかったペア情報は s1_pairs テーブルにも登録する。

使い方:
  cd D:\\sotsuron\\rainsar-hub\\backend
  .venv\\Scripts\\python.exe scripts/download_s1_tokyo_rain.py [--dry-run]
"""

import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict

import requests
from dotenv import load_dotenv

from app.db.session import SessionLocal
from app.db import models
from app.services.s1_cdse_client import S1CDSEClient
from app.core.config import settings

logger = logging.getLogger(__name__)

# ── 東京バウンディングボックス (preprocess_s1_tokyo_batch.py と同一) ──
TOKYO_BBOX = {
    "min_lat": 35.50,
    "max_lat": 35.90,
    "min_lon": 139.40,
    "max_lon": 140.00,
}


# ------------------------------------------------------------------ #
#  Password Grant パッチ (download_s1_by_area.py と同じ認証方式)
# ------------------------------------------------------------------ #
def patch_s1cdse_client_for_password_grant():
    """
    S1CDSEClient の _get_token を CDSE の username/password 方式にすり替える。
    """
    original_get_token = S1CDSEClient._get_token

    def custom_get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._access_token and now < self._token_expire_at:
            return self._access_token

        env_path = Path(__file__).parent.parent / ".env"
        load_dotenv(env_path)

        username = os.getenv("CDSE_USERNAME")
        password = os.getenv("CDSE_PASSWORD")

        if not username or not password:
            logger.warning(
                "No CDSE_USERNAME or CDSE_PASSWORD found in .env. "
                "Falling back to default client_credentials."
            )
            return original_get_token(self)

        data = {
            "grant_type": "password",
            "client_id": "cdse-public",
            "username": username,
            "password": password,
        }

        try:
            resp = self._session.post(self._token_url, data=data, timeout=30)
            resp.raise_for_status()
            j = resp.json()
            access_token = j["access_token"]
            expires_in = int(j.get("expires_in", 3600))
            self._access_token = access_token
            self._token_expire_at = now + timedelta(seconds=expires_in - 60)
            return access_token
        except Exception as e:
            logger.error(f"Failed to get CDSE token via Password Grant: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return ""

    S1CDSEClient._get_token = custom_get_token


# ------------------------------------------------------------------ #
#  ヘルパー関数
# ------------------------------------------------------------------ #
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


def get_scene_id(scene) -> str:
    """product_identifier があればそれを、なければ stac_id を返す"""
    return scene.product_identifier or scene.stac_id


# ------------------------------------------------------------------ #
#  DB クエリ: 東京 bbox 内の降雨ポイント取得
# ------------------------------------------------------------------ #
def fetch_rain_points(db, threshold: float) -> list:
    """
    gsmap_points から東京 bbox 内 ＆ gauge_mm_h >= threshold のレコードを取得。
    ts_utc 昇順で返す。
    """
    points = (
        db.query(models.GsmapPoint)
        .filter(
            models.GsmapPoint.lat >= TOKYO_BBOX["min_lat"],
            models.GsmapPoint.lat <= TOKYO_BBOX["max_lat"],
            models.GsmapPoint.lon >= TOKYO_BBOX["min_lon"],
            models.GsmapPoint.lon <= TOKYO_BBOX["max_lon"],
            models.GsmapPoint.gauge_mm_h >= threshold,
        )
        .order_by(models.GsmapPoint.ts_utc.asc())
        .all()
    )
    return points


# ------------------------------------------------------------------ #
#  連続降雨イベント集約
#  - 東京全体を1エリアとして扱い、時間軸で集約する
#  - 同一時刻に複数グリッドで雨が降っている場合はまとめる
# ------------------------------------------------------------------ #
def aggregate_rain_events(
    points: list, gap_hours: float = 1.5
) -> List[Dict[str, Any]]:
    """
    同じ時刻帯（gap_hours 以内の連続）を1イベントにまとめる。
    異なるグリッドポイントは同一時刻ならまとめられる。
    """
    if not points:
        return []

    # まず時刻ごとに集約
    ts_data: Dict[datetime, Dict[str, Any]] = {}
    for p in points:
        ts = p.ts_utc
        if ts not in ts_data:
            ts_data[ts] = {
                "max_rain": p.gauge_mm_h,
                "count": 1,
                "grids": {(p.lat, p.lon)},
            }
        else:
            ts_data[ts]["max_rain"] = max(ts_data[ts]["max_rain"], p.gauge_mm_h)
            ts_data[ts]["count"] += 1
            ts_data[ts]["grids"].add((p.lat, p.lon))

    sorted_ts = sorted(ts_data.keys())

    events: List[Dict[str, Any]] = []
    current = {
        "start": sorted_ts[0],
        "end": sorted_ts[0],
        "max_rain": ts_data[sorted_ts[0]]["max_rain"],
        "total_records": ts_data[sorted_ts[0]]["count"],
        "grids": set(ts_data[sorted_ts[0]]["grids"]),
    }

    for ts in sorted_ts[1:]:
        if (ts - current["end"]) <= timedelta(hours=gap_hours):
            current["end"] = ts
            current["max_rain"] = max(current["max_rain"], ts_data[ts]["max_rain"])
            current["total_records"] += ts_data[ts]["count"]
            current["grids"] |= ts_data[ts]["grids"]
        else:
            events.append(current)
            current = {
                "start": ts,
                "end": ts,
                "max_rain": ts_data[ts]["max_rain"],
                "total_records": ts_data[ts]["count"],
                "grids": set(ts_data[ts]["grids"]),
            }
    events.append(current)

    # hit_hours 計算 (ユニークなタイムスタンプ数)
    for ev in events:
        # イベント期間内のユニークなタイムスタンプ数
        ev["hit_hours"] = sum(
            1 for ts in sorted_ts if ev["start"] <= ts <= ev["end"]
        )
        ev["grid_count"] = len(ev["grids"])

    return events


# ------------------------------------------------------------------ #
#  メイン処理
# ------------------------------------------------------------------ #
def main():
    patch_s1cdse_client_for_password_grant()

    parser = argparse.ArgumentParser(
        description="Download S1 GRD images for Tokyo rain events"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        help="Rain threshold mm/h (default: 10.0)",
    )
    parser.add_argument(
        "--after-hours",
        type=float,
        default=3.0,
        help="Search window AFTER rain ends, in hours (default: 3.0)",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=r"E:\s1_tokyo_2025",
        help="Output directory for downloaded ZIPs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Search & display only — no download, no DB insert",
    )
    parser.add_argument(
        "--auto-orbit",
        action="store_true",
        default=True,
        help="Automatically select dominant orbit direction",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    out_dir = Path(args.out_dir)
    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    db = SessionLocal()
    client = S1CDSEClient()

    download_count = 0
    inserted_count = 0
    skipped_count = 0

    try:
        # ── Step 1: DB から降雨ポイント取得 ──
        logger.info("=" * 60)
        logger.info("Step 1: Fetching rain points from DB...")
        logger.info(
            f"  Tokyo BBOX: lat=[{TOKYO_BBOX['min_lat']}, {TOKYO_BBOX['max_lat']}], "
            f"lon=[{TOKYO_BBOX['min_lon']}, {TOKYO_BBOX['max_lon']}]"
        )
        logger.info(f"  Threshold: >= {args.threshold} mm/h")

        points = fetch_rain_points(db, args.threshold)
        if not points:
            logger.warning("No rain points found for Tokyo area. Exiting.")
            return
        logger.info(f"  Found {len(points)} rain records")

        # ── Step 2: 連続降雨イベント集約 ──
        logger.info("=" * 60)
        logger.info("Step 2: Aggregating into continuous rain events...")
        events = aggregate_rain_events(points)
        logger.info(f"  Extracted {len(events)} rain events")

        for i, ev in enumerate(events, 1):
            logger.info(
                f"  Event {i:3d}: {ev['start']} ~ {ev['end']} | "
                f"Max: {ev['max_rain']:.1f} mm/h | "
                f"Records: {ev['total_records']} | "
                f"Grids: {ev['grid_count']}"
            )

        # ── Step 3: 各イベントについて S1 画像を検索 ──
        logger.info("=" * 60)
        logger.info("Step 3: Searching S1 GRD images for each event...")
        logger.info(f"  After-hours window: 0 ~ {args.after_hours} h")

        for i, ev in enumerate(events, 1):
            start_ts = ev["start"]
            end_ts = ev["end"]
            if start_ts.tzinfo is None:
                start_ts = start_ts.replace(tzinfo=timezone.utc)
            if end_ts.tzinfo is None:
                end_ts = end_ts.replace(tzinfo=timezone.utc)

            search_start = end_ts
            search_end = end_ts + timedelta(hours=args.after_hours)

            logger.info(f"\n--- Event {i}/{len(events)} ---")
            logger.info(
                f"  Rain: {start_ts} ~ {end_ts} "
                f"(Max {ev['max_rain']:.1f} mm/h, {ev['grid_count']} grids)"
            )
            logger.info(f"  S1 search window: {search_start} ~ {search_end}")

            # bbox 検索
            scenes = client.search_grd_bbox_time(
                min_lat=TOKYO_BBOX["min_lat"],
                max_lat=TOKYO_BBOX["max_lat"],
                min_lon=TOKYO_BBOX["min_lon"],
                max_lon=TOKYO_BBOX["max_lon"],
                start=search_start,
                end=search_end,
                limit=50,
            )

            if not scenes:
                logger.info("  [Skip] No S1 scene found in time window.")
                skipped_count += 1
                continue

            # 重複排除 (同じ product_identifier)
            seen_ids = set()
            unique_scenes = []
            for s in scenes:
                sid = get_scene_id(s)
                if sid not in seen_ids:
                    seen_ids.add(sid)
                    unique_scenes.append(s)

            logger.info(f"  Found {len(unique_scenes)} S1 scene(s)")

            for s in unique_scenes:
                sid = get_scene_id(s)
                delay_h = (
                    (s.acquisition_time - end_ts).total_seconds() / 3600.0
                )
                logger.info(
                    f"    - {sid} | "
                    f"Acquired: {s.acquisition_time} | "
                    f"Delay: {delay_h:.2f}h | "
                    f"Orbit: {s.orbit_direction} | "
                    f"Platform: {s.platform}"
                )

                if args.dry_run:
                    continue

                # ── DB 登録 ──
                # グリッド代表を東京 bbox 中心に設定
                repr_lat = (TOKYO_BBOX["min_lat"] + TOKYO_BBOX["max_lat"]) / 2
                repr_lon = (TOKYO_BBOX["min_lon"] + TOKYO_BBOX["max_lon"]) / 2
                grid_id = f"N{int(repr_lat * 100):05d}E{int(repr_lon * 100):05d}"

                existing = (
                    db.query(models.S1Pair)
                    .filter(
                        models.S1Pair.after_scene_id == sid,
                        models.S1Pair.event_start_ts_utc == start_ts,
                    )
                    .first()
                )

                if existing:
                    logger.info("    [DB] Record already exists, skip insert.")
                else:
                    hit_hours = ev.get("hit_hours", 1)

                    new_pair = models.S1Pair(
                        grid_id=grid_id,
                        lat=repr_lat,
                        lon=repr_lon,
                        event_start_ts_utc=start_ts,
                        event_end_ts_utc=end_ts,
                        threshold_mm_h=args.threshold,
                        hit_hours=hit_hours,
                        max_gauge_mm_h=ev["max_rain"],
                        after_scene_id=sid,
                        after_platform=s.platform,
                        after_mission=normalize_mission(s.platform),
                        after_pass_direction=normalize_pass_direction(
                            s.orbit_direction
                        ),
                        after_relative_orbit=s.relative_orbit,
                        after_start_ts_utc=s.acquisition_time,
                        after_end_ts_utc=s.acquisition_time,
                        # before は後で別途検索可能（ここでは None）
                        before_scene_id=None,
                        before_start_ts_utc=None,
                        before_end_ts_utc=None,
                        before_relative_orbit=None,
                        delay_h=delay_h,
                        source="cdse_tokyo_rain",
                    )
                    db.add(new_pair)
                    db.commit()
                    inserted_count += 1
                    logger.info("    [DB] Inserted new s1_pairs record.")

                # ── ダウンロード ──
                logger.info(f"    [Download] Saving to {out_dir} ...")
                saved = client.download_product(sid, out_dir)
                if saved:
                    download_count += 1
                    logger.info(f"    -> Downloaded: {saved.name}")
                else:
                    logger.error(f"    -> Download FAILED for {sid}")

            # API レート制限への配慮
            time.sleep(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()
        logger.info("\n" + "=" * 60)
        logger.info("=== Summary ===")
        logger.info(f"  Total events   : {len(events) if 'events' in dir() else 0}")
        logger.info(f"  DB Inserted    : {inserted_count}")
        logger.info(f"  Downloaded     : {download_count}")
        logger.info(f"  Skipped (no S1): {skipped_count}")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
