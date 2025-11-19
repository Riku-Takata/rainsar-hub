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

  # 全期間・全世界（インポート済み範囲）からイベント作成
  root@backend:/app# python -m scripts.build_gsmap_events \\
        --threshold-mm-h 4

  # 2018 年だけ対象
  root@backend:/app# python -m scripts.build_gsmap_events \\
        --threshold-mm-h 4 \\
        --start-date 2018-01-01 \\
        --end-date 2018-12-31

  # 既存のイベントを消さずに、件数だけ確認したい場合:
  root@backend:/app# python -m scripts.build_gsmap_events \\
        --threshold-mm-h 4 \\
        --dry-run

  # 日本付近の bbox だけ対象にする場合(例):
  root@backend:/app# python -m scripts.build_gsmap_events \\
        --threshold-mm-h 4 \\
        --start-date 2018-01-01 \\
        --end-date 2018-12-31 \\
        --min-lat 24 --max-lat 46 \\
        --min-lon 123 --max-lon 146

  # さらに「日本国土ポリゴン」でマスクする場合:
  root@backend:/app# python -m scripts.build_gsmap_events \\
        --threshold-mm-h 10 \\
        --min-lat 20 --max-lat 50 \\
        --min-lon 120 --max-lon 150 \\
        --japan-mask
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

try:
    import geopandas as gpd
except ImportError:
    gpd = None

from shapely.geometry import Point
from shapely.ops import unary_union

from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db import models

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Natural Earth から日本ポリゴンを取得するための設定
# ----------------------------------------------------------------------

# Natural Earth admin_0_countries (10m) の CDN URL
NE_ADMIN0_URL = (
    "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip"
)

# コンテナ内でのキャッシュ先
NE_ADMIN0_CACHE = Path("/tmp/ne_10m_admin_0_countries.zip")


def _ensure_admin0_zip() -> Path:
    """
    Natural Earth の admin_0_countries ZIP を /tmp にキャッシュして返す。

    - 既に /tmp/ne_10m_admin_0_countries.zip があればそれを使う
    - なければ NE_ADMIN0_URL からダウンロード
    - ダウンロード失敗時は RuntimeError を投げて、手動配置を案内
    """
    if NE_ADMIN0_CACHE.exists():
        logger.info("using cached Natural Earth admin_0_countries: %s", NE_ADMIN0_CACHE)
        return NE_ADMIN0_CACHE

    logger.info("downloading Natural Earth admin_0_countries from: %s", NE_ADMIN0_URL)
    try:
        resp = requests.get(NE_ADMIN0_URL, stream=True, timeout=120)
        resp.raise_for_status()
    except Exception as e:  # noqa: BLE001
        logger.error("failed to download Natural Earth admin_0_countries: %s", e)
        raise RuntimeError(
            "Failed to download Natural Earth admin_0_countries.\n"
            f"  URL: {NE_ADMIN0_URL}\n"
            "手動でこの ZIP をダウンロードして、コンテナ内の\n"
            "  /tmp/ne_10m_admin_0_countries.zip\n"
            "に配置してから再実行してください。"
        ) from e

    with open(NE_ADMIN0_CACHE, "wb") as f:
        for chunk in resp.iter_content(1024 * 1024):
            if not chunk:
                continue
            f.write(chunk)

    logger.info("saved Natural Earth admin_0_countries to: %s", NE_ADMIN0_CACHE)
    return NE_ADMIN0_CACHE


def get_japan_polygon():
    """
    Natural Earth admin_0_countries から日本 (ADM0_A3 == 'JPN') のポリゴンを取得して返す。

    - CRS は WGS84 (EPSG:4326) に揃える
    - マルチポリゴンは unary_union で 1 個の geometry にまとめる
    """
    if gpd is None:
        raise RuntimeError(
            "geopandas がインストールされていないため --japan-mask は使用できません。\n"
            "backend コンテナ内で `pip install geopandas shapely` などを実行してください。"
        )

    zip_path = _ensure_admin0_zip()
    logger.info("loading Natural Earth admin_0_countries from: %s", zip_path)

    # geopandas は zip:// パスを直接読める
    world = gpd.read_file(f"zip://{zip_path}")

    # Natural Earth の日本は ADM0_A3 == 'JPN' でフィルタするのが定番
    jp = world[world["ADM0_A3"] == "JPN"]
    if jp.empty:
        raise RuntimeError("Natural Earth admin_0_countries に ADM0_A3 == 'JPN' が見つかりません。")

    # 念のため WGS84 に揃える
    jp = jp.to_crs(epsg=4326)

    # 複数ポリゴンを一つにまとめる
    geom = unary_union(jp.geometry)
    logger.info("Japan polygon loaded (type=%s)", geom.geom_type)
    return geom


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
        help=(
            "gsmap_points をストリーム取得するときのバッチサイズ "
            "(ORM の yield_per, デフォルト: 10000)"
        ),
    )

    # bbox フィルタ（任意）
    ap.add_argument(
        "--min-lat",
        type=float,
        default=None,
        help="緯度下限。指定した場合、この値以上の地点のみ対象 (例: 24.0)。未指定なら制限なし。",
    )
    ap.add_argument(
        "--max-lat",
        type=float,
        default=None,
        help="緯度上限。指定した場合、この値以下の地点のみ対象 (例: 46.0)。未指定なら制限なし。",
    )
    ap.add_argument(
        "--min-lon",
        type=float,
        default=None,
        help="経度下限。指定した場合、この値以上の地点のみ対象 (例: 123.0)。未指定なら制限なし。",
    )
    ap.add_argument(
        "--max-lon",
        type=float,
        default=None,
        help="経度上限。指定した場合、この値以下の地点のみ対象 (例: 146.0)。未指定なら制限なし。",
    )

    # 日本ポリゴンによるマスク
    ap.add_argument(
        "--japan-mask",
        action="store_true",
        help="日本国土ポリゴン（Natural Earth）内の格子点のみを対象にする",
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
        "build_gsmap_events: threshold=%.3f, start=%s, end=%s, dry_run=%s, japan_mask=%s",
        args.threshold_mm_h,
        start_dt,
        end_dt,
        args.dry_run,
        args.japan_mask,
    )
    logger.info(
        "bbox filter: min_lat=%s, max_lat=%s, min_lon=%s, max_lon=%s",
        args.min_lat,
        args.max_lat,
        args.min_lon,
        args.max_lon,
    )

    # 日本ポリゴンの事前ロード（エラーなら早めに落ちる）
    jp_geom = None
    if args.japan_mask:
        jp_geom = get_japan_polygon()

    with SessionLocal() as db:
        # --------------------------------------------------------------
        # 1) 既存のイベントを削除（同じ threshold & 期間 & bbox のみ）
        #    ※ japan_mask は SQL で適用できないので、ここでは考慮しない。
        # --------------------------------------------------------------
        if not args.dry_run:
            q_del = db.query(models.GsmapEvent).filter(
                models.GsmapEvent.threshold_mm_h == args.threshold_mm_h
            )
            if start_dt is not None:
                q_del = q_del.filter(models.GsmapEvent.start_ts_utc >= start_dt)
            if end_dt is not None:
                q_del = q_del.filter(models.GsmapEvent.start_ts_utc < end_dt)

            if args.min_lat is not None:
                q_del = q_del.filter(models.GsmapEvent.lat >= args.min_lat)
            if args.max_lat is not None:
                q_del = q_del.filter(models.GsmapEvent.lat <= args.max_lat)
            if args.min_lon is not None:
                q_del = q_del.filter(models.GsmapEvent.lon >= args.min_lon)
            if args.max_lon is not None:
                q_del = q_del.filter(models.GsmapEvent.lon <= args.max_lon)

            deleted = q_del.delete(synchronize_session=False)
            db.commit()
            logger.info("deleted old events: %d rows", deleted)

        # --------------------------------------------------------------
        # 2) gsmap_points を grid_id, ts_utc 順にストリーム取得
        # --------------------------------------------------------------
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

        # bbox フィルタ（ある場合のみ）
        if args.min_lat is not None:
            query = query.filter(models.GsmapPoint.lat >= args.min_lat)
        if args.max_lat is not None:
            query = query.filter(models.GsmapPoint.lat <= args.max_lat)
        if args.min_lon is not None:
            query = query.filter(models.GsmapPoint.lon >= args.min_lon)
        if args.max_lon is not None:
            query = query.filter(models.GsmapPoint.lon <= args.max_lon)

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

            # 日本ポリゴン外ならスキップ
            if jp_geom is not None:
                # lon, lat の順で Point を作る（Geo は x=lon, y=lat）
                if not Point(float(lon), float(lat)).within(jp_geom):
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
            "done. total_events=%d, inserted_events=%d (threshold=%.3f, period=%s..%s, japan_mask=%s)",
            total_events,
            inserted_events,
            args.threshold_mm_h,
            start_dt,
            end_dt,
            args.japan_mask,
        )


if __name__ == "__main__":
    main()
