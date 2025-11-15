#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
import_gsmap_points.py

FTP などで取得した GSMaP Gauge v8 hourly のバイナリ
(.dat または .dat.gz) を直接パースして、MySQL の
gsmap_points テーブルに格納するスクリプト。

- グローバル 0.1 度グリッドのうち、日本領域だけを抽出
- 1 ファイル = 1 時刻 (UTC, 1 時間平均) として扱う
- 中間 CSV ファイルは作成しない

使い方例:

  (venv) $ cd backend
  (venv) $ python -m scripts.import_gsmap_points `
        --root /path/to/rain-data-all/standard/v8/hourly_G `
        --min-lat 20 --max-lat 50 `
        --min-lon 120 --max-lon 150 `
        --threshold 0.0
"""

from __future__ import annotations

import argparse
import gzip
import logging
import re
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Tuple

from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db.models import GsmapPoint

logger = logging.getLogger(__name__)


# ---- GSMaP バイナリの仕様（v8 hourly gauge, 0.1deg, -60〜60N, 0〜360E 想定） ----
N_LON = 3600  # 0.1deg * 360
N_LAT = 1200  # 0.1deg * 120
CELL_SIZE_DEG = 0.1

# GSMaP では通常、「セルの中心が (lon=0.05, lat=-59.95)」のように 0.05 シフト
LON0 = 0.05      # 列 index=0 のセル中心経度
LAT0 = -59.95    # 行 index=0 のセル中心緯度

MISSING_VALUE = -999.0  # 欠損値（仕様に合わせて調整）


def parse_ts_from_filename(path: Path) -> datetime:
    """
    ファイル名から UTC 時刻をパースする。

    例: gsmap_gauge.20220414.1400.v8.XXX.dat.gz
         -> 2022-04-14 14:00:00+00:00
    """
    m = re.search(r"(\d{8})\.(\d{2})", path.name)
    if not m:
        raise ValueError(f"Cannot parse datetime from filename: {path.name}")

    yyyymmdd, hh = m.groups()
    dt = datetime.strptime(yyyymmdd + hh, "%Y%m%d%H")
    # GSMaP hourly は基本 UTC (00Z〜23Z) なので tzinfo=UTC を付与
    return dt.replace(tzinfo=timezone.utc)


def open_binary(path: Path) -> bytes:
    """dat または dat.gz を読み込んでバイト列を返す。"""
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f:
            return f.read()
    else:
        with path.open("rb") as f:
            return f.read()


def iter_japan_cells(
    buf: bytes,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
) -> Iterator[Tuple[float, float, float]]:
    """
    GSMaP グローバル 0.1deg グリッドから、日本領域に該当するセルだけを yield する。

    yield するのは (lat, lon, value_mm_h)。

    ※ バイナリフォーマットは「ビッグエンディアン float32 (mm/h)」を想定。
      もし JAXA の仕様が little-endian 等であれば struct のフォーマットを
      適宜修正してください。
    """
    expected_len = N_LAT * N_LON * 4  # float32=4byte
    if len(buf) != expected_len:
        logger.warning(
            "Unexpected binary size: len=%d (expected=%d)",
            len(buf),
            expected_len,
        )

    it = struct.iter_unpack(">f", buf)  # >f = big-endian float32

    for idx, (value,) in enumerate(it):
        if value <= MISSING_VALUE:
            continue  # 欠損はスキップ（仕様に応じて条件を調整）

        row = idx // N_LON  # 0〜N_LAT-1
        col = idx % N_LON   # 0〜N_LON-1

        lat = LAT0 + row * CELL_SIZE_DEG
        lon = LON0 + col * CELL_SIZE_DEG

        if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
            continue

        yield lat, lon, float(value)


def ingest_file(
    session: Session,
    path: Path,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    rain_threshold: float,
) -> int:
    """
    1 つの .dat(.gz) ファイルをパースして、gsmap_points に挿入する。

    戻り値は INSERT したレコード数。
    """
    ts_utc = parse_ts_from_filename(path)
    logger.info("Ingesting %s (ts=%s)", path.name, ts_utc.isoformat())

    buf = open_binary(path)

    batch = []
    inserted = 0
    BATCH_SIZE = 5000

    for lat, lon, gauge_mm_h in iter_japan_cells(
        buf,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lon=min_lon,
        max_lon=max_lon,
    ):
        if gauge_mm_h < rain_threshold:
            continue

        batch.append(
            GsmapPoint(
                ts_utc=ts_utc,
                lat=lat,
                lon=lon,
                gauge_mm_h=gauge_mm_h,
                rain_mm_h=None,
                region="Japan",
                grid_id=None,
                source_file=str(path),
            )
        )

        if len(batch) >= BATCH_SIZE:
            session.bulk_save_objects(batch)
            session.commit()
            inserted += len(batch)
            batch.clear()

    if batch:
        session.bulk_save_objects(batch)
        session.commit()
        inserted += len(batch)

    logger.info("Inserted %d rows from %s", inserted, path.name)
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="gsmap_gauge.*.dat(.gz) を格納したルートディレクトリ",
    )
    parser.add_argument("--min-lat", type=float, default=20.0)
    parser.add_argument("--max-lat", type=float, default=50.0)
    parser.add_argument("--min-lon", type=float, default=120.0)
    parser.add_argument("--max-lon", type=float, default=150.0)
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.0,
        help="この mm/h 以上のみ DB に格納（0.0 なら全て）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="insert せず件数だけログに出す",
    )
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    files = sorted(
        p for p in args.root.rglob("*.dat*")
        if p.is_file()
    )
    logger.info("Found %d binary files under %s", len(files), args.root)

    total_inserted = 0

    with SessionLocal() as session:
        for path in files:
            if args.dry_run:
                # サイズと時刻だけ確認したいとき用
                ts_utc = parse_ts_from_filename(path)
                logger.info("DRY-RUN: %s (ts=%s)", path.name, ts_utc.isoformat())
                continue

            total_inserted += ingest_file(
                session=session,
                path=path,
                min_lat=args.min_lat,
                max_lat=args.max_lat,
                min_lon=args.min_lon,
                max_lon=args.max_lon,
                rain_threshold=args.threshold,
            )

    logger.info("Total inserted rows: %d", total_inserted)


if __name__ == "__main__":
    main()