#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/import_gsmap_points.py

FTP などで取得した GSMaP Gauge v8 hourly のバイナリ
(.dat.gz) を直接パースして、MySQL の gsmap_points テーブルに
格納するスクリプト。

- 全球 0.1 度グリッドのうち、日本周辺の bbox だけを抽出
- 1 ファイル = 1 時刻 (UTC, 1 時間平均) として扱う
- 中間 CSV ファイルは作成しない
- 値は mm/h のまま格納（標高補正などはここではしない）

使い方例 (コンテナ内):

  root@backend:/app# python -m scripts.import_gsmap_points \
        --root /data/gsmap \
        --min-lat 20 --max-lat 48 \
        --min-lon 120 --max-lon 150 \
        --min-gauge-mm-h 7 \
        --start-year 2018 --end-year 2020 \
        --workers 4
"""

from __future__ import annotations

import argparse
import gzip
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple, Optional

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models

logger = logging.getLogger(__name__)

# 例: gsmap_gauge.20180101.0000.v8.0000.1.dat.gz
FNAME_RE = re.compile(r"gsmap_gauge\.(\d{8})\.(\d{4})\.v8\..*\.dat\.gz$")


# ----------------------------------------------------------------------
# 引数
# ----------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        type=str,
        default=str(settings.gsmap_data_path),
        help="GSMAP バイナリのルートディレクトリ（YYYY/MM/DD/*.dat.gz がぶら下がる）",
    )

    # 日本周辺 bbox（必要に応じて調整）
    parser.add_argument("--min-lat", type=float, default=20.0)
    parser.add_argument("--max-lat", type=float, default=50.0)
    parser.add_argument("--min-lon", type=float, default=120.0)
    parser.add_argument("--max-lon", type=float, default=150.0)

    # 0 ばかり入るのを避けるためのしきい値（mm/h）
    parser.add_argument(
        "--min-gauge-mm-h",
        type=float,
        default=0.0,
        help="この値 (mm/h) 未満の格子は登録しない（デフォルト: 0.0）",
    )

    # 対象年の絞り込み
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="この年以降のファイルのみ対象（YYYY）。未指定なら制限なし。",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="この年までのファイルのみ対象（YYYY）。未指定なら制限なし。",
    )

    # 並列ワーカー数
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="ThreadPoolExecutor のワーカー数（デフォルト: 1=シングルスレッド）",
    )

    return parser.parse_args()


# ----------------------------------------------------------------------
# ファイル列挙
# ----------------------------------------------------------------------
def iter_dat_files(
    root: Path,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> Iterable[Path]:
    """
    root/YYYY/MM/DD/*.dat.gz を順に yield。
    start_year / end_year が指定されていれば、その範囲に含まれる年だけ。
    """
    if not root.exists():
        raise FileNotFoundError(f"GSMAP_DATA_ROOT not found: {root}")

    for year_dir in sorted(root.glob("[0-9][0-9][0-9][0-9]")):
        try:
            year = int(year_dir.name)
        except ValueError:
            continue

        if start_year is not None and year < start_year:
            continue
        if end_year is not None and year > end_year:
            continue

        for month_dir in sorted(year_dir.glob("[0-1][0-9]")):
            for day_dir in sorted(month_dir.glob("[0-3][0-9]")):
                for f in sorted(day_dir.glob("*.dat.gz")):
                    yield f


# ----------------------------------------------------------------------
# 1ファイル読み込み
# ----------------------------------------------------------------------
def load_one_dat_gz(path: Path) -> Tuple[datetime, np.ndarray, np.ndarray, np.ndarray]:
    """
    1 つの GSMaP Gauge v8 hourly dat.gz を読み込んで、
    - 観測時刻 (UTC; datetime, tzinfo=UTC)
    - lat2d: shape (1200, 3600)
    - lon2d: shape (1200, 3600)
    - gauge_mm_h: shape (1200, 3600) [mm/h]
    を返す。

    前提:
      - 全球 0.1deg (lon: 3600, lat: 1200)
      - data: little-endian float32 (<f4)
      - lat:  59.95, 59.85, ..., -59.95  (60N〜60S)
      - lon:   0.05,  0.15, ..., 359.95  (0〜360E) を -180〜180 に変換
      - 負値は欠損フラグ → NaN に置き換える
    """
    m = FNAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected GSMaP file name: {path.name}")

    date_str, hhmm = m.groups()  # "20180101", "0000" など
    ts = datetime.strptime(date_str + hhmm, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)

    # バイナリ読み込み
    with gzip.open(path, "rb") as f:
        raw = f.read()

    # little-endian float32 で読み出し（公式仕様）
    data = np.frombuffer(raw, dtype="<f4")

    # 全球 0.1deg (lon: 3600, lat: 1200) を想定
    nlon = 3600
    nlat = 1200
    expected = nlon * nlat
    if data.size != expected:
        raise ValueError(
            f"Unexpected data size for {path}: {data.size} != {expected}"
        )

    # (lat, lon) 形状に reshape
    gauge = data.reshape((nlat, nlon))

    # 欠損値: 負値は NaN に
    gauge = np.where(gauge < 0, np.nan, gauge)

    # 緯度・経度の 2D 配列を作成
    # lat: 59.95, 59.85, ..., -59.95
    # lon: 0.05, 0.15, ..., 359.95 → -180〜180 へ
    lat_1d = 59.95 - 0.1 * np.arange(nlat)
    lon_1d_0_360 = 0.05 + 0.1 * np.arange(nlon)
    lon_1d = (lon_1d_0_360 + 180.0) % 360.0 - 180.0

    lat2d, lon2d = np.meshgrid(lat_1d, lon_1d, indexing="ij")  # (lat, lon)

    return ts, lat2d, lon2d, gauge


# ----------------------------------------------------------------------
# 1ファイルぶん DB への insert
# ----------------------------------------------------------------------
def insert_points_for_file(
    db: Session,
    ts_utc: datetime,
    lat2d: np.ndarray,
    lon2d: np.ndarray,
    gauge_mm_h: np.ndarray,
    *,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    min_gauge_mm_h: float,
    source_file: str,
) -> int:
    """
    bbox に入る & しきい値以上のセルだけを抽出して DB に入れる。
    戻り値は登録したレコード数。
    """
    # NaN/inf を除外
    valid = np.isfinite(gauge_mm_h)

    mask = (
        valid
        & (lat2d >= min_lat)
        & (lat2d <= max_lat)
        & (lon2d >= min_lon)
        & (lon2d <= max_lon)
        & (gauge_mm_h >= min_gauge_mm_h)
    )

    idx_i, idx_j = np.where(mask)
    if idx_i.size == 0:
        return 0

    count = 0
    for i, j in zip(idx_i, idx_j):
        lat = float(lat2d[i, j])
        lon = float(lon2d[i, j])
        gval = float(gauge_mm_h[i, j])

        db.add(
            models.GsmapPoint(
                ts_utc=ts_utc,
                lat=lat,
                lon=lon,
                gauge_mm_h=gval,
                rain_mm_h=None,
                region="Japan",  # bbox 内という意味でとりあえず Japan にしておく
                grid_id=None,
                source_file=source_file,
            )
        )
        count += 1

    return count


# ----------------------------------------------------------------------
# スレッド 1本が 1ファイルを処理する関数
# ----------------------------------------------------------------------
def process_one_file(
    root: Path,
    f: Path,
    *,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    min_gauge_mm_h: float,
) -> tuple[str, int]:
    """
    1つの dat.gz ファイルをパースして DB に insert する。
    スレッドごとに独立した SessionLocal() を生成。
    """
    rel_path = str(f.relative_to(root))
    tname = threading.current_thread().name

    with SessionLocal() as db:
        # 再実行安全: すでに同じ source_file があればスキップ
        exists = db.execute(
            select(models.GsmapPoint.id).where(
                models.GsmapPoint.source_file == rel_path
            )
        ).first()
        if exists:
            logger.info("[thread %s] [skip] already imported: %s", tname, rel_path)
            return rel_path, 0

        logger.info("[thread %s] importing %s", tname, rel_path)

        ts_utc, lat2d, lon2d, gauge_mm_h = load_one_dat_gz(f)

        inserted = insert_points_for_file(
            db,
            ts_utc=ts_utc,
            lat2d=lat2d,
            lon2d=lon2d,
            gauge_mm_h=gauge_mm_h,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
            min_gauge_mm_h=min_gauge_mm_h,
            source_file=rel_path,
        )
        db.commit()

        logger.info(
            "[thread %s] done %s -> inserted %d rows",
            tname,
            rel_path,
            inserted,
        )
        return rel_path, inserted


# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = parse_args()
    root = Path(args.root)

    logger.info("GSMAP root: %s", root)
    logger.info(
        "bbox: lat=[%.2f, %.2f], lon=[%.2f, %.2f], min_gauge_mm_h=%.2f",
        args.min_lat,
        args.max_lat,
        args.min_lon,
        args.max_lon,
        args.min_gauge_mm_h,
    )
    logger.info(
        "year range: %s - %s (None=unlimited), workers=%d",
        args.start_year,
        args.end_year,
        args.workers,
    )

    files = list(iter_dat_files(root, args.start_year, args.end_year))
    total_files = len(files)
    if total_files == 0:
        logger.warning("No .dat.gz files found under %s", root)
        return

    logger.info("found %d dat.gz files in target year range", total_files)

    total_inserted = 0
    processed = 0

    # 並列処理
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_file = {
            executor.submit(
                process_one_file,
                root,
                f,
                min_lat=args.min_lat,
                max_lat=args.max_lat,
                min_lon=args.min_lon,
                max_lon=args.max_lon,
                min_gauge_mm_h=args.min_gauge_mm_h,
            ): f
            for f in files
        }

        for future in as_completed(future_to_file):
            f = future_to_file[future]
            try:
                rel_path, inserted = future.result()
                total_inserted += inserted
            except Exception as e:  # noqa: BLE001
                logger.exception("failed to process %s: %s", f, e)
            finally:
                processed += 1
                if processed % 10 == 0 or processed == total_files:
                    logger.info(
                        "progress: %d/%d files processed, total_inserted=%d",
                        processed,
                        total_files,
                        total_inserted,
                    )

    logger.info(
        "all done. processed=%d files, total_inserted=%d rows",
        processed,
        total_inserted,
    )


if __name__ == "__main__":
    main()