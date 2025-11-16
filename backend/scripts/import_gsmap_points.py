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
        --min-lat 20 --max-lat 50 \
        --min-lon 120 --max-lon 150 \
        --min-gauge-mm-h 4
"""

from __future__ import annotations

import argparse
import gzip
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models

logger = logging.getLogger(__name__)

# 例: gsmap_gauge.20180101.0000.v8.0000.1.dat.gz
FNAME_RE = re.compile(r"gsmap_gauge\.(\d{8})\.(\d{4})\.v8\..*\.dat\.gz$")


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
    return parser.parse_args()


def iter_dat_files(root: Path) -> Iterable[Path]:
    """
    root/YYYY/MM/DD/*.dat.gz を順に yield
    OS 依存しないように Path.glob を使う。
    """
    if not root.exists():
        raise FileNotFoundError(f"GSMAP_DATA_ROOT not found: {root}")

    for year_dir in sorted(root.glob("[0-9][0-9][0-9][0-9]")):
        for month_dir in sorted(year_dir.glob("[0-1][0-9]")):
            for day_dir in sorted(month_dir.glob("[0-3][0-9]")):
                for f in sorted(day_dir.glob("*.dat.gz")):
                    yield f


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


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = parse_args()
    root = Path(args.root)
    logger.info("GSMAP root: %s", root)

    with SessionLocal() as db:
        for idx, f in enumerate(iter_dat_files(root), start=1):
            rel_path = str(f.relative_to(root))

            # 再実行安全: すでに同じ source_file があればスキップ
            exists = db.execute(
                select(models.GsmapPoint.id).where(
                    models.GsmapPoint.source_file == rel_path
                )
            ).first()
            if exists:
                logger.info("[skip] already imported: %s", rel_path)
                continue

            logger.info("[%d] importing %s", idx, rel_path)

            try:
                ts_utc, lat2d, lon2d, gauge_mm_h = load_one_dat_gz(f)
            except Exception as e:  # noqa: BLE001
                logger.exception("failed to parse %s: %s", f, e)
                continue

            try:
                inserted = insert_points_for_file(
                    db,
                    ts_utc=ts_utc,
                    lat2d=lat2d,
                    lon2d=lon2d,
                    gauge_mm_h=gauge_mm_h,
                    min_lat=args.min_lat,
                    max_lat=args.max_lat,
                    min_lon=args.min_lon,
                    max_lon=args.max_lon,
                    min_gauge_mm_h=args.min_gauge_mm_h,
                    source_file=rel_path,
                )
                db.commit()
                logger.info("  -> inserted %d rows for %s", inserted, rel_path)
            except Exception as e:  # noqa: BLE001
                logger.exception("failed to insert rows for %s: %s", f, e)
                db.rollback()


if __name__ == "__main__":
    main()