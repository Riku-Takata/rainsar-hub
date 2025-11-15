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
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Tuple

import numpy as np
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=str,
        default=str(settings.gsmap_data_path),
        help="GSMAP バイナリのルートディレクトリ（YYYY/MM/DD/*.dat.gz がぶら下がる）",
    )
    # 日本領域はとりあえず引数として持っておく（デフォルト: 大雑把な日本域）
    parser.add_argument("--min-lat", type=float, default=20.0)
    parser.add_argument("--max-lat", type=float, default=50.0)
    parser.add_argument("--min-lon", type=float, default=120.0)
    parser.add_argument("--max-lon", type=float, default=150.0)
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
    ここはすでに作ってある「GSMAP dat.gz パーサ」を使う想定。
    - 時刻（UTC）
    - 緯度配列
    - 経度配列
    - gauge_mm/h 配列
    を返す形に揃えておく。

    ここではダミー実装の形だけ記載（本物のパーサは既存コードを転用）。
    """
    raise NotImplementedError("既存のバイナリ読み取りロジックをここに移植してください")


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
    source_file: str,
) -> None:
    """
    日本領域に入るセルだけを抽出して DB に入れる。
    """
    mask = (
        (lat2d >= min_lat)
        & (lat2d <= max_lat)
        & (lon2d >= min_lon)
        & (lon2d <= max_lon)
    )

    idx_i, idx_j = np.where(mask)
    if idx_i.size == 0:
        return

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
                region="Japan",
                grid_id=None,
                source_file=source_file,
            )
        )


def main() -> None:
    args = parse_args()
    root = Path(args.root)

    with SessionLocal() as db:
        for f in iter_dat_files(root):
            # ここで dat.gz を読む（UTC 時刻と配列を取得）
            ts_utc, lat2d, lon2d, gauge_mm_h = load_one_dat_gz(f)

            insert_points_for_file(
                db,
                ts_utc=ts_utc,
                lat2d=lat2d,
                lon2d=lon2d,
                gauge_mm_h=gauge_mm_h,
                min_lat=args.min_lat,
                max_lat=args.max_lat,
                min_lon=args.min_lon,
                max_lon=args.max_lon,
                source_file=str(f.relative_to(root)),
            )
            db.commit()


if __name__ == "__main__":
    main()