#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/import_gsmap_points.py

FTP などで取得した GSMaP Gauge v8 hourly のバイナリ
(.dat.gz) を直接パースして、MySQL の gsmap_points テーブルに
格納するスクリプト。

- 全球 0.1 度グリッドのうち、日本領域だけを抽出
  - まず bbox（緯度経度の範囲）で粗く絞り込み
  - さらに Natural Earth の Japan ポリゴンで「日本の陸地だけ」にマスク
- 1 ファイル = 1 時刻 (UTC, 1 時間平均) として扱う
- 中間 CSV ファイルは作成しない

使い方例 (コンテナ内):

  root@backend:/app# python -m scripts.import_gsmap_points \
        --min-lat 20 --max-lat 50 \
        --min-lon 120 --max-lon 150 \
        --min-gauge-mm-h 0.1
"""

from __future__ import annotations

import argparse
import gzip
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Tuple

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models

logger = logging.getLogger(__name__)

# 例: gsmap_gauge.20180101.0000.v8.0000.1.dat.gz
FNAME_RE = re.compile(
    r"gsmap_gauge\.(\d{8})\.(\d{4})\.v8\..*\.dat\.gz$"
)

# Natural Earth admin_0 の URL
NE_ADMIN_URL = "https://naturalearth.s3.amazonaws.com/10m_cultural/ne_10m_admin_0_countries.zip"


# ----------------------------------------------------------------------
# Natural Earth から Japan ポリゴンを取得
# ----------------------------------------------------------------------
def _download_and_cache(url: str, cache_dir: Path) -> Path:
    """URL から zip をダウンロードしてキャッシュ。既にあれば再利用。"""
    import requests

    cache_dir.mkdir(parents=True, exist_ok=True)
    zpath = cache_dir / Path(url).name
    if not zpath.exists():
        logger.info(f"Downloading Natural Earth: {url}")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        zpath.write_bytes(r.content)
        logger.info(f"Cached: {zpath}")
    else:
        logger.debug(f"Using cached file: {zpath}")
    return zpath


def _unzip_if_needed(zfile: Path, out_dir: Path) -> Path:
    """zip を out_dir に解凍（1 回だけ）。"""
    import zipfile

    out_dir.mkdir(parents=True, exist_ok=True)
    marker = out_dir / (zfile.stem + ".unzipped")
    if not marker.exists():
        with zipfile.ZipFile(zfile, "r") as zf:
            zf.extractall(out_dir)
        marker.write_text("ok")
        logger.debug(f"Unzipped to: {out_dir}")
    return out_dir


def load_japan_geom(cache_root: Path) -> Optional["BaseGeometry"]:
    """
    Natural Earth admin_0_countries から Japan ポリゴンを 1 つの geometry として返す。
    失敗したら None。
    """
    try:
        import geopandas as gpd
        from shapely.ops import unary_union
    except Exception as e:
        logger.error(f"Missing dependency for Japan geom: {e}")
        return None

    try:
        z_admin = _download_and_cache(NE_ADMIN_URL, cache_root)
        d_admin = _unzip_if_needed(z_admin, cache_root / "ne_admin_10m")

        shp = next(d_admin.glob("*.shp"))
        # 必要な列だけ読む
        gdf = gpd.read_file(shp)[["ADMIN", "NAME_LONG", "geometry"]]

        jp = gdf[(gdf["ADMIN"] == "Japan") | (gdf["NAME_LONG"] == "Japan")]
        if jp.empty:
            logger.warning("Japan not found in admin_0_countries")
            return None

        geom = unary_union(jp.geometry.values).buffer(0)
        logger.info("Japan land polygon loaded from Natural Earth.")
        return geom

    except Exception as e:
        logger.warning(f"Failed to load Japan geom: {e}")
        return None



def mask_on_geom(
    lats: np.ndarray,
    lons: np.ndarray,
    geom: Optional["BaseGeometry"],
) -> np.ndarray:
    """
    lat/lon 1 次元配列に対して、geom 内かどうかの bool マスクを返す。
    geom が None の場合は全 True。
    """
    if geom is None or lats.size == 0:
        return np.ones_like(lats, dtype=bool)

    try:
        import shapely

        pts = shapely.points(lons, lats)
        mask = shapely.covers(geom, pts)  # ポリゴン境界も含める
        if isinstance(mask, np.ndarray):
            return mask.astype(bool)
        else:
            return np.ones_like(lats, dtype=bool)
    except Exception as e:
        logger.warning(f"Japan land mask failed: {e}")
        return np.ones_like(lats, dtype=bool)


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
    # 日本領域（デフォルトはざっくり日本周辺）。ポリゴン前の粗い絞り込み用。
    parser.add_argument("--min-lat", type=float, default=20.0)
    parser.add_argument("--max-lat", type=float, default=50.0)
    parser.add_argument("--min-lon", type=float, default=120.0)
    parser.add_argument("--max-lon", type=float, default=150.0)
    # 0 ばかり入るのを避けるためのしきい値
    parser.add_argument(
        "--min-gauge-mm-h",
        type=float,
        default=0.0,
        help="この値 (mm/h) 未満の格子は登録しない（デフォルト: 0.0）",
    )
    parser.add_argument(
        "--ne-cache-dir",
        type=str,
        default="./.ne_cache",
        help="Natural Earth データのキャッシュディレクトリ",
    )
    return parser.parse_args()


# ----------------------------------------------------------------------
# ファイル列挙
# ----------------------------------------------------------------------
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


# ----------------------------------------------------------------------
# GSMaP バイナリ 1 ファイルを読む
# ----------------------------------------------------------------------
def load_one_dat_gz(path: Path) -> Tuple[datetime, np.ndarray, np.ndarray, np.ndarray]:
    """
    1 つの GSMaP Gauge v8 hourly dat.gz を読み込んで、
    - 観測時刻 (UTC; datetime, tzinfo=UTC)
    - lat2d: shape (1200, 3600)
    - lon2d: shape (1200, 3600)
    - gauge_mm_h: shape (1200, 3600)
    を返す。

    前提:
      - 全球 0.1deg (lon: 3600, lat: 1200)
      - data: big-endian float32 (>f4)
      - lat:  59.95, 59.85, ..., -59.95  (60N〜60S)
      - lon:   0.05,  0.15, ..., 359.95 (0〜360E) を -180〜180 に変換
    """
    m = FNAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected GSMaP file name: {path.name}")

    date_str, hhmm = m.groups()  # "20180101", "0000" など
    ts = datetime.strptime(date_str + hhmm, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)

    # バイナリ読み込み
    with gzip.open(path, "rb") as f:
        raw = f.read()

    # big-endian float32 で読み出し
    data = np.frombuffer(raw, dtype=">f4")

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

    # 緯度・経度の 2D 配列を作成
    # lat: 59.95, 59.85, ..., -59.95
    # lon: 0.05, 0.15, ..., 359.95 → -180〜180 へ
    lat_1d = 59.95 - 0.1 * np.arange(nlat)
    lon_1d_0_360 = 0.05 + 0.1 * np.arange(nlon)
    lon_1d = (lon_1d_0_360 + 180.0) % 360.0 - 180.0

    lat2d, lon2d = np.meshgrid(lat_1d, lon_1d, indexing="ij")  # (lat, lon)

    return ts, lat2d, lon2d, gauge


# ----------------------------------------------------------------------
# DB へ INSERT
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
    japan_geom: Optional["BaseGeometry"],
    source_file: str,
) -> int:
    """
    日本領域に入る & しきい値以上のセルだけを抽出して DB に入れる。
    さらに Japan ポリゴンで「日本の陸地のみ」に絞る。
    戻り値は登録したレコード数。
    """
    # NaN/inf を除外
    valid = np.isfinite(gauge_mm_h)

    # まず bbox + 雨量しきい値で 2D マスク
    mask_2d = (
        valid
        & (lat2d >= min_lat)
        & (lat2d <= max_lat)
        & (lon2d >= min_lon)
        & (lon2d <= max_lon)
        & (gauge_mm_h >= min_gauge_mm_h)
    )

    if not np.any(mask_2d):
        return 0

    # 2D を 1D に圧縮
    lats = lat2d[mask_2d].astype(float)
    lons = lon2d[mask_2d].astype(float)
    vals = gauge_mm_h[mask_2d].astype(float)

    # Japan ポリゴンでマスク
    mask_land = mask_on_geom(lats, lons, japan_geom)
    if not np.any(mask_land):
        return 0

    lats = lats[mask_land]
    lons = lons[mask_land]
    vals = vals[mask_land]

    count = 0
    for lat, lon, gval in zip(lats, lons, vals):
        db.add(
            models.GsmapPoint(
                ts_utc=ts_utc,
                lat=float(lat),
                lon=float(lon),
                gauge_mm_h=float(gval),
                rain_mm_h=None,
                region="Japan",
                grid_id=None,
                source_file=source_file,
            )
        )
        count += 1

    return count


# ----------------------------------------------------------------------
# メイン
# ----------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    args = parse_args()
    root = Path(args.root)
    logger.info("GSMAP root: %s", root)

    # Natural Earth から Japan ポリゴンをロード
    ne_cache = Path(args.ne_cache_dir)
    japan_geom = load_japan_geom(ne_cache)
    if japan_geom is None:
        logger.warning("Japan geom not available. Only bbox + gauge threshold will be applied.")

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
                    japan_geom=japan_geom,
                    source_file=rel_path,
                )
                db.commit()
                logger.info("  -> inserted %d rows for %s", inserted, rel_path)
            except Exception as e:  # noqa: BLE001
                logger.exception("failed to insert rows for %s: %s", f, e)
                db.rollback()


if __name__ == "__main__":
    main()
