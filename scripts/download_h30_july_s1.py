#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_h30_july_s1.py

平成30年7月豪雨で特定したS1シーンのうち、
「日本国土に重なるもの」だけをフィルタしてダウンロードする。

参考スクリプト:
  - backend/scripts/download_s1_by_area.py  (password grant / download_product)
  - backend/scripts/download_s1_events_by_grid.py (S1CDSEClient の使い方)
  - scripts/h30_july_rain_s1_search.py       (STAC一括検索)

フロー:
  1. 前回検索結果 s1_scenes.csv を読み込む
  2. STAC API でフットプリント(geometry)を取得
  3. Natural Earth の日本国土ポリゴンと intersect チェック → 非日本を除外
  4. 同一パス（同日・同軌道）をグループ化し、代表stac_idで1回だけダウンロード
  5. download_s1_by_area.py と同じ Password Grant 認証 + download_product() を使用
"""

from __future__ import annotations

import os
import sys
import time
import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
import cartopy.io.shapereader as shpreader

# ─────────────────────────────────────────────────────────────────────────────
# 設定
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 前回の出力ディレクトリ
INPUT_SCENES_CSV = "output/h30_july/s1_scenes.csv"
OUTPUT_DIR       = "output/h30_july"

# ダウンロード先
DOWNLOAD_DIR = Path("output/h30_july/s1_downloads")

# CDSE 設定
CDSE_TOKEN_URL  = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CDSE_STAC_URL   = "https://stac.dataspace.copernicus.eu/v1/search"

# ─────────────────────────────────────────────────────────────────────────────
# .env 読み込み
# ─────────────────────────────────────────────────────────────────────────────
def load_env(path: str = "backend/.env") -> Dict[str, str]:
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

ENV = load_env()
CDSE_USERNAME      = ENV.get("CDSE_USERNAME", "")
CDSE_PASSWORD      = ENV.get("CDSE_PASSWORD", "")
CDSE_CLIENT_ID     = ENV.get("CDSE_CLIENT_ID", "")
CDSE_CLIENT_SECRET = ENV.get("CDSE_CLIENT_SECRET", "")

# ─────────────────────────────────────────────────────────────────────────────
# 日本国土ポリゴン取得 (Natural Earth 10m)
# ─────────────────────────────────────────────────────────────────────────────
def get_japan_polygon():
    """Natural Earth 10m から日本の MultiPolygon を返す"""
    logger.info("日本国土ポリゴンを Natural Earth から取得中...")
    shp_path = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_0_countries"
    )
    reader = shpreader.Reader(shp_path)
    japan_geoms = []
    for rec in reader.records():
        name = rec.attributes.get("NAME", "") or rec.attributes.get("ADMIN", "")
        if "Japan" in name:
            japan_geoms.append(rec.geometry)

    if not japan_geoms:
        raise RuntimeError("日本のポリゴンが見つかりませんでした")

    japan = unary_union(japan_geoms)
    logger.info(f"  日本ポリゴン取得完了 (geom_type={japan.geom_type})")
    return japan


# ─────────────────────────────────────────────────────────────────────────────
# CDSE 認証 (download_s1_by_area.py の Password Grant 方式を踏襲)
# ─────────────────────────────────────────────────────────────────────────────
_ACCESS_TOKEN: Optional[str] = None
_TOKEN_EXPIRE:  datetime     = datetime.min.replace(tzinfo=timezone.utc)

def get_token(session: requests.Session) -> str:
    """Password Grant (CDSE_USERNAME/PASSWORD) → Client Credentials の順で試みる"""
    global _ACCESS_TOKEN, _TOKEN_EXPIRE
    now = datetime.now(timezone.utc)
    if _ACCESS_TOKEN and now < _TOKEN_EXPIRE:
        return _ACCESS_TOKEN

    # ① Password Grant (download_s1_by_area.py と同じ)
    if CDSE_USERNAME and CDSE_PASSWORD:
        data = {
            "grant_type": "password",
            "client_id":  "cdse-public",
            "username":   CDSE_USERNAME,
            "password":   CDSE_PASSWORD,
        }
        try:
            resp = session.post(CDSE_TOKEN_URL, data=data, timeout=30)
            resp.raise_for_status()
            j = resp.json()
            _ACCESS_TOKEN = j["access_token"]
            _TOKEN_EXPIRE = now + timedelta(seconds=int(j.get("expires_in", 3600)) - 60)
            logger.info("  認証成功 (Password Grant)")
            return _ACCESS_TOKEN
        except Exception as e:
            logger.warning(f"Password Grant 失敗: {e}, Client Credentials を試みます")

    # ② Client Credentials フォールバック
    if CDSE_CLIENT_ID and CDSE_CLIENT_SECRET:
        data = {
            "grant_type":    "client_credentials",
            "client_id":     CDSE_CLIENT_ID,
            "client_secret": CDSE_CLIENT_SECRET,
        }
        try:
            resp = session.post(CDSE_TOKEN_URL, data=data, timeout=30)
            resp.raise_for_status()
            j = resp.json()
            _ACCESS_TOKEN = j["access_token"]
            _TOKEN_EXPIRE = now + timedelta(seconds=int(j.get("expires_in", 3600)) - 60)
            logger.info("  認証成功 (Client Credentials)")
            return _ACCESS_TOKEN
        except Exception as e:
            logger.error(f"Client Credentials も失敗: {e}")

    return ""


def auth_headers(session: requests.Session) -> Dict[str, str]:
    token = get_token(session)
    h = {"Accept": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


# ─────────────────────────────────────────────────────────────────────────────
# STAC: stac_id → geometry 取得
# ─────────────────────────────────────────────────────────────────────────────
def _parse_dt(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    if "." in s:
        main, frac = s.split(".", 1)
        tz_part = ""
        if "+" in frac:
            frac, tz_part = frac.split("+", 1)
            tz_part = "+" + tz_part
        s = f"{main}.{frac[:6]}{tz_part}"
    return datetime.fromisoformat(s)


def stac_get_geometry_by_id(
    session: requests.Session, stac_id: str
) -> Optional[Any]:
    """stac_id を直接 STAC API に投げてジオメトリを取得する"""
    # CDSE STAC の items エンドポイント: GET /collections/sentinel-1-grd/items/{id}
    url = f"https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-grd/items/{stac_id}"
    for attempt in range(4):
        try:
            resp = session.get(url, headers=auth_headers(session), timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            feat = resp.json()
            geom = feat.get("geometry")
            return geom
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = 2.0 * (2 ** attempt) + random.random() * 2
                logger.warning(f"  429 Rate limit, wait {wait:.1f}s ...")
                time.sleep(wait)
            else:
                logger.debug(f"  HTTP error for {stac_id}: {e}")
                return None
        except Exception as e:
            logger.debug(f"  Error for {stac_id}: {e}")
            return None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# ダウンロード (download_s1_by_area.py の download_product 方式を踏襲)
# ─────────────────────────────────────────────────────────────────────────────
def normalize_product_name(name: str) -> str:
    """_COG / .SAFE を除去して製品名の stem を返す"""
    if name.endswith("_COG"):
        name = name[:-4]
    if name.endswith(".SAFE"):
        name = name[:-5]
    return name


def get_odata_uuid(session: requests.Session, product_name: str) -> Optional[str]:
    """OData API で製品名からUUIDを取得 (download_s1_by_area.py と同じロジック)"""
    stem = normalize_product_name(product_name)
    url  = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

    for filter_q in [
        f"Name eq '{stem}.SAFE' or Name eq '{stem}'",
        f"contains(Name, '{stem}')",
    ]:
        params = {
            "$filter":  filter_q,
            "$top":     1,
            "$orderby": "ContentDate/Start desc",
        }
        try:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            values = resp.json().get("value", [])
            if values:
                return values[0].get("Id")
        except Exception:
            continue
    return None


def download_product(session: requests.Session, stac_id: str, out_dir: Path) -> Optional[Path]:
    """
    stac_id → OData UUID を解決し、SAFE.zip をダウンロードする。
    download_s1_by_area.py / S1CDSEClient.download_product() と同じ手順。
    """
    product_name = normalize_product_name(stac_id)
    safe_filename = product_name + ".zip"
    save_path = out_dir / safe_filename

    if save_path.exists() and save_path.stat().st_size > 0:
        logger.info(f"  既存スキップ: {safe_filename}")
        return save_path

    logger.info(f"  OData UUID を解決中: {product_name}")
    uuid = get_odata_uuid(session, product_name)
    if not uuid:
        logger.error(f"  UUID が見つかりません: {product_name}")
        return None

    download_url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value"
    logger.info(f"  ダウンロード開始: {safe_filename}")

    try:
        with session.get(
            download_url,
            headers=auth_headers(session),
            stream=True,
            timeout=120,
        ) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            done  = 0
            with open(save_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        done += len(chunk)
                        if total > 0:
                            pct = done / total * 100
                            print(f"\r    {pct:5.1f}%  ({done/1e6:.1f}/{total/1e6:.1f} MB)", end="", flush=True)
            print()
        logger.info(f"  -> 保存: {save_path}")
        return save_path
    except Exception as e:
        logger.error(f"  ダウンロード失敗: {e}")
        if save_path.exists():
            save_path.unlink()
        return None


# ─────────────────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────────────────
def main():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # ── 1. 前回検索結果の読み込み ──
    logger.info(f"シーンCSV読み込み: {INPUT_SCENES_CSV}")
    df_scenes = pd.read_csv(INPUT_SCENES_CSV)
    logger.info(f"  全シーン数: {len(df_scenes)}")

    # ── 2. 日本国土ポリゴン ──
    japan_poly = get_japan_polygon()

    # ── 3. フットプリント取得 & 日本陸地フィルタ ──
    session = requests.Session()
    logger.info("各シーンのフットプリントを取得して日本陸地と重なりを確認中...")

    df_scenes["intersects_japan"] = False
    df_scenes["footprint_wkt"]    = None

    for idx, row in df_scenes.iterrows():
        stac_id = row["stac_id"]
        geom_dict = stac_get_geometry_by_id(session, stac_id)
        time.sleep(0.3)  # API制限への配慮

        if geom_dict is None:
            logger.warning(f"  [{idx+1}/{len(df_scenes)}] geom取得失敗: {stac_id[:60]}")
            continue

        try:
            geom = shape(geom_dict)
            intersects = geom.intersects(japan_poly)
            df_scenes.at[idx, "intersects_japan"] = intersects
            df_scenes.at[idx, "footprint_wkt"]    = geom.wkt

            status = "✓ Japan" if intersects else "✗ 非Japan"
            logger.info(f"  [{idx+1:2d}/{len(df_scenes)}] {status} | {row['acq_jst']} | {row['orbit_dir']} orbit={row['rel_orbit']} | {stac_id[:50]}...")
        except Exception as e:
            logger.warning(f"  [{idx+1}/{len(df_scenes)}] geometry error: {e}")

    # フィルタ結果の保存
    filtered_csv = os.path.join(OUTPUT_DIR, "s1_scenes_japan_filtered.csv")
    df_scenes.to_csv(filtered_csv, index=False, encoding="utf-8-sig")
    logger.info(f"\nフィルタ結果CSV: {filtered_csv}")

    df_japan = df_scenes[df_scenes["intersects_japan"] == True].copy()
    logger.info(f"日本国土と重なるシーン: {len(df_japan)} / {len(df_scenes)}")

    if df_japan.empty:
        logger.error("日本と重なるシーンが0件です。終了します。")
        return

    # ── 4. 同一パス（同日・同軌道）でグループ化して代表シーンを選ぶ ──
    #   stac_idから日付・軌道方向・相対軌道を取り、パスごとに1回だけDLする
    df_japan["acq_date"]  = pd.to_datetime(df_japan["acq_jst"]).dt.date.astype(str)
    groups = df_japan.groupby(["acq_date", "orbit_dir", "rel_orbit"], dropna=False)

    targets = []
    for key, grp in groups:
        # そのパスの全stac_idをリスト化 → 全グラニュールをDL対象にする
        for _, row in grp.iterrows():
            targets.append({
                "stac_id":   row["stac_id"],
                "acq_jst":  row["acq_jst"],
                "orbit_dir": row["orbit_dir"],
                "rel_orbit": row["rel_orbit"],
                "acq_date":  row["acq_date"],
            })

    logger.info(f"\nダウンロード対象: {len(targets)} シーン（日本国土と交差するもの）")
    logger.info("=" * 60)
    for t in targets:
        logger.info(f"  {t['acq_jst']} | {t['orbit_dir']:12s} | orbit={t['rel_orbit']} | {t['stac_id'][:60]}")
    logger.info("=" * 60)

    # ── 5. ダウンロード ──
    success_count = 0
    fail_count    = 0

    for i, t in enumerate(targets, 1):
        stac_id = t["stac_id"]
        logger.info(f"\n[{i}/{len(targets)}] {t['acq_jst']} | {t['orbit_dir']} orbit={t['rel_orbit']}")
        logger.info(f"  stac_id: {stac_id}")

        saved = download_product(session, stac_id, DOWNLOAD_DIR)
        if saved:
            success_count += 1
        else:
            fail_count += 1

        time.sleep(1)  # API制限への配慮

    # ── 6. サマリ ──
    logger.info("\n" + "=" * 60)
    logger.info("=== ダウンロード完了 ===")
    logger.info(f"  対象シーン数:    {len(targets)}")
    logger.info(f"  成功:            {success_count}")
    logger.info(f"  失敗:            {fail_count}")
    logger.info(f"  保存先:          {DOWNLOAD_DIR}")
    logger.info("=" * 60)

    # ダウンロード結果をCSVに記録
    result_records = []
    for t in targets:
        product_name = normalize_product_name(t["stac_id"])
        zip_path = DOWNLOAD_DIR / (product_name + ".zip")
        result_records.append({
            "stac_id":    t["stac_id"],
            "acq_jst":    t["acq_jst"],
            "orbit_dir":  t["orbit_dir"],
            "rel_orbit":  t["rel_orbit"],
            "downloaded": zip_path.exists() and zip_path.stat().st_size > 0,
            "file_path":  str(zip_path) if zip_path.exists() else "",
            "file_size_mb": round(zip_path.stat().st_size / 1e6, 1) if zip_path.exists() else 0,
        })

    result_csv = os.path.join(OUTPUT_DIR, "download_results.csv")
    pd.DataFrame(result_records).to_csv(result_csv, index=False, encoding="utf-8-sig")
    logger.info(f"  ダウンロード結果CSV: {result_csv}")


if __name__ == "__main__":
    main()
