#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_h30_july_s1_ref.py

H30.7月豪雨の降雨後シーン（24シーン）に対応する「晴れ日参照シーン」を取得する。
- 各ユニーク軌道（platform, rel_orbit, orbit_dir）について
  -12日, -24日, ... と遡り、シーンフットプリントbbox内に有意な降雨がない最近の日を選択
- CDSE STACで同軌道の参照シーンを検索
- 日本国土と重なるシーンのみダウンロード
- ESA SNAP GPTで前処理（Orbit→TNR→Cal→TC→dB）
"""

import csv
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pymysql
import requests
from shapely.geometry import shape
from shapely.wkt import loads as wkt_loads
import cartopy.io.shapereader as shpreader

# -------------------------------------------------------------------------
# 設定
# -------------------------------------------------------------------------
DB_CONFIG = dict(host="127.0.0.1", port=3307, user="rainsar", password="rainsar_pw", db="rainsar_hub")

CDSE_USERNAME = os.environ.get("CDSE_USERNAME", "RikuChestnut66@gmail.com")
CDSE_PASSWORD = os.environ.get("CDSE_PASSWORD", "Rikucopernicus1019/")

SCENES_CSV    = Path("output/h30_july/s1_scenes_japan_filtered.csv")
OUTPUT_DIR    = Path("output/h30_july/s1_ref_downloads")
PROCESSED_DIR = Path("output/h30_july/s1_ref_processed")
GRAPH_XML     = Path("scripts/s1_preprocess_graph.xml")
GPT_EXE       = r"C:\Program Files\esa-snap\bin\gpt.exe"

RAIN_THRESHOLD_MM_H = 2.0   # この値以上が1グリッドでもあれば「雨あり」
MAX_LOOKBACK_DAYS   = 180   # 最大180日遡る (15回分の12日サイクル = 梅雨前まで対応)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# 認証
# -------------------------------------------------------------------------
def get_token(session: requests.Session) -> str:
    url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    data = {
        "grant_type": "password",
        "client_id":  "cdse-public",
        "username":   CDSE_USERNAME,
        "password":   CDSE_PASSWORD,
    }
    r = session.post(url, data=data, timeout=30)
    r.raise_for_status()
    token = r.json()["access_token"]
    logger.info("  認証成功 (Password Grant)")
    return token


# -------------------------------------------------------------------------
# DB ヘルパー
# -------------------------------------------------------------------------
def has_rain_in_bbox(date_str: str, lat1: float, lat2: float,
                     lon1: float, lon2: float) -> bool:
    """指定日・bbox内に RAIN_THRESHOLD_MM_H 超のグリッドがあればTrue"""
    sql = """
        SELECT COUNT(*) FROM gsmap_points
        WHERE DATE(ts_utc) = %(dt)s
          AND gauge_mm_h > %(thr)s
          AND lat BETWEEN %(lat1)s AND %(lat2)s
          AND lon BETWEEN %(lon1)s AND %(lon2)s
        LIMIT 1
    """
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, dict(dt=date_str, thr=RAIN_THRESHOLD_MM_H,
                                  lat1=lat1, lat2=lat2, lon1=lon1, lon2=lon2))
            cnt = cur.fetchone()[0]
        return cnt > 0
    finally:
        conn.close()


def db_has_data_for_date(date_str: str) -> bool:
    """DBにその日付のデータが存在するか確認"""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gsmap_points WHERE DATE(ts_utc) = %s LIMIT 1", (date_str,))
            return cur.fetchone()[0] > 0
    finally:
        conn.close()


# -------------------------------------------------------------------------
# 日本国土ポリゴン
# -------------------------------------------------------------------------
def get_japan_polygon():
    shp = shpreader.natural_earth(resolution="10m", category="cultural", name="admin_0_countries")
    for rec in shpreader.Reader(shp).records():
        if "Japan" in rec.attributes.get("NAME", ""):
            return shape(rec.geometry)
    raise RuntimeError("Japan polygon not found")


# -------------------------------------------------------------------------
# STAC検索
# -------------------------------------------------------------------------
def stac_search_by_orbit(session: requests.Session, token: str,
                          orbit_dir: str, rel_orbit: int, platform: str,
                          date_start: str, date_end: str) -> list:
    """指定軌道・日付でSTACを検索し、ヒットしたアイテムのリストを返す。
    CQL2フィルタは使わずbbox+日付で取得し、Pythonでplatform/orbit絞り込み。"""
    headers = {"Authorization": f"Bearer {token}"}
    search_url = "https://stac.dataspace.copernicus.eu/v1/search"

    plat_map = {"sentinel-1a": "SENTINEL-1A", "sentinel-1b": "SENTINEL-1B"}
    target_platform = plat_map.get(platform, platform.upper())

    body = {
        "collections": ["sentinel-1-grd"],
        "datetime": f"{date_start}T00:00:00Z/{date_end}T23:59:59Z",
        "bbox": [122.0, 24.0, 150.0, 46.0],
        "limit": 100,
    }

    all_items = []
    url = search_url
    while url:
        r = session.post(url, json=body, headers=headers, timeout=60)
        if r.status_code == 401:
            token = get_token(session)
            headers["Authorization"] = f"Bearer {token}"
            r = session.post(url, json=body, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        all_items.extend(data.get("features", []))
        url = None
        body = {}
        for lnk in data.get("links", []):
            if lnk.get("rel") == "next":
                url = lnk["href"]
                break

    # Pythonでplatform・relative_orbit・orbit_dirを絞り込み
    matched = []
    for item in all_items:
        props = item.get("properties", {})
        item_plat = props.get("platform", "").upper()
        item_orb  = props.get("sat:relative_orbit")
        item_dir  = props.get("sat:orbit_state", "")
        if (item_plat == target_platform and
                item_orb == int(rel_orbit) and
                item_dir.lower() == orbit_dir.lower()):
            matched.append(item)

    return matched, token


# -------------------------------------------------------------------------
# OData ダウンロード
# -------------------------------------------------------------------------
def normalize_product_name(name: str) -> str:
    if name.endswith("_COG"):
        name = name[:-4]
    if name.endswith(".SAFE"):
        name = name[:-5]
    return name


def get_odata_uuid(session: requests.Session, token: str, product_name: str) -> str | None:
    url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"$filter": f"Name eq '{product_name}'", "$top": 1}
    r = session.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    items = r.json().get("value", [])
    if not items:
        return None
    return items[0]["Id"]


def download_product(session: requests.Session, token: str, uuid: str,
                     out_path: Path) -> bool:
    url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value"
    headers = {"Authorization": f"Bearer {token}"}
    r = session.get(url, headers=headers, stream=True, timeout=60)
    if r.status_code == 401:
        token = get_token(session)
        headers["Authorization"] = f"Bearer {token}"
        r = session.get(url, headers=headers, stream=True, timeout=60)
    r.raise_for_status()

    total = int(r.headers.get("Content-Length", 0))
    downloaded = 0
    tmp_path = out_path.with_suffix(".part")
    with open(tmp_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r  {pct:5.1f}%  ({downloaded/1e6:.1f}/{total/1e6:.1f} MB)",
                      end="", flush=True)
    print()
    tmp_path.rename(out_path)
    return True


# -------------------------------------------------------------------------
# GPT 前処理
# -------------------------------------------------------------------------
def run_gpt(zip_path: Path, out_tif: Path) -> bool:
    cmd = [
        GPT_EXE, str(GRAPH_XML),
        f"-PsourceFile={zip_path}",
        f"-PtargetFile={out_tif.with_suffix('')}",
        "-J-Xmx16G", "-c", "8G", "-e", "-q", "4",
    ]
    result = subprocess.run(cmd, cwd=str(Path(__file__).parent.parent))
    if result.returncode == 0:
        logger.info(f"  [GPT OK] {out_tif.name}")
        return True
    else:
        logger.error(f"  [GPT FAILED] exit={result.returncode}: {zip_path.name}")
        return False


# -------------------------------------------------------------------------
# メイン
# -------------------------------------------------------------------------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # 参照シーン結果CSV
    ref_csv_path = Path("output/h30_july/s1_ref_results.csv")

    # 1. イベントシーン読み込み
    df = pd.read_csv(SCENES_CSV)
    df = df[df["intersects_japan"] == True].copy()
    logger.info(f"イベントシーン数: {len(df)}")

    # フットプリントBBOXを抽出
    def bbox_from_wkt(wkt):
        from shapely.wkt import loads
        geom = loads(wkt)
        return geom.bounds  # (minx, miny, maxx, maxy)

    df["bbox"] = df["footprint_wkt"].apply(bbox_from_wkt)

    # 2. ユニーク軌道グループを特定
    df["acq_date"] = pd.to_datetime(df["acq_utc"]).dt.date
    groups = df.groupby(["platform", "rel_orbit", "orbit_dir"])

    japan_poly = get_japan_polygon()
    logger.info("日本国土ポリゴン取得完了")

    session = requests.Session()
    token = get_token(session)

    all_ref_scenes = []

    # 3. 各軌道グループで参照日を決定（一括バルククエリで高速化）
    for (platform, rel_orbit, orbit_dir), grp in groups:
        # グループのフットプリントを合算したbbox
        lon1 = min(b[0] for b in grp["bbox"])
        lat1 = min(b[1] for b in grp["bbox"])
        lon2 = max(b[2] for b in grp["bbox"])
        lat2 = max(b[3] for b in grp["bbox"])

        # 最遅のイベント取得日
        event_date = max(grp["acq_date"])
        logger.info(f"\n[{platform} | {orbit_dir} | orbit={rel_orbit}]")
        logger.info(f"  イベント日: {event_date} | bbox: {lat1:.1f}-{lat2:.1f}N, {lon1:.1f}-{lon2:.1f}E")

        # 全候補日をまとめて1クエリで雨チェック（高速化）
        candidates = [event_date - timedelta(days=12 * n)
                      for n in range(1, MAX_LOOKBACK_DAYS // 12 + 1)]
        cand_strs = [c.strftime("%Y-%m-%d") for c in candidates]
        placeholders = ",".join(["%s"] * len(cand_strs))
        sql = f"""
            SELECT DATE(ts_utc) FROM gsmap_points
            WHERE DATE(ts_utc) IN ({placeholders})
              AND gauge_mm_h > %(thr)s
              AND lat BETWEEN %(lat1)s AND %(lat2)s
              AND lon BETWEEN %(lon1)s AND %(lon2)s
            GROUP BY DATE(ts_utc)
        """
        conn = pymysql.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cur:
                params = cand_strs + [RAIN_THRESHOLD_MM_H, lat1, lat2, lon1, lon2]
                # pymysql: named + positional 混在不可、全%sに統一
                sql2 = f"""
                    SELECT DATE(ts_utc) FROM gsmap_points
                    WHERE DATE(ts_utc) IN ({placeholders})
                      AND gauge_mm_h > %s
                      AND lat BETWEEN %s AND %s
                      AND lon BETWEEN %s AND %s
                    GROUP BY DATE(ts_utc)
                """
                cur.execute(sql2, cand_strs + [RAIN_THRESHOLD_MM_H, lat1, lat2, lon1, lon2])
                rainy_dates = {r[0] for r in cur.fetchall()}
        finally:
            conn.close()

        # 候補日から最近の雨なし日を選択
        ref_date = None
        for n, cand in enumerate(candidates, 1):
            import datetime as _dt
            cand_d = cand if isinstance(cand, _dt.date) else cand.date()
            rainy_keys = {r if isinstance(r, _dt.date) else r for r in rainy_dates}
            is_rain = cand_d in rainy_keys
            status = "雨あり" if is_rain else "雨なし [OK]"
            logger.info(f"  {cand_d} (-{12*n}d): {status}")
            if not is_rain:
                ref_date = cand_d
                break

        if ref_date is None:
            logger.warning(f"  [WARN] {platform} orbit={rel_orbit}: 雨なし日が見つかりません")
            continue

        logger.info(f"  => 参照日決定: {ref_date}")

        # 4. STACで参照シーンを検索（±1日）
        date_s = (ref_date - timedelta(days=1)).strftime("%Y-%m-%d")
        date_e = (ref_date + timedelta(days=1)).strftime("%Y-%m-%d")
        items, token = stac_search_by_orbit(
            session, token, orbit_dir, int(rel_orbit), platform, date_s, date_e
        )
        logger.info(f"  STAC ヒット: {len(items)}シーン")

        for item in items:
            stac_id = item["id"]
            geom_raw = item.get("geometry")
            if geom_raw is None:
                continue
            geom = shape(geom_raw)
            if not geom.intersects(japan_poly):
                continue

            # STACの Product アセットから直接ダウンロードURL/UUIDを取得
            product_asset = item.get("assets", {}).get("Product", {})
            product_url = product_asset.get("href", "")
            # URL形式: https://download.../Products({uuid})/$value
            import re as _re
            m = _re.search(r"Products\(([^)]+)\)", product_url)
            odata_uuid = m.group(1) if m else None

            item_dt = item["properties"].get("datetime", "")[:10]
            all_ref_scenes.append({
                "stac_id":    stac_id,
                "platform":   platform,
                "orbit_dir":  orbit_dir,
                "rel_orbit":  rel_orbit,
                "ref_date":   ref_date.strftime("%Y-%m-%d"),
                "item_dt":    item_dt,
                "footprint":  geom.wkt,
                "odata_uuid": odata_uuid,
                "product_url": product_url,
            })
            logger.info(f"  -> 参照候補: {stac_id[:60]} (uuid={odata_uuid or 'N/A'})")

    # 重複排除
    seen = set()
    unique_scenes = []
    for sc in all_ref_scenes:
        if sc["stac_id"] not in seen:
            seen.add(sc["stac_id"])
            unique_scenes.append(sc)

    logger.info(f"\n参照シーン（日本重複・ユニーク）: {len(unique_scenes)}シーン")

    # 5. ダウンロード
    download_results = []
    for i, sc in enumerate(unique_scenes, 1):
        stac_id = sc["stac_id"]
        product_name = normalize_product_name(stac_id)
        zip_path = OUTPUT_DIR / f"{product_name}.zip"
        logger.info(f"\n[{i:02d}/{len(unique_scenes)}] {stac_id[:70]}")

        if zip_path.exists():
            logger.info(f"  SKIP (既存): {zip_path.name}")
            sc["downloaded"] = True
            sc["file_path"]  = str(zip_path)
            sc["file_size_mb"] = zip_path.stat().st_size / 1e6
            download_results.append(sc)
            continue

        # UUID取得: STACアセットから直接取得 → 失敗時はODataフォールバック
        uuid = sc.get("odata_uuid")
        if not uuid:
            logger.info(f"  STACからUUID取得できず、ODataで検索: {product_name}")
            uuid = get_odata_uuid(session, token, product_name)
            # .SAFE付きでも試みる
            if not uuid:
                uuid = get_odata_uuid(session, token, product_name + ".SAFE")
        if not uuid:
            logger.warning(f"  UUID取得失敗: {product_name}")
            sc["downloaded"] = False
            download_results.append(sc)
            continue

        logger.info(f"  UUID: {uuid}")
        logger.info(f"  ダウンロード開始: {zip_path.name}")
        try:
            ok = download_product(session, token, uuid, zip_path)
            sc["downloaded"]   = ok
            sc["file_path"]    = str(zip_path) if ok else ""
            sc["file_size_mb"] = zip_path.stat().st_size / 1e6 if ok else 0
        except Exception as e:
            logger.error(f"  ダウンロードエラー: {e}")
            sc["downloaded"] = False
        download_results.append(sc)

        # トークン更新 (30分おきに再取得)
        if i % 10 == 0:
            token = get_token(session)

    # 6. 結果CSV保存
    ref_df = pd.DataFrame(download_results)
    ref_df.to_csv(ref_csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"\n参照シーンCSV: {ref_csv_path}")

    # 7. GPT前処理
    ok_scenes = [sc for sc in download_results if sc.get("downloaded")]
    logger.info(f"\nGPT前処理: {len(ok_scenes)}シーン")
    logger.info("=" * 60)

    preproc_results = []
    for i, sc in enumerate(ok_scenes, 1):
        zip_path = Path(sc["file_path"])
        out_tif  = PROCESSED_DIR / f"{zip_path.stem}_proc.tif"

        if out_tif.exists():
            logger.info(f"[{i:02d}/{len(ok_scenes)}] SKIP (既存): {out_tif.name}")
            preproc_results.append((zip_path.stem, "skipped"))
            continue

        logger.info(f"[{i:02d}/{len(ok_scenes)}] 前処理: {zip_path.stem[:60]}")
        ok = run_gpt(zip_path, out_tif)
        preproc_results.append((zip_path.stem, "success" if ok else "failed"))

    # 前処理結果CSV
    proc_csv = PROCESSED_DIR / "preprocess_ref_results.csv"
    pd.DataFrame(preproc_results, columns=["scene", "status"]).to_csv(
        proc_csv, index=False, encoding="utf-8-sig"
    )

    # サマリー
    logger.info("\n" + "=" * 60)
    success = sum(1 for _, s in preproc_results if s == "success")
    skipped = sum(1 for _, s in preproc_results if s == "skipped")
    failed  = sum(1 for _, s in preproc_results if s == "failed")
    logger.info(f"参照シーン前処理: {success}成功 / {skipped}スキップ / {failed}失敗")
    logger.info(f"前処理結果: {proc_csv}")
    logger.info("完了!")


if __name__ == "__main__":
    main()
