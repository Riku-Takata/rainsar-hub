#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download_inundation_area_s1.py

浸水検出マスクから対象グリッドを抽出し、
降雨後 0~5時間以内に撮影された Sentinel-1 シーンを 2014〜2026年全期間で
検索・ダウンロード・前処理する。

手順:
1. output/h30_july/inundation/*/mask_*.tif から浸水 0.1°グリッドセルを抽出
2. DB(gsmap_points) から >= 10 mm/h の降雨時刻を年ごとに取得(2014-2026)
3. 四半期単位で CDSE STAC を Japan 全域一括検索 (49クエリ)
4. Python側で「撮影0~5h前に対象グリッドに >= 10mm/h 雨があったか」を判定
5. マッチシーン一覧を CSV に保存
6. ダウンロード → SNAP GPT 前処理
   --dry-run フラグで CSV 作成のみ (ダウンロードなし)
"""

import argparse
import csv
import logging
import os
import subprocess
import sys
import time
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pymysql
import requests
import rasterio
import rasterio.transform
from shapely.geometry import shape as shapely_shape

# -------------------------------------------------------------------------
# 設定
# -------------------------------------------------------------------------
DB_CONFIG = dict(
    host="127.0.0.1",
    port=3307,
    user="rainsar",
    password="rainsar_pw",
    db="rainsar_hub",
    charset="utf8mb4",
)

CDSE_USERNAME  = os.environ.get("CDSE_USERNAME", "RikuChestnut66@gmail.com")
CDSE_PASSWORD  = os.environ.get("CDSE_PASSWORD", "Rikucopernicus1019/")
CDSE_TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CDSE_STAC_URL  = "https://stac.dataspace.copernicus.eu/v1/search"
CDSE_ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
CDSE_DL_URL    = "https://download.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value"

INUND_DIR      = Path("output/h30_july/inundation")
DOWNLOAD_DIR   = Path("output/s1_rain_after/downloads")
PROCESSED_DIR  = Path("output/s1_rain_after/processed")
MATCH_CSV      = Path("output/s1_rain_after/matched_scenes.csv")
GRAPH_XML      = Path("scripts/s1_preprocess_graph.xml")
GPT_EXE        = r"C:\Program Files\esa-snap\bin\gpt.exe"

RAIN_THRESHOLD_MM_H = 10.0
AFTER_HOURS_MAX     = 5.0
GSMAP_CELL_SIZE     = 0.1

# S1A 打ち上げ: 2014-04-03, S1B: 2016-04-25, S1C: 2023-12-05
DATA_START = datetime(2014, 4, 1, tzinfo=timezone.utc)
DATA_END   = datetime(2026, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

# 日本全域 bbox (STAC 検索用)
JAPAN_MIN_LAT, JAPAN_MAX_LAT = 24.0, 46.0
JAPAN_MIN_LON, JAPAN_MAX_LON = 122.0, 150.0

# -------------------------------------------------------------------------
# ロギング
# -------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# 認証
# -------------------------------------------------------------------------
_tok: dict = {"v": None, "exp": datetime.min.replace(tzinfo=timezone.utc)}


def get_token(session: requests.Session) -> str:
    now = datetime.now(timezone.utc)
    if _tok["v"] and now < _tok["exp"]:
        return _tok["v"]
    r = session.post(CDSE_TOKEN_URL, data={
        "grant_type": "password", "client_id": "cdse-public",
        "username": CDSE_USERNAME, "password": CDSE_PASSWORD,
    }, timeout=30)
    r.raise_for_status()
    j = r.json()
    _tok["v"]   = j["access_token"]
    _tok["exp"] = now + timedelta(seconds=int(j.get("expires_in", 3600)) - 60)
    return _tok["v"]


def hdr(session):
    return {"Authorization": f"Bearer {get_token(session)}"}


# -------------------------------------------------------------------------
# ステップ1: 浸水グリッドセル抽出
# -------------------------------------------------------------------------
def extract_inundated_grid_cells() -> set:
    mask_paths = sorted(INUND_DIR.rglob("mask_*.tif"))
    logger.info(f"マスクファイル: {len(mask_paths)} 枚")
    cells: set = set()
    half = GSMAP_CELL_SIZE / 2
    for p in mask_paths:
        with rasterio.open(p) as src:
            data = src.read(1)
            rows, cols = np.where(data == 1)
            if len(rows) == 0:
                continue
            xs, ys = rasterio.transform.xy(src.transform, rows, cols)
            glats = np.floor(np.array(ys) / GSMAP_CELL_SIZE) * GSMAP_CELL_SIZE + half
            glons = np.floor(np.array(xs) / GSMAP_CELL_SIZE) * GSMAP_CELL_SIZE + half
            for gl, gln in zip(glats, glons):
                cells.add((round(float(gl), 2), round(float(gln), 2)))
    logger.info(f"浸水グリッドセル: {len(cells)} 個")
    return cells


# -------------------------------------------------------------------------
# ステップ2: 降雨イベント取得 (年ごと)
# -------------------------------------------------------------------------
def fetch_rain_events_all_years(grid_cells: set) -> dict:
    """
    rain_map = {(lat, lon): sorted list of ts_utc}
    年ごとにDBクエリしてメモリ効率化。
    """
    target_set = set(grid_cells)
    lats = sorted(set(g[0] for g in grid_cells))
    lons = sorted(set(g[1] for g in grid_cells))
    half = GSMAP_CELL_SIZE / 2
    lat_min, lat_max = lats[0] - half, lats[-1] + half
    lon_min, lon_max = lons[0] - half, lons[-1] + half

    rain_map: dict = {}  # (lat,lon) -> sorted list of ts

    conn = pymysql.connect(**DB_CONFIG)
    try:
        for year in range(2014, 2027):
            t_start = f"{year}-01-01 00:00:00"
            t_end   = f"{year}-12-31 23:59:59"
            sql = """
                SELECT lat, lon, ts_utc
                FROM gsmap_points
                WHERE ts_utc BETWEEN %(t_start)s AND %(t_end)s
                  AND gauge_mm_h >= %(thr)s
                  AND lat BETWEEN %(lat_min)s AND %(lat_max)s
                  AND lon BETWEEN %(lon_min)s AND %(lon_max)s
            """
            with conn.cursor() as cur:
                cur.execute(sql, dict(
                    t_start=t_start, t_end=t_end, thr=RAIN_THRESHOLD_MM_H,
                    lat_min=lat_min, lat_max=lat_max,
                    lon_min=lon_min, lon_max=lon_max,
                ))
                rows = cur.fetchall()

            matched = 0
            for lat, lon, ts in rows:
                key = (round(float(lat), 2), round(float(lon), 2))
                if key not in target_set:
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                rain_map.setdefault(key, []).append(ts)
                matched += 1

            logger.info(f"  {year}: bbox {len(rows)} 行 → 対象グリッド {matched} イベント")
    finally:
        conn.close()

    # ソート
    for key in rain_map:
        rain_map[key].sort()

    total = sum(len(v) for v in rain_map.values())
    logger.info(f"降雨イベント合計: {total} 件 ({len(rain_map)} グリッド)")
    return rain_map


# -------------------------------------------------------------------------
# ステップ3: 四半期単位 STAC 一括検索
# -------------------------------------------------------------------------
def quarter_ranges():
    """DATA_START から DATA_END まで 3ヶ月単位の (start, end) を生成。"""
    cur = DATA_START.replace(day=1, hour=0, minute=0, second=0)
    while cur < DATA_END:
        # 四半期末 = 3ヶ月後の1日 - 1秒
        m_end = cur.month + 3
        y_end = cur.year
        if m_end > 12:
            m_end -= 12
            y_end += 1
        next_q = datetime(y_end, m_end, 1, tzinfo=timezone.utc)
        q_end = min(next_q - timedelta(seconds=1), DATA_END)
        yield cur, q_end
        cur = next_q


def stac_search_period(session: requests.Session, start: datetime, end: datetime) -> list:
    """指定期間の日本全域 S1 IW GRD VV+VH シーンを取得。
    next リンクの body (token) を使ったページネーションで全件取得。
    """
    first_body = {
        "collections": ["sentinel-1-grd"],
        "bbox": [JAPAN_MIN_LON, JAPAN_MIN_LAT, JAPAN_MAX_LON, JAPAN_MAX_LAT],
        "datetime": (
            f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/"
            f"{end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        ),
        "limit": 100,
    }
    all_items = []
    post_body = first_body
    page_num = 0

    while post_body is not None:
        page_num += 1
        for attempt in range(8):
            try:
                r = session.post(CDSE_STAC_URL, json=post_body, headers=hdr(session), timeout=90)
                if r.status_code == 401:
                    _tok["v"] = None
                    r = session.post(CDSE_STAC_URL, json=post_body, headers=hdr(session), timeout=90)
                if r.status_code == 429:
                    w = float(r.headers.get("Retry-After", 30)) + 5
                    logger.warning(f"    Rate limit. Wait {w:.0f}s")
                    time.sleep(w)
                    continue
                r.raise_for_status()
                break
            except requests.exceptions.Timeout:
                logger.warning(f"    Timeout attempt {attempt+1}/8")
                time.sleep(15)
            except Exception as e:
                logger.warning(f"    Error: {e}")
                time.sleep(5)

        data = r.json()
        features = data.get("features", [])
        for item in features:
            p = item.get("properties", {})
            if p.get("sar:instrument_mode") == "IW":
                pols = p.get("sar:polarizations", [])
                if "VV" in pols and "VH" in pols:
                    all_items.append(item)

        # next リンクの body を次のリクエストに使う
        post_body = None
        for lnk in data.get("links", []):
            if lnk.get("rel") == "next" and lnk.get("body"):
                post_body = lnk["body"]
                break

        if page_num % 10 == 0:
            logger.info(f"    ... page {page_num}, 累計 {len(all_items)} シーン")
        time.sleep(0.3)

    return all_items


# -------------------------------------------------------------------------
# ステップ4: 雨後フィルタ
# -------------------------------------------------------------------------
def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    if "." in s:
        m, f = s.split(".", 1)
        tz = ""
        if "+" in f:
            f, tz = f.split("+", 1)
            tz = "+" + tz
        s = f"{m}.{f[:6]}{tz}"
    return datetime.fromisoformat(s)


def is_rain_after(item: dict, rain_map: dict) -> bool:
    """
    シーン取得時刻 t に対し、[t-5h, t] の間に
    シーンの bbox 内の浸水グリッドで >= threshold の雨があればTrue。
    バイナリサーチで高速化。
    """
    props = item.get("properties", {})
    acq = _parse_dt(props.get("datetime") or props.get("start_datetime"))
    if not acq:
        return False
    if acq.tzinfo is None:
        acq = acq.replace(tzinfo=timezone.utc)

    geom = item.get("geometry")
    if not geom:
        return False
    try:
        shape = shapely_shape(geom)
    except Exception:
        return False

    minx, miny, maxx, maxy = shape.bounds
    half = GSMAP_CELL_SIZE / 2
    win_start = acq - timedelta(hours=AFTER_HOURS_MAX)
    win_end   = acq

    for (glat, glon), ts_list in rain_map.items():
        if not (miny - half <= glat <= maxy + half and
                minx - half <= glon <= maxx + half):
            continue
        lo = bisect_left(ts_list, win_start)
        hi = bisect_right(ts_list, win_end)
        if lo < hi:
            return True
    return False


# -------------------------------------------------------------------------
# CSV 保存
# -------------------------------------------------------------------------
CSV_FIELDS = [
    "scene_id", "product_identifier", "acq_time_utc",
    "platform", "relative_orbit", "orbit_direction",
    "bbox_min_lon", "bbox_min_lat", "bbox_max_lon", "bbox_max_lat",
]


def save_matches_csv(matches: list, csv_path: Path, mode: str = "w") -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = (mode == "w") or not csv_path.exists()
    with open(csv_path, mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            w.writeheader()
        for item in matches:
            props = item.get("properties", {})
            geom  = item.get("geometry", {})
            try:
                bounds = shapely_shape(geom).bounds
            except Exception:
                bounds = (None, None, None, None)
            pid = (
                props.get("s1:product_identifier")
                or props.get("productIdentifier")
                or props.get("identifier")
                or item.get("id", "")
            )
            w.writerow({
                "scene_id":         item.get("id", ""),
                "product_identifier": pid,
                "acq_time_utc":     props.get("datetime", "")[:19],
                "platform":         props.get("platform", ""),
                "relative_orbit":   props.get("sat:relative_orbit", ""),
                "orbit_direction":  props.get("sat:orbit_state", ""),
                "bbox_min_lon":     bounds[0],
                "bbox_min_lat":     bounds[1],
                "bbox_max_lon":     bounds[2],
                "bbox_max_lat":     bounds[3],
            })


# -------------------------------------------------------------------------
# ダウンロード
# -------------------------------------------------------------------------
def get_uuid(session, product_name: str) -> str | None:
    name = product_name.removesuffix("_COG").removesuffix(".SAFE")
    for filt in [f"Name eq '{name}.SAFE'", f"contains(Name, '{name}')"]:
        try:
            r = session.get(CDSE_ODATA_URL, params={"$filter": filt, "$top": 1},
                            headers=hdr(session), timeout=30)
            r.raise_for_status()
            vals = r.json().get("value", [])
            if vals:
                return vals[0]["Id"]
        except Exception:
            pass
    return None


def download_scene(session, product_name: str, out_dir: Path) -> Path | None:
    name = product_name.removesuffix("_COG").removesuffix(".SAFE")
    out_zip = out_dir / f"{name}.zip"
    if out_zip.exists() and out_zip.stat().st_size > 0:
        logger.info(f"  スキップ(既存): {out_zip.name}")
        return out_zip
    uuid = get_uuid(session, name)
    if not uuid:
        logger.error(f"  UUID 未発見: {name}")
        return None
    url = CDSE_DL_URL.format(uuid=uuid)
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_zip.with_suffix(".part")
    try:
        r = session.get(url, headers=hdr(session), stream=True, timeout=120)
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        done = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
                    done += len(chunk)
                    if total:
                        print(f"\r  {done/1e6:.0f}/{total/1e6:.0f} MB ({done/total*100:.1f}%)",
                              end="", flush=True)
        print()
        tmp.rename(out_zip)
        logger.info(f"  完了: {out_zip.name}")
        return out_zip
    except Exception as e:
        logger.error(f"  失敗: {e}")
        if tmp.exists():
            tmp.unlink()
        return None


# -------------------------------------------------------------------------
# SNAP GPT 前処理
# -------------------------------------------------------------------------
def run_gpt(zip_path: Path, out_tif: Path) -> bool:
    if out_tif.exists() and out_tif.stat().st_size > 0:
        logger.info(f"  スキップ(処理済): {out_tif.name}")
        return True
    cmd = [
        GPT_EXE, str(GRAPH_XML),
        f"-PsourceFile={zip_path}",
        f"-PtargetFile={out_tif.with_suffix('')}",
        "-J-Xmx16G", "-c", "8G", "-e", "-q", "4",
    ]
    logger.info(f"  GPT: {zip_path.stem[:55]}")
    res = subprocess.run(cmd, cwd=str(Path(__file__).parent.parent))
    ok = res.returncode == 0
    logger.info(f"  {'[OK]' if ok else '[FAIL]'} {out_tif.name}")
    return ok


# -------------------------------------------------------------------------
# メイン
# -------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="CSV 作成のみ。ダウンロード・前処理はしない。")
    parser.add_argument("--skip-stac", action="store_true",
                        help="STAC 検索をスキップし、既存の matched_scenes.csv を使う。")
    args = parser.parse_args()

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # ---- ステップ1: 浸水グリッド ----
    logger.info("=== Step1: 浸水グリッドセル抽出 ===")
    grid_cells = extract_inundated_grid_cells()
    if not grid_cells:
        logger.error("浸水グリッドなし。終了。")
        return

    # ---- ステップ2: 降雨イベント ----
    logger.info("=== Step2: 降雨データ取得 (2014-2026) ===")
    rain_map = fetch_rain_events_all_years(grid_cells)
    if not rain_map:
        logger.warning("降雨イベントなし。終了。")
        return

    # ---- ステップ3&4: STAC + 雨後フィルタ ----
    all_matched: list = []

    if args.skip_stac and MATCH_CSV.exists():
        logger.info(f"=== Step3: STAC スキップ → {MATCH_CSV} を使用 ===")
        with open(MATCH_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_matched.append(row)
        logger.info(f"既存マッチシーン: {len(all_matched)} 件")

    else:
        logger.info("=== Step3&4: 四半期STAC検索 + 雨後フィルタ ===")
        session = requests.Session()
        get_token(session)
        logger.info("  CDSE 認証成功")

        quarters = list(quarter_ranges())
        logger.info(f"  四半期数: {len(quarters)}")

        # 既存 CSV があればスキップ済み四半期を判定
        existing_acq = set()
        if MATCH_CSV.exists():
            with open(MATCH_CSV, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    existing_acq.add(row["scene_id"])
            logger.info(f"  既存CSV: {len(existing_acq)} シーン (スキップ済み)")

        csv_mode = "a" if MATCH_CSV.exists() else "w"
        total_searched = 0
        quarter_matched_items = []

        for qi, (q_start, q_end) in enumerate(quarters, 1):
            label = f"{q_start.strftime('%Y-Q')}{ (q_start.month-1)//3+1 }"
            logger.info(f"  [{qi:02d}/{len(quarters)}] {label}: {q_start.date()} ~ {q_end.date()}")

            items = stac_search_period(session, q_start, q_end)
            total_searched += len(items)
            logger.info(f"    取得: {len(items)} シーン")

            new_matches = []
            for item in items:
                if item.get("id") in existing_acq:
                    continue
                if is_rain_after(item, rain_map):
                    new_matches.append(item)

            if new_matches:
                logger.info(f"    マッチ: {len(new_matches)} シーン → CSV 追記")
                save_matches_csv(new_matches, MATCH_CSV, mode=csv_mode)
                csv_mode = "a"
                for item in new_matches:
                    props = item.get("properties", {})
                    existing_acq.add(item.get("id"))
                    all_matched.append({
                        "scene_id": item.get("id", ""),
                        "product_identifier": (
                            props.get("s1:product_identifier")
                            or props.get("identifier")
                            or item.get("id", "")
                        ),
                        "acq_time_utc": props.get("datetime", "")[:19],
                    })

            time.sleep(0.5)

        logger.info(f"\nSTAC 検索完了: {total_searched} シーン検索 → {len(all_matched)} 件マッチ")
        logger.info(f"マッチ結果: {MATCH_CSV}")

    if not all_matched:
        logger.warning("マッチシーンが 0 件。終了。")
        return

    if args.dry_run:
        logger.info("--dry-run: ダウンロード・前処理をスキップ。")
        return

    # ---- ステップ5: ダウンロード ----
    logger.info(f"\n=== Step5: ダウンロード ({len(all_matched)} シーン) ===")
    session = requests.Session()
    get_token(session)

    downloaded = []
    for i, row in enumerate(sorted(all_matched, key=lambda x: x.get("acq_time_utc", "")), 1):
        pid = row.get("product_identifier") or row.get("scene_id", "")
        logger.info(f"[{i}/{len(all_matched)}] {pid[:65]}  {row.get('acq_time_utc','')}")
        zp = download_scene(session, pid, DOWNLOAD_DIR)
        if zp:
            downloaded.append(zp)
        time.sleep(1)

    logger.info(f"\nダウンロード: {len(downloaded)}/{len(all_matched)} 完了")

    # ---- ステップ6: SNAP GPT 前処理 ----
    logger.info(f"\n=== Step6: SNAP GPT 前処理 ({len(downloaded)} シーン) ===")
    ok = fail = 0
    for i, zp in enumerate(downloaded, 1):
        out_tif = PROCESSED_DIR / f"{zp.stem}_proc.tif"
        logger.info(f"[{i}/{len(downloaded)}]")
        if run_gpt(zp, out_tif):
            ok += 1
        else:
            fail += 1

    logger.info(f"\n=== 完了 ===")
    logger.info(f"  前処理成功: {ok} / 失敗: {fail}")
    logger.info(f"  出力: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
