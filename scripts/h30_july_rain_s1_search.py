#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
h30_july_rain_s1_search.py

平成30年7月豪雨（2018-07-05〜07-09 JST）を対象に
1. gsmap_points からグリッド別連続降雨イベントを抽出
2. 各イベント終了後 0〜3 時間以内に取得された Sentinel-1 GRD シーンを CDSE STAC で検索
3. 結果を CSV / PNG で出力

検索の効率化戦略:
- 日本全体の bbox で時間帯ごとに一括検索 → grids とのマッチングはメモリ上で実施
- 個々のグリッドごとに API コールを立てることを回避
"""

from __future__ import annotations

import os
import sys
import time
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import numpy as np
import pandas as pd
import pymysql
import requests
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from shapely.geometry import shape, Point

# ─────────────────────────────────────────────────────────────────────────────
# 設定
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DB_CONFIG = dict(host="127.0.0.1", port=3307, user="rainsar", password="rainsar_pw", db="rainsar_hub")

OUTPUT_DIR = "output/h30_july"

# H30.7月豪雨 JST → UTC
EVENT_START_JST = "2018-07-05 00:00:00"
EVENT_END_JST   = "2018-07-09 23:59:59"
# UTC
EVENT_START_UTC = datetime(2018, 7, 4, 15, 0, 0, tzinfo=timezone.utc)  # 2018-07-05 00:00 JST
EVENT_END_UTC   = datetime(2018, 7, 9, 14, 59, 59, tzinfo=timezone.utc) # 2018-07-09 23:59 JST

# 降雨イベント定義
THRESHOLD_MM_H  = 4.0   # ヒット判定の最低雨量 mm/h
MIN_HIT_HOURS   = 2     # イベントとして認める最低継続時間 (h)
MIN_MAX_MM_H    = 10.0  # 記録的豪雨に絞る: 最大時間雨量 >= 10 mm/h

# S1 検索
S1_AFTER_HOURS  = 3     # イベント終了後この時間以内の S1 を探す

# CDSE 認証 (.env から読む)
CDSE_TOKEN_URL  = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CDSE_STAC_URL   = "https://stac.dataspace.copernicus.eu/v1/search"

# Japan bbox
JAPAN_BBOX = (122.0, 24.0, 150.0, 46.0)  # min_lon, min_lat, max_lon, max_lat

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
CDSE_CLIENT_ID     = ENV.get("CDSE_CLIENT_ID", "")
CDSE_CLIENT_SECRET = ENV.get("CDSE_CLIENT_SECRET", "")

# ─────────────────────────────────────────────────────────────────────────────
# データクラス
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class RainEvent:
    grid_id: str
    lat: float
    lon: float
    start_utc: datetime
    end_utc: datetime
    hit_hours: int
    max_mm_h: float
    sum_mm: float
    mean_mm_h: float


@dataclass
class S1Scene:
    stac_id: str
    product_id: Optional[str]
    acquisition_time: datetime
    orbit_direction: Optional[str]
    relative_orbit: Optional[int]
    platform: Optional[str]
    geometry: Dict[str, Any]


# ─────────────────────────────────────────────────────────────────────────────
# Step 1: gsmap_points → 降雨イベント抽出
# ─────────────────────────────────────────────────────────────────────────────
def _make_grid_id(lat: float, lon: float) -> str:
    """lat/lon から一意のグリッドIDを生成（gsmap_points.grid_id がNULLのため代用）"""
    return f"{lat:.2f}_{lon:.2f}"


def extract_rain_events() -> List[RainEvent]:
    """
    gsmap_points を Japan bbox でフィルタし、
    (lat, lon) をグリッドIDとして連続降雨イベントを抽出する。
    japan_grids テーブルが空のため bbox フィルタを使用。
    """
    logger.info("Step 1: gsmap_points から降雨イベントを抽出中...")

    # japan_grids が空のため、bbox + region で日本を絞り込む
    sql = """
        SELECT
            lat,
            lon,
            ts_utc,
            gauge_mm_h
        FROM gsmap_points
        WHERE ts_utc >= %(start)s
          AND ts_utc <= %(end)s
          AND gauge_mm_h >= %(thr)s
          AND lat BETWEEN 24.0 AND 46.0
          AND lon BETWEEN 122.0 AND 150.0
        ORDER BY lat, lon, ts_utc
    """
    conn = pymysql.connect(**DB_CONFIG)
    try:
        df = pd.read_sql(sql, conn, params={
            "start": EVENT_START_UTC.strftime("%Y-%m-%d %H:%M:%S"),
            "end":   EVENT_END_UTC.strftime("%Y-%m-%d %H:%M:%S"),
            "thr":   THRESHOLD_MM_H,
        })
    finally:
        conn.close()

    logger.info(f"  取得行数: {len(df):,}")
    if df.empty:
        return []

    # ts_utc を timezone-aware datetime に
    df["ts_utc"] = pd.to_datetime(df["ts_utc"]).dt.tz_localize("UTC")
    # グリッドID を lat/lon から生成
    df["grid_id"] = df.apply(lambda r: _make_grid_id(r["lat"], r["lon"]), axis=1)

    events: List[RainEvent] = []
    cur_grid = None
    cur_lat = cur_lon = 0.0
    cur_start = cur_end = None
    cur_max = cur_sum = 0.0
    cur_hours = 0

    def flush():
        if cur_hours < MIN_HIT_HOURS:
            return
        events.append(RainEvent(
            grid_id=cur_grid, lat=cur_lat, lon=cur_lon,
            start_utc=cur_start, end_utc=cur_end,
            hit_hours=cur_hours, max_mm_h=cur_max, sum_mm=cur_sum,
            mean_mm_h=cur_sum / cur_hours,
        ))

    for _, row in df.iterrows():
        gid   = row["grid_id"]
        lat   = float(row["lat"])
        lon   = float(row["lon"])
        ts    = row["ts_utc"]
        gauge = float(row["gauge_mm_h"])

        if cur_grid is None:
            cur_grid, cur_lat, cur_lon = gid, lat, lon
            cur_start = cur_end = ts
            cur_max, cur_sum, cur_hours = gauge, gauge, 1
            continue

        same = (gid == cur_grid)
        gap  = (ts - cur_end).total_seconds() / 3600.0 if same else None

        if same and gap is not None and gap <= 1.01:
            cur_end    = ts
            cur_hours += 1
            cur_sum   += gauge
            if gauge > cur_max:
                cur_max = gauge
        else:
            flush()
            cur_grid, cur_lat, cur_lon = gid, lat, lon
            cur_start = cur_end = ts
            cur_max, cur_sum, cur_hours = gauge, gauge, 1

    flush()

    # 記録的豪雨フィルタ
    events = [e for e in events if e.max_mm_h >= MIN_MAX_MM_H]
    logger.info(f"  抽出イベント数: {len(events):,}  (max_mm_h≥{MIN_MAX_MM_H}mm/h, 継続≥{MIN_HIT_HOURS}h)")
    return events


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: CDSE STAC で S1 一括検索
# ─────────────────────────────────────────────────────────────────────────────
_ACCESS_TOKEN: Optional[str] = None
_TOKEN_EXPIRE: datetime = datetime.min.replace(tzinfo=timezone.utc)

def _get_token(session: requests.Session) -> str:
    global _ACCESS_TOKEN, _TOKEN_EXPIRE
    now = datetime.now(timezone.utc)
    if _ACCESS_TOKEN and now < _TOKEN_EXPIRE:
        return _ACCESS_TOKEN
    if not CDSE_CLIENT_ID or not CDSE_CLIENT_SECRET:
        return ""
    try:
        resp = session.post(CDSE_TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": CDSE_CLIENT_ID,
            "client_secret": CDSE_CLIENT_SECRET,
        }, timeout=30)
        resp.raise_for_status()
        j = resp.json()
        _ACCESS_TOKEN = j["access_token"]
        _TOKEN_EXPIRE = now + timedelta(seconds=int(j.get("expires_in", 3600)) - 60)
        return _ACCESS_TOKEN
    except Exception as e:
        logger.error(f"Token error: {e}")
        return ""


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


def stac_search_bbox(session: requests.Session, start: datetime, end: datetime,
                     bbox: Tuple[float,float,float,float], limit: int = 200) -> List[S1Scene]:
    """日本全域 bbox で Sentinel-1 GRD を一括検索"""
    min_lon, min_lat, max_lon, max_lat = bbox
    token = _get_token(session)
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    params = {
        "collections": "sentinel-1-grd",
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "datetime": (f"{start.isoformat().replace('+00:00','Z')}/"
                     f"{end.isoformat().replace('+00:00','Z')}"),
        "limit": limit,
    }

    for attempt in range(6):
        try:
            resp = session.get(CDSE_STAC_URL, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            scenes = []
            for feat in data.get("features", []):
                props = feat.get("properties", {}) or {}
                dt_str = props.get("datetime") or props.get("start_datetime")
                if not dt_str:
                    continue
                scenes.append(S1Scene(
                    stac_id=feat.get("id", ""),
                    product_id=(props.get("s1:product_identifier")
                                or props.get("productIdentifier")
                                or props.get("identifier")),
                    acquisition_time=_parse_dt(dt_str),
                    orbit_direction=(props.get("sat:orbit_state")
                                     or props.get("s1:orbitDirection")),
                    relative_orbit=(props.get("s1:relativeOrbitNumber")
                                    or props.get("sat:relative_orbit")),
                    platform=props.get("platform") or props.get("platformSerialIdentifier"),
                    geometry=feat.get("geometry") or {},
                ))
            return scenes
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = 2.0 * (2 ** attempt)
                logger.warning(f"429 Too Many Requests, wait {wait:.0f}s...")
                time.sleep(wait)
            else:
                logger.error(f"HTTP error: {e}")
                return []
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []
    return []


def search_s1_for_events(events: List[RainEvent]) -> Tuple[List[S1Scene], List[str]]:
    """
    イベント期間中 + 終了後 S1_AFTER_HOURS の全 S1 シーンを一括取得。
    1時間ごとのスライディングウィンドウで検索し、重複排除する。
    """
    logger.info("Step 2: CDSE STAC で Sentinel-1 を一括検索中...")

    if not events:
        return [], []

    # 検索範囲: 最初のイベント開始 〜 最後のイベント終了 + 3h
    search_start = min(e.start_utc for e in events)
    search_end   = max(e.end_utc   for e in events) + timedelta(hours=S1_AFTER_HOURS)

    logger.info(f"  検索範囲 (UTC): {search_start} → {search_end}")

    session = requests.Session()
    all_scenes_map: Dict[str, S1Scene] = {}  # stac_id → S1Scene (重複排除)

    # API limit=200 の上限があるため、1日ごとに分割して検索
    cur = search_start
    while cur < search_end:
        next_t = min(cur + timedelta(days=1), search_end)
        logger.info(f"  検索: {cur.date()} ~ {next_t.date()}")
        scenes = stac_search_bbox(session, cur, next_t, JAPAN_BBOX)
        for s in scenes:
            all_scenes_map[s.stac_id] = s
        logger.info(f"    → {len(scenes)} シーン取得 (累計 {len(all_scenes_map)})")
        time.sleep(0.5)
        cur = next_t

    all_scenes = list(all_scenes_map.values())
    all_scenes.sort(key=lambda s: s.acquisition_time)
    logger.info(f"  合計シーン数 (重複排除済): {len(all_scenes)}")

    # S1 シーンのフットプリント geometry を Shapely で保持（マッチング用）
    scene_shapes = {}
    for s in all_scenes:
        try:
            scene_shapes[s.stac_id] = shape(s.geometry)
        except Exception:
            scene_shapes[s.stac_id] = None

    return all_scenes, scene_shapes


# ─────────────────────────────────────────────────────────────────────────────
# Step 3: イベント × S1 マッチング
# ─────────────────────────────────────────────────────────────────────────────
def match_events_to_s1(events: List[RainEvent],
                       all_scenes: List[S1Scene],
                       scene_shapes: Dict) -> pd.DataFrame:
    """
    各降雨イベントに対して、終了後 0〜S1_AFTER_HOURS 時間以内に
    取得された S1 シーンを対応づける。
    """
    logger.info("Step 3: イベント × S1 マッチング中...")

    records = []
    for ev in events:
        window_start = ev.end_utc
        window_end   = ev.end_utc + timedelta(hours=S1_AFTER_HOURS)

        point = Point(ev.lon, ev.lat)

        matched = []
        for s in all_scenes:
            acq = s.acquisition_time
            if not (window_start <= acq <= window_end):
                continue
            # フットプリントチェック
            geom = scene_shapes.get(s.stac_id)
            if geom is not None and not geom.is_empty:
                if not geom.contains(point) and not geom.intersects(point.buffer(0.2)):
                    continue
            delay_h = (acq - ev.end_utc).total_seconds() / 3600.0
            matched.append((delay_h, s))

        if not matched:
            # マッチなしの場合も記録
            records.append({
                "grid_id": ev.grid_id,
                "lat": ev.lat,
                "lon": ev.lon,
                "event_start_jst": (ev.start_utc + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M"),
                "event_end_jst":   (ev.end_utc   + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M"),
                "hit_hours": ev.hit_hours,
                "max_mm_h": ev.max_mm_h,
                "sum_mm": round(ev.sum_mm, 1),
                "mean_mm_h": round(ev.mean_mm_h, 2),
                "s1_matched": False,
                "delay_h": None,
                "s1_stac_id": None,
                "s1_product_id": None,
                "s1_acq_jst": None,
                "s1_platform": None,
                "s1_orbit_dir": None,
                "s1_rel_orbit": None,
            })
        else:
            for delay_h, s in sorted(matched, key=lambda x: x[0]):
                acq_jst = (s.acquisition_time + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M")
                records.append({
                    "grid_id": ev.grid_id,
                    "lat": ev.lat,
                    "lon": ev.lon,
                    "event_start_jst": (ev.start_utc + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M"),
                    "event_end_jst":   (ev.end_utc   + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M"),
                    "hit_hours": ev.hit_hours,
                    "max_mm_h": ev.max_mm_h,
                    "sum_mm": round(ev.sum_mm, 1),
                    "mean_mm_h": round(ev.mean_mm_h, 2),
                    "s1_matched": True,
                    "delay_h": round(delay_h, 2),
                    "s1_stac_id": s.stac_id,
                    "s1_product_id": s.product_id,
                    "s1_acq_jst": acq_jst,
                    "s1_platform": s.platform,
                    "s1_orbit_dir": s.orbit_direction,
                    "s1_rel_orbit": s.relative_orbit,
                })

    df = pd.DataFrame(records)
    n_matched = df["s1_matched"].sum() if not df.empty else 0
    logger.info(f"  マッチ成功: {n_matched} 件 / 全イベント: {len(events)} 件")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Step 4: 可視化
# ─────────────────────────────────────────────────────────────────────────────
def visualize(events: List[RainEvent], df_match: pd.DataFrame,
              all_scenes: List[S1Scene], scene_shapes: Dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── 図1: 降雨イベント分布 (max_mm_h) ──
    fig, ax = plt.subplots(figsize=(12, 10),
                           subplot_kw={"projection": ccrs.PlateCarree()})
    ax.set_extent([122, 150, 24, 46], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.OCEAN, facecolor="#d6eaf8", zorder=0)
    ax.add_feature(cfeature.LAND,  facecolor="#f5f5f5", zorder=1)
    ax.add_feature(cfeature.COASTLINE, linewidth=0.7, edgecolor="#555", zorder=2)
    ax.gridlines(draw_labels=True, linewidth=0.3, color="gray", alpha=0.5)

    lats = [e.lat for e in events]
    lons = [e.lon for e in events]
    vals = [e.max_mm_h for e in events]

    norm = mcolors.Normalize(vmin=0, vmax=100)
    sc = ax.scatter(lons, lats, c=vals, cmap="YlOrRd", norm=norm,
                    s=5, alpha=0.85, linewidths=0,
                    transform=ccrs.PlateCarree(), zorder=3)
    cbar = plt.colorbar(sc, ax=ax, shrink=0.7, pad=0.02)
    cbar.set_label("最大時間雨量 (mm/h)", fontsize=11)

    ax.set_title("平成30年7月豪雨 ─ 連続降雨イベント分布\n"
                 f"2018-07-05〜07-09 JST  閾値={THRESHOLD_MM_H}mm/h  "
                 f"最大雨量≥{MIN_MAX_MM_H}mm/h  継続≥{MIN_HIT_HOURS}h",
                 fontsize=12, fontweight="bold")

    fig.savefig(os.path.join(OUTPUT_DIR, "rain_events_map.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("  rain_events_map.png 保存")

    # ── 図2: S1 マッチ結果 (matched/not-matched) ──
    df_m = df_match[df_match["s1_matched"] == True].drop_duplicates("grid_id")
    df_nm = df_match[df_match["s1_matched"] == False].drop_duplicates("grid_id")

    fig, ax = plt.subplots(figsize=(12, 10),
                           subplot_kw={"projection": ccrs.PlateCarree()})
    ax.set_extent([122, 150, 24, 46], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.OCEAN, facecolor="#d6eaf8", zorder=0)
    ax.add_feature(cfeature.LAND,  facecolor="#f5f5f5", zorder=1)
    ax.add_feature(cfeature.COASTLINE, linewidth=0.7, edgecolor="#555", zorder=2)
    ax.gridlines(draw_labels=True, linewidth=0.3, color="gray", alpha=0.5)

    if not df_nm.empty:
        ax.scatter(df_nm["lon"], df_nm["lat"], c="#cccccc", s=4,
                   alpha=0.6, label=f"S1なし ({len(df_nm)}格子)",
                   transform=ccrs.PlateCarree(), zorder=3)
    if not df_m.empty:
        norm2 = mcolors.Normalize(vmin=0, vmax=S1_AFTER_HOURS)
        sc2 = ax.scatter(df_m["lon"], df_m["lat"],
                         c=df_m["delay_h"], cmap="cool",
                         norm=norm2, s=12, alpha=0.9,
                         label=f"S1あり ({len(df_m)}格子)",
                         transform=ccrs.PlateCarree(), zorder=4)
        cbar2 = plt.colorbar(sc2, ax=ax, shrink=0.6, pad=0.02)
        cbar2.set_label("イベント終了後の遅延 (h)", fontsize=11)

    # S1 フットプリント（期間内のもの）を薄く描画
    from matplotlib.patches import Polygon as MplPolygon
    from matplotlib.collections import PatchCollection
    plotted_ids = set()
    for s in all_scenes:
        if s.stac_id in plotted_ids:
            continue
        geom = scene_shapes.get(s.stac_id)
        if geom is None:
            continue
        try:
            coords = list(geom.exterior.coords)
            lons_s = [c[0] for c in coords]
            lats_s = [c[1] for c in coords]
            ax.plot(lons_s, lats_s, color="steelblue", linewidth=0.5,
                    alpha=0.25, transform=ccrs.PlateCarree(), zorder=2)
            plotted_ids.add(s.stac_id)
        except Exception:
            pass

    ax.legend(loc="lower left", fontsize=9, framealpha=0.8)
    ax.set_title(f"平成30年7月豪雨 ─ S1 マッチング結果 (イベント終了後 0〜{S1_AFTER_HOURS}h)\n"
                 f"2018-07-05〜07-09 JST  合計S1シーン: {len(all_scenes)}",
                 fontsize=12, fontweight="bold")

    fig.savefig(os.path.join(OUTPUT_DIR, "s1_match_map.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("  s1_match_map.png 保存")

    # ── 図3: delay_h ヒストグラム ──
    if not df_m.empty:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(df_m["delay_h"].dropna(), bins=np.arange(0, S1_AFTER_HOURS + 0.25, 0.25),
                color="steelblue", edgecolor="white", alpha=0.85)
        ax.set_xlabel("イベント終了 → S1 取得までの時間 (h)", fontsize=12)
        ax.set_ylabel("格子数", fontsize=12)
        ax.set_title("S1 遅延時間分布 (平成30年7月豪雨)", fontsize=13, fontweight="bold")
        ax.grid(axis="y", alpha=0.4)
        fig.tight_layout()
        fig.savefig(os.path.join(OUTPUT_DIR, "delay_histogram.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)
        logger.info("  delay_histogram.png 保存")

    # ── 図4: S1 シーンカバレッジ時系列 ──
    if all_scenes:
        acq_times = sorted(s.acquisition_time + timedelta(hours=9) for s in all_scenes)
        acq_dates  = [t.strftime("%m/%d %H:%M") for t in acq_times]
        platforms  = [s.platform or "?" for s in sorted(all_scenes, key=lambda x: x.acquisition_time)]
        orbits     = [s.orbit_direction or "?" for s in sorted(all_scenes, key=lambda x: x.acquisition_time)]

        fig, ax = plt.subplots(figsize=(14, 5))
        colors = {"ascending": "#e67e22", "descending": "#2980b9"}
        for i, (s, t) in enumerate(zip(sorted(all_scenes, key=lambda x: x.acquisition_time), acq_times)):
            c = colors.get((s.orbit_direction or "").lower(), "#7f8c8d")
            ax.bar(i, 1, color=c, alpha=0.8, edgecolor="none")

        ax.set_xticks(range(len(acq_dates)))
        ax.set_xticklabels(acq_dates, rotation=60, ha="right", fontsize=7)
        ax.set_yticks([])
        ax.set_ylabel("S1 シーン")
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor="#e67e22", label="Ascending"),
                           Patch(facecolor="#2980b9", label="Descending"),
                           Patch(facecolor="#7f8c8d", label="不明")]
        ax.legend(handles=legend_elements, loc="upper right")
        ax.set_title(f"S1 シーン取得時刻 (JST) ─ 期間内 計{len(all_scenes)}シーン", fontweight="bold")

        # 豪雨期間帯をシェーディング
        ax.axvline(x=-0.5, color="red", linewidth=0, label="")
        ax.set_xlim(-0.5, len(acq_dates) - 0.5)
        ax.grid(axis="x", linewidth=0.3, alpha=0.4)
        fig.tight_layout()
        fig.savefig(os.path.join(OUTPUT_DIR, "s1_timeline.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)
        logger.info("  s1_timeline.png 保存")


# ─────────────────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────────────────
def main():
    # 日本語フォント
    for fname in ["Yu Gothic", "Meiryo", "MS Gothic"]:
        try:
            plt.rcParams["font.family"] = fname
            break
        except Exception:
            pass

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Step 1: 降雨イベント抽出 ──
    events = extract_rain_events()
    if not events:
        logger.error("イベントが0件。終了します。")
        return

    # イベント CSV 保存
    df_events = pd.DataFrame([{
        "grid_id": e.grid_id,
        "lat": e.lat,
        "lon": e.lon,
        "start_jst": (e.start_utc + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M"),
        "end_jst":   (e.end_utc   + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M"),
        "hit_hours": e.hit_hours,
        "max_mm_h": e.max_mm_h,
        "sum_mm": round(e.sum_mm, 1),
        "mean_mm_h": round(e.mean_mm_h, 2),
    } for e in events])

    events_csv = os.path.join(OUTPUT_DIR, "rain_events.csv")
    df_events.to_csv(events_csv, index=False, encoding="utf-8-sig")
    logger.info(f"  イベントCSV: {events_csv}  ({len(df_events)}行)")

    # イベントの統計サマリ
    logger.info(f"\n=== 降雨イベントサマリ ===")
    logger.info(f"  総イベント数: {len(events)}")
    logger.info(f"  ユニーク格子数: {df_events['grid_id'].nunique()}")
    logger.info(f"  最大時間雨量: {df_events['max_mm_h'].max():.1f} mm/h")
    logger.info(f"  最長継続時間: {df_events['hit_hours'].max()} h")
    logger.info(f"  継続時間分布:\n{df_events['hit_hours'].describe()}")

    # ── Step 2: S1 一括検索 ──
    all_scenes, scene_shapes = search_s1_for_events(events)

    # S1 シーン CSV
    df_scenes = pd.DataFrame([{
        "stac_id": s.stac_id,
        "product_id": s.product_id,
        "acq_utc": s.acquisition_time.strftime("%Y-%m-%d %H:%M:%S"),
        "acq_jst": (s.acquisition_time + timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S"),
        "platform": s.platform,
        "orbit_dir": s.orbit_direction,
        "rel_orbit": s.relative_orbit,
    } for s in all_scenes])

    scenes_csv = os.path.join(OUTPUT_DIR, "s1_scenes.csv")
    df_scenes.to_csv(scenes_csv, index=False, encoding="utf-8-sig")
    logger.info(f"  S1シーンCSV: {scenes_csv}")

    # ── Step 3: マッチング ──
    df_match = match_events_to_s1(events, all_scenes, scene_shapes)
    match_csv = os.path.join(OUTPUT_DIR, "event_s1_match.csv")
    df_match.to_csv(match_csv, index=False, encoding="utf-8-sig")
    logger.info(f"  マッチングCSV: {match_csv}")

    # マッチング統計
    n_total  = df_match["grid_id"].nunique()
    n_hit    = df_match[df_match["s1_matched"]]["grid_id"].nunique()
    logger.info(f"\n=== S1 マッチングサマリ ===")
    logger.info(f"  イベントのあった格子: {n_total}")
    logger.info(f"  S1 マッチあり格子:    {n_hit} ({100*n_hit/max(n_total,1):.1f}%)")
    if not df_match[df_match["s1_matched"]].empty:
        delays = df_match[df_match["s1_matched"]]["delay_h"].dropna()
        logger.info(f"  遅延時間 (h) 統計:\n{delays.describe()}")

    # ── Step 4: 可視化 ──
    logger.info("\nStep 4: 可視化中...")
    visualize(events, df_match, all_scenes, scene_shapes)

    logger.info(f"\n完了！  出力先: {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
