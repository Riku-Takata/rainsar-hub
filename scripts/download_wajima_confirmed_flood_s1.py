#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Download Sentinel-1 scenes around confirmed Wajima flood polygons.

The script:
1. reads confirmed flood polygons,
2. builds rainfall events from gsmap_points because gsmap_events may be empty,
3. searches Sentinel-1 GRD scenes acquired within 24 hours after rainfall end,
4. finds a matching before-rain scene with the same orbit key,
5. downloads missing products through the existing CDSE client, and
6. writes metadata including elapsed hours after rainfall.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zipfile import BadZipFile, ZipFile

import geopandas as gpd
import pymysql
from shapely.geometry import mapping


ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env_file(BACKEND_DIR / ".env")

from app.services.s1_cdse_client import S1CDSEClient, S1Scene  # noqa: E402


LOGGER = logging.getLogger("wajima_confirmed_flood_s1")
UTC = timezone.utc
JST = timezone(timedelta(hours=9))


DEFAULT_TRUTH_PATH = Path(r"D:\shuron\GT-data\sinsuiiki\shinsui.shp")
DEFAULT_OUTPUT_DIR = Path(r"D:\shuron\downloads")
DEFAULT_DOWNLOAD_DIR = Path(r"D:\shuron\downloads")
DEFAULT_SAFE_DIR = ROOT_DIR / "data" / "final" / "SAFE"
DEFAULT_RAIN_START_UTC = None
DEFAULT_RAIN_END_UTC = None


@dataclass(frozen=True)
class RainEvent:
    event_id: str
    grid_lat: float
    grid_lon: float
    start_utc: datetime
    end_utc: datetime
    hit_hours: int
    max_gauge_mm_h: float
    mean_gauge_mm_h: float
    sum_gauge_mm: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--truth-path", type=Path, default=DEFAULT_TRUTH_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--download-dir", type=Path, default=DEFAULT_DOWNLOAD_DIR)
    parser.add_argument("--safe-dir", type=Path, default=DEFAULT_SAFE_DIR)
    parser.add_argument("--db-host", default="127.0.0.1")
    parser.add_argument("--db-port", type=int, default=3307)
    parser.add_argument("--db-user", default=os.environ.get("DB_USER", "rainsar"))
    parser.add_argument("--db-password", default=os.environ.get("DB_PASSWORD", "rainsar_pw"))
    parser.add_argument("--db-name", default=os.environ.get("DB_NAME", "rainsar_hub"))
    parser.add_argument("--rain-min-mm-h", type=float, default=10.0)
    parser.add_argument("--rain-max-mm-h", type=float, default=30.0)
    parser.add_argument("--rain-start-utc", default=DEFAULT_RAIN_START_UTC)
    parser.add_argument("--rain-end-utc", default=DEFAULT_RAIN_END_UTC)
    parser.add_argument("--aoi-buffer-deg", type=float, default=0.08)
    parser.add_argument("--rain-search-pad-deg", type=float, default=0.10)
    parser.add_argument("--after-hours", type=float, default=24.0)
    parser.add_argument("--before-lookback-days", type=int, default=180)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-events", type=int, default=0, help="0 means all rainfall events.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def ensure_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    return ensure_utc(datetime.fromisoformat(value.strip().replace("Z", "+00:00")))


def scene_product_name(scene: S1Scene) -> str:
    return scene.product_identifier or scene.stac_id


def normalize_product_name(name: str) -> str:
    return name.removesuffix("_COG").removesuffix(".SAFE")


def scene_match_key(scene: S1Scene) -> tuple[str, str, str, str]:
    return (
        scene.platform or "",
        scene.orbit_direction or "",
        str(scene.relative_orbit or ""),
        scene.product_type or "",
    )


def local_safe_path(product_name: str, safe_dir: Path) -> Path | None:
    normalized = normalize_product_name(product_name)
    exact = safe_dir / f"{normalized}.zip"
    if exact.exists() and exact.stat().st_size > 0:
        return exact
    matches = sorted(safe_dir.glob(f"{normalized}*.zip"))
    for path in matches:
        if path.exists() and path.stat().st_size > 0:
            return path
    return None


def is_valid_zip(path: Path) -> bool:
    try:
        with ZipFile(path) as zf:
            return zf.testzip() is None
    except BadZipFile:
        return False


def download_or_reuse(
    client: S1CDSEClient,
    product_name: str,
    output_dir: Path,
    safe_dir: Path,
    dry_run: bool,
) -> tuple[bool, str, str]:
    local = local_safe_path(product_name, safe_dir)
    if local is not None:
        valid = is_valid_zip(local)
        return valid, str(local), "existing_safe" if valid else "existing_corrupt"

    normalized = normalize_product_name(product_name)
    expected = output_dir / f"{normalized}.zip"
    if expected.exists() and expected.stat().st_size > 0:
        valid = is_valid_zip(expected)
        return valid, str(expected), "existing_output" if valid else "existing_corrupt"

    if dry_run:
        return False, "", "dry_run"

    saved = client.download_product(product_name, output_dir)
    if saved is None:
        return False, "", "download_failed"
    return True, str(saved), "downloaded"


def load_truth_bbox(truth_path: Path, buffer_deg: float) -> tuple[tuple[float, float, float, float], dict[str, Any]]:
    truth = gpd.read_file(truth_path)
    if truth.crs is None:
        raise RuntimeError(f"Truth file has no CRS: {truth_path}")
    truth = truth[truth.geometry.notna() & ~truth.geometry.is_empty].to_crs("EPSG:4326")
    geom = truth.union_all()
    min_lon, min_lat, max_lon, max_lat = geom.bounds
    bbox = (
        float(min_lon - buffer_deg),
        float(min_lat - buffer_deg),
        float(max_lon + buffer_deg),
        float(max_lat + buffer_deg),
    )
    return bbox, mapping(geom)


def build_rain_events_from_points(
    *,
    conn: pymysql.connections.Connection,
    bbox: tuple[float, float, float, float],
    pad_deg: float,
    rain_min: float,
    rain_max: float,
    rain_start_utc: datetime | None,
    rain_end_utc: datetime | None,
    max_events: int,
) -> list[RainEvent]:
    min_lon, min_lat, max_lon, max_lat = bbox
    params: list[Any] = [
        min_lat - pad_deg,
        max_lat + pad_deg,
        min_lon - pad_deg,
        max_lon + pad_deg,
        rain_min,
        rain_max,
    ]
    time_where = ""
    if rain_start_utc is not None:
        time_where += " AND ts_utc >= %s"
        params.append(rain_start_utc.replace(tzinfo=None))
    if rain_end_utc is not None:
        time_where += " AND ts_utc < %s"
        params.append(rain_end_utc.replace(tzinfo=None))

    sql = f"""
        SELECT ts_utc, lat, lon, gauge_mm_h
        FROM gsmap_points
        WHERE lat BETWEEN %s AND %s
          AND lon BETWEEN %s AND %s
          AND gauge_mm_h BETWEEN %s AND %s
          {time_where}
        ORDER BY lat, lon, ts_utc
    """
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

    grouped: dict[tuple[float, float], list[tuple[datetime, float]]] = {}
    for ts_utc, lat, lon, gauge in rows:
        grouped.setdefault((float(lat), float(lon)), []).append((ensure_utc(ts_utc), float(gauge)))

    events: list[RainEvent] = []
    for (lat, lon), values in grouped.items():
        current: list[tuple[datetime, float]] = []
        for ts_utc, gauge in values:
            if not current:
                current = [(ts_utc, gauge)]
                continue
            prev_ts = current[-1][0]
            if ts_utc - prev_ts <= timedelta(hours=1):
                current.append((ts_utc, gauge))
            else:
                events.append(make_event(lat, lon, current))
                current = [(ts_utc, gauge)]
        if current:
            events.append(make_event(lat, lon, current))

    events.sort(key=lambda ev: (ev.max_gauge_mm_h, ev.hit_hours, ev.end_utc), reverse=True)
    if max_events and max_events > 0:
        return events[:max_events]
    return events


def make_event(lat: float, lon: float, values: list[tuple[datetime, float]]) -> RainEvent:
    gauges = [gauge for _ts, gauge in values]
    start = values[0][0]
    # GSMaP points are hourly values. Treat ts + 1h as the end of that hourly rain period.
    end = values[-1][0] + timedelta(hours=1)
    event_id = f"lat{lat:.2f}_lon{lon:.2f}_{start.strftime('%Y%m%dT%H%M')}_{end.strftime('%Y%m%dT%H%M')}"
    return RainEvent(
        event_id=event_id,
        grid_lat=lat,
        grid_lon=lon,
        start_utc=start,
        end_utc=end,
        hit_hours=len(values),
        max_gauge_mm_h=max(gauges),
        mean_gauge_mm_h=sum(gauges) / len(gauges),
        sum_gauge_mm=sum(gauges),
    )


def search_s1_bbox(
    client: S1CDSEClient,
    bbox: tuple[float, float, float, float],
    start: datetime,
    end: datetime,
    limit: int,
) -> list[S1Scene]:
    min_lon, min_lat, max_lon, max_lat = bbox
    LOGGER.info("CDSE search: %s .. %s", start.isoformat(), end.isoformat())
    return client.search_grd_bbox_time(
        min_lat=min_lat,
        max_lat=max_lat,
        min_lon=min_lon,
        max_lon=max_lon,
        start=start,
        end=end,
        limit=limit,
    )


def find_before_scene(
    client: S1CDSEClient,
    *,
    bbox: tuple[float, float, float, float],
    after_scene: S1Scene,
    event_start: datetime,
    lookback_days: int,
    limit: int,
) -> S1Scene | None:
    scenes = search_s1_bbox(
        client,
        bbox,
        start=event_start - timedelta(days=lookback_days),
        end=event_start - timedelta(seconds=1),
        limit=limit,
    )
    key = scene_match_key(after_scene)
    candidates = [scene for scene in scenes if scene.acquisition_time < event_start and scene_match_key(scene) == key]
    if not candidates:
        return None
    return max(candidates, key=lambda scene: scene.acquisition_time)


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.download_dir.mkdir(parents=True, exist_ok=True)
    rain_start_utc = parse_utc(args.rain_start_utc)
    rain_end_utc = parse_utc(args.rain_end_utc)

    bbox, truth_geometry = load_truth_bbox(args.truth_path, args.aoi_buffer_deg)
    LOGGER.info("AOI bbox: %.6f, %.6f, %.6f, %.6f", *bbox)
    LOGGER.info(
        "Rain event time filter: %s .. %s",
        rain_start_utc.isoformat() if rain_start_utc else "none",
        rain_end_utc.isoformat() if rain_end_utc else "none",
    )

    conn = pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
    )
    try:
        rain_events = build_rain_events_from_points(
            conn=conn,
            bbox=bbox,
            pad_deg=args.rain_search_pad_deg,
            rain_min=args.rain_min_mm_h,
            rain_max=args.rain_max_mm_h,
            rain_start_utc=rain_start_utc,
            rain_end_utc=rain_end_utc,
            max_events=args.max_events,
        )
    finally:
        conn.close()

    if not rain_events:
        LOGGER.warning("No rainfall events found.")
        write_csv([], args.output_dir / "wajima_s1_rain_pairs.csv")
        return 0

    client = S1CDSEClient()
    rows: list[dict[str, Any]] = []

    for index, event in enumerate(rain_events, 1):
        LOGGER.info(
            "[%d/%d] rain %s max=%.2f end=%s",
            index,
            len(rain_events),
            event.event_id,
            event.max_gauge_mm_h,
            event.end_utc.isoformat(),
        )
        try:
            after_scenes = search_s1_bbox(
                client,
                bbox,
                start=event.end_utc,
                end=event.end_utc + timedelta(hours=args.after_hours),
                limit=args.limit,
            )
        except Exception as exc:
            LOGGER.exception("After-scene search failed for %s: %s", event.event_id, exc)
            rows.append(base_row(event, bbox, truth_geometry, None, None, "", "", False, str(exc), "search_error"))
            write_csv(rows, args.output_dir / "wajima_s1_rain_pairs.csv")
            continue
        if not after_scenes:
            rows.append(base_row(event, bbox, truth_geometry, None, None, "", "", False, "", "no_after_scene"))
            write_csv(rows, args.output_dir / "wajima_s1_rain_pairs.csv")
            continue

        for after_scene in after_scenes:
            elapsed_h = (after_scene.acquisition_time - event.end_utc).total_seconds() / 3600.0
            try:
                before_scene = find_before_scene(
                    client,
                    bbox=bbox,
                    after_scene=after_scene,
                    event_start=event.start_utc,
                    lookback_days=args.before_lookback_days,
                    limit=args.limit,
                )
            except Exception as exc:
                LOGGER.exception("Before-scene search failed for %s: %s", after_scene.stac_id, exc)
                before_scene = None

            after_ok, after_path, after_status = download_or_reuse(
                client,
                scene_product_name(after_scene),
                args.download_dir,
                args.safe_dir,
                args.dry_run,
            )
            time.sleep(1)
            before_ok = False
            before_path = ""
            before_status = "no_before_scene"
            if before_scene is not None:
                before_ok, before_path, before_status = download_or_reuse(
                    client,
                    scene_product_name(before_scene),
                    args.download_dir,
                    args.safe_dir,
                    args.dry_run,
                )
                time.sleep(1)

            rows.append(
                base_row(
                    event,
                    bbox,
                    truth_geometry,
                    after_scene,
                    before_scene,
                    after_path,
                    before_path,
                    bool(after_ok and (before_scene is None or before_ok)),
                    f"after={after_status};before={before_status};elapsed_h={elapsed_h:.3f}",
                    "matched",
                )
            )
            write_csv(rows, args.output_dir / "wajima_s1_rain_pairs.csv")

    write_csv(rows, args.output_dir / "wajima_s1_rain_pairs.csv")
    write_report_clean(args.output_dir, args, bbox, rain_events, rows)
    LOGGER.info("result csv: %s", args.output_dir / "wajima_s1_rain_pairs.csv")
    return 0


def base_row(
    event: RainEvent,
    bbox: tuple[float, float, float, float],
    truth_geometry: dict[str, Any],
    after_scene: S1Scene | None,
    before_scene: S1Scene | None,
    after_path: str,
    before_path: str,
    downloaded_or_existing: bool,
    note: str,
    status: str,
) -> dict[str, Any]:
    elapsed_h = ""
    if after_scene is not None:
        elapsed_h = (after_scene.acquisition_time - event.end_utc).total_seconds() / 3600.0
    return {
        "status": status,
        "event_id": event.event_id,
        "grid_lat": event.grid_lat,
        "grid_lon": event.grid_lon,
        "rain_start_utc": event.start_utc.isoformat(),
        "rain_end_utc": event.end_utc.isoformat(),
        "rain_start_jst": event.start_utc.astimezone(JST).isoformat(),
        "rain_end_jst": event.end_utc.astimezone(JST).isoformat(),
        "hit_hours": event.hit_hours,
        "max_gauge_mm_h": event.max_gauge_mm_h,
        "mean_gauge_mm_h": event.mean_gauge_mm_h,
        "sum_gauge_mm": event.sum_gauge_mm,
        "elapsed_after_rain_h": elapsed_h,
        "after_stac_id": after_scene.stac_id if after_scene else "",
        "after_product_identifier": scene_product_name(after_scene) if after_scene else "",
        "after_acquisition_utc": after_scene.acquisition_time.isoformat() if after_scene else "",
        "after_acquisition_jst": after_scene.acquisition_time.astimezone(JST).isoformat() if after_scene else "",
        "after_platform": after_scene.platform if after_scene else "",
        "after_orbit_direction": after_scene.orbit_direction if after_scene else "",
        "after_relative_orbit": after_scene.relative_orbit if after_scene else "",
        "after_product_type": after_scene.product_type if after_scene else "",
        "after_path": after_path,
        "before_stac_id": before_scene.stac_id if before_scene else "",
        "before_product_identifier": scene_product_name(before_scene) if before_scene else "",
        "before_acquisition_utc": before_scene.acquisition_time.isoformat() if before_scene else "",
        "before_acquisition_jst": before_scene.acquisition_time.astimezone(JST).isoformat() if before_scene else "",
        "before_path": before_path,
        "downloaded_or_existing": downloaded_or_existing,
        "aoi_min_lon": bbox[0],
        "aoi_min_lat": bbox[1],
        "aoi_max_lon": bbox[2],
        "aoi_max_lat": bbox[3],
        "truth_geometry": truth_geometry,
        "note": note,
    }


def write_report(
    output_dir: Path,
    args: argparse.Namespace,
    bbox: tuple[float, float, float, float],
    events: list[RainEvent],
    rows: list[dict[str, Any]],
) -> None:
    matched = [row for row in rows if row["status"] == "matched"]
    report = [
        "# 輪島 確認済み浸水域 Sentinel-1 取得結果",
        "",
        "## 条件",
        "",
        f"- 正解ラベル: `{args.truth_path}`",
        f"- AOI bbox: `{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}`",
        f"- 降雨強度: {args.rain_min_mm_h:.1f}〜{args.rain_max_mm_h:.1f} mm/h",
        f"- 降雨後 Sentinel-1: 降雨終了から {args.after_hours:.1f} 時間以内",
        f"- 降雨前 Sentinel-1: 同じ軌道条件で降雨開始前 {args.before_lookback_days} 日以内",
        "",
        "## 結果",
        "",
        f"- 降雨イベント候補: {len(events)}",
        f"- Sentinel-1 after/before ペア候補: {len(matched)}",
        f"- 既存または取得済みペア: {sum(1 for row in matched if str(row['downloaded_or_existing']) == 'True')}",
        "",
        "詳細は `wajima_s1_rain_pairs.csv` を確認してください。",
    ]
    (output_dir / "README.md").write_text("\n".join(report), encoding="utf-8-sig")


def write_report_clean(
    output_dir: Path,
    args: argparse.Namespace,
    bbox: tuple[float, float, float, float],
    events: list[RainEvent],
    rows: list[dict[str, Any]],
) -> None:
    matched = [row for row in rows if row["status"] == "matched"]
    report = [
        "# 輪島 確認済み浸水域 Sentinel-1 取得結果",
        "",
        "## 条件",
        "",
        f"- 正解ラベル: `{args.truth_path}`",
        f"- AOI bbox: `{bbox[0]:.6f}, {bbox[1]:.6f}, {bbox[2]:.6f}, {bbox[3]:.6f}`",
        f"- 降雨強度: {args.rain_min_mm_h:.1f}〜{args.rain_max_mm_h:.1f} mm/h",
        f"- 降雨イベント抽出期間: {args.rain_start_utc} .. {args.rain_end_utc}",
        f"- 降雨後 Sentinel-1: 降雨終了から {args.after_hours:.1f} 時間以内",
        f"- 降雨前 Sentinel-1: 同じ軌道条件で降雨開始前 {args.before_lookback_days} 日以内",
        f"- ダウンロード先: `{args.download_dir}`",
        "",
        "## 結果",
        "",
        f"- 降雨イベント候補: {len(events)}",
        f"- Sentinel-1 after/before ペア候補: {len(matched)}",
        f"- 既存または取得済みペア: {sum(1 for row in matched if str(row['downloaded_or_existing']) == 'True')}",
        "",
        "詳細は `wajima_s1_rain_pairs.csv` を確認してください。",
    ]
    (output_dir / "README.md").write_text("\n".join(report), encoding="utf-8-sig")


if __name__ == "__main__":
    raise SystemExit(main())
