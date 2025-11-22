# backend/app/api/grids.py

import json
from typing import List, Optional, Dict, Any, Set
from datetime import datetime, timedelta, timezone
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy import func
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.dependencies.db import get_db
from app.db import models
from app.services.s1_cdse_client import S1CDSEClient, S1Scene
from app.core.config import settings

# Helper functions
def normalize_mission(platform: Optional[str]) -> Optional[str]:
    if not platform: return None
    p = platform.lower()
    if "sentinel-1a" in p or p.endswith("1a") or p == "s1a": return "S1A"
    if "sentinel-1b" in p or p.endswith("1b") or p == "s1b": return "S1B"
    if "sentinel-1" in p: return "S1"
    return platform[:8].upper()

def normalize_pass_direction(direction: Optional[str]) -> Optional[str]:
    if not direction: return None
    d = direction.lower()
    if d.startswith("asc"): return "ASC"
    if d.startswith("des"): return "DSC"
    return direction[:3].upper()

router = APIRouter()

# --- Models ---
class GridStatResponse(BaseModel):
    grid_id: str
    lat: float
    lon: float
    rain_point_count: int
    class Config: from_attributes = True

class SceneMetadata(BaseModel):
    file_name: str
    acquisition_time: datetime
    platform: Optional[str] = None
    orbit_direction: Optional[str] = None
    relative_orbit: Optional[int] = None

class SatelliteInfo(BaseModel):
    found: bool
    searched: bool
    delay_hours: Optional[float] = None
    after: Optional[SceneMetadata] = None
    before: Optional[SceneMetadata] = None

class RainEventResponse(BaseModel):
    id: int
    start_ts: datetime
    end_ts: datetime
    max_gauge_mm_h: float
    satellite: SatelliteInfo
    class Config: from_attributes = True

class DownloadStatusResponse(BaseModel):
    status: str  # not_started, downloading, downloaded, processing, processed, failed
    progress: float

# --- State Management ---
download_tasks: Dict[str, float] = {}
cancellation_requests: Set[str] = set()

# --- Endpoints ---

@router.get("/grids/stats", response_model=List[GridStatResponse])
def get_grid_stats(min_rain: float = 1.0, db: Session = Depends(get_db)):
    """
    グリッドごとの雨量統計（降雨時間合計）を返す。
    高速化のため、数百万件の GsmapPoint ではなく、集約済みの GsmapEvent を使用する。
    """
    # GsmapEvent (軽量) を結合して hit_hours (降雨時間) を合計する
    stmt = (
        db.query(
            models.JapanGrid.grid_id,
            models.JapanGrid.lat,
            models.JapanGrid.lon,
            func.coalesce(func.sum(models.GsmapEvent.hit_hours), 0).label("rain_point_count")
        )
        .outerjoin(
            models.GsmapEvent,
            (models.JapanGrid.grid_id == models.GsmapEvent.grid_id) &
            (models.GsmapEvent.max_gauge_mm_h >= min_rain)
        )
        .group_by(models.JapanGrid.grid_id, models.JapanGrid.lat, models.JapanGrid.lon)
    )
    return stmt.all()

@router.get("/grids/{grid_id}/events", response_model=List[RainEventResponse])
def get_grid_rain_events(grid_id: str, limit: int = 10000, min_rain: float = 1.0, db: Session = Depends(get_db)):
    points = (
        db.query(models.GsmapPoint)
        .filter(models.GsmapPoint.grid_id == grid_id, models.GsmapPoint.gauge_mm_h >= min_rain)
        .order_by(models.GsmapPoint.ts_utc.asc())
        .all()
    )
    if not points: return []

    existing_pairs = db.query(models.S1Pair).filter(models.S1Pair.grid_id == grid_id).all()
    pairs_map = {p.event_start_ts_utc.replace(tzinfo=timezone.utc): p for p in existing_pairs}

    events_list = []
    if points:
        def to_utc(dt): return dt.replace(tzinfo=timezone.utc)
        current_evt = {"start": to_utc(points[0].ts_utc), "end": to_utc(points[0].ts_utc), "max_rain": points[0].gauge_mm_h}
        for p in points[1:]:
            p_ts = to_utc(p.ts_utc)
            if (p_ts - current_evt["end"]) <= timedelta(hours=1.1):
                current_evt["end"] = p_ts
                current_evt["max_rain"] = max(current_evt["max_rain"], p.gauge_mm_h)
            else:
                events_list.append(current_evt)
                current_evt = {"start": p_ts, "end": p_ts, "max_rain": p.gauge_mm_h}
        events_list.append(current_evt)

    response_list = []
    for i, ev in enumerate(reversed(events_list)):
        pair = pairs_map.get(ev["start"])
        sat_info = SatelliteInfo(found=False, searched=False)
        if pair:
            after_meta = SceneMetadata(
                file_name=pair.after_scene_id,
                acquisition_time=pair.after_start_ts_utc.replace(tzinfo=timezone.utc),
                platform=pair.after_mission,
                orbit_direction=pair.after_pass_direction,
                relative_orbit=pair.after_relative_orbit
            )
            before_meta = None
            if pair.before_scene_id:
                before_meta = SceneMetadata(
                    file_name=pair.before_scene_id,
                    acquisition_time=pair.before_start_ts_utc.replace(tzinfo=timezone.utc),
                    platform=None, orbit_direction=None, relative_orbit=pair.before_relative_orbit
                )
            sat_info = SatelliteInfo(
                found=True, searched=True, delay_hours=pair.delay_h,
                after=after_meta, before=before_meta
            )
        response_list.append(RainEventResponse(
            id=i, start_ts=ev["start"], end_ts=ev["end"], max_gauge_mm_h=ev["max_rain"], satellite=sat_info
        ))
    return response_list

@router.get("/search/satellite", response_model=SatelliteInfo)
def search_satellite_for_event(
    grid_id: str, lat: float, lon: float,
    event_start_str: str, event_end_str: str, max_rain: float,
    force: bool = False,
    db: Session = Depends(get_db)
):
    event_start = datetime.fromisoformat(event_start_str.replace("Z", "+00:00"))
    event_end = datetime.fromisoformat(event_end_str.replace("Z", "+00:00"))
    
    existing = db.query(models.S1Pair).filter(
        models.S1Pair.grid_id == grid_id,
        models.S1Pair.event_start_ts_utc == event_start
    ).first()
    
    if existing:
        if not force:
            after_meta = SceneMetadata(
                file_name=existing.after_scene_id,
                acquisition_time=existing.after_start_ts_utc.replace(tzinfo=timezone.utc),
                platform=existing.after_mission,
                orbit_direction=existing.after_pass_direction,
                relative_orbit=existing.after_relative_orbit
            )
            before_meta = None
            if existing.before_scene_id:
                before_meta = SceneMetadata(
                    file_name=existing.before_scene_id,
                    acquisition_time=existing.before_start_ts_utc.replace(tzinfo=timezone.utc),
                    platform=None, orbit_direction=None, relative_orbit=existing.before_relative_orbit
                )
            return SatelliteInfo(
                found=True, searched=True, delay_hours=existing.delay_h,
                after=after_meta, before=before_meta
            )
        else:
            db.delete(existing)
            db.commit()
    
    client = S1CDSEClient()
    after_scene = client.find_after_scene(lat=lat, lon=lon, event_end_utc=event_end, after_hours=12.0)
    if not after_scene: return SatelliteInfo(found=False, searched=True)
    
    before_scene = client.find_before_scene_unbounded(
        lat=lat, lon=lon, 
        ref_time_utc=after_scene.acquisition_time,
        platform=after_scene.platform,
        orbit_direction=after_scene.orbit_direction,
        relative_orbit=after_scene.relative_orbit
    )
    if not before_scene: return SatelliteInfo(found=False, searched=True)
        
    delay_sec = (after_scene.acquisition_time - event_end).total_seconds()
    delay_h = round(delay_sec / 3600.0, 2)
    
    after_id = after_scene.product_identifier or after_scene.stac_id
    before_id = before_scene.product_identifier or before_scene.stac_id
    
    after_meta = SceneMetadata(
        file_name=after_id,
        acquisition_time=after_scene.acquisition_time,
        platform=normalize_mission(after_scene.platform),
        orbit_direction=normalize_pass_direction(after_scene.orbit_direction),
        relative_orbit=after_scene.relative_orbit
    )
    before_meta = SceneMetadata(
        file_name=before_id,
        acquisition_time=before_scene.acquisition_time,
        platform=normalize_mission(before_scene.platform),
        orbit_direction=normalize_pass_direction(before_scene.orbit_direction),
        relative_orbit=before_scene.relative_orbit
    )

    new_pair = models.S1Pair(
        grid_id=grid_id, lat=lat, lon=lon,
        event_start_ts_utc=event_start, event_end_ts_utc=event_end,
        threshold_mm_h=0, hit_hours=0, max_gauge_mm_h=max_rain,
        after_scene_id=after_id, after_platform=after_scene.platform, after_mission=after_meta.platform, after_pass_direction=after_meta.orbit_direction, after_relative_orbit=after_scene.relative_orbit,
        after_start_ts_utc=after_scene.acquisition_time, after_end_ts_utc=after_scene.acquisition_time,
        before_scene_id=before_id, before_start_ts_utc=before_scene.acquisition_time, before_end_ts_utc=before_scene.acquisition_time, before_relative_orbit=before_scene.relative_orbit,
        delay_h=delay_h, source="cdse_web_search"
    )
    db.add(new_pair)
    db.commit()

    return SatelliteInfo(
        found=True, searched=True, delay_hours=delay_h,
        after=after_meta, before=before_meta
    )

# --- Download & Status Logic ---

@router.post("/download/product")
def download_single_product(
    product_id: str, 
    grid_id: str = Query(..., description="Grid ID for directory structure"),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    if product_id in cancellation_requests:
        cancellation_requests.remove(product_id)
    
    download_tasks[product_id] = 0.0
    background_tasks.add_task(run_download, product_id, grid_id)
    return {"status": "Download started", "product_id": product_id, "grid_id": grid_id}

@router.post("/download/cancel/{product_id}")
def cancel_download(product_id: str):
    if product_id in download_tasks:
        cancellation_requests.add(product_id)
        return {"status": "Cancellation requested", "product_id": product_id}
    return {"status": "Not downloading", "product_id": product_id}

@router.get("/download/status/{product_id}", response_model=DownloadStatusResponse)
def get_download_status(product_id: str, grid_id: Optional[str] = None):
    client = S1CDSEClient()
    stem = client._normalize_product_name(product_id)
    
    # 1. まず「前処理済み(Processed)」をチェック
    # s1_samples/{grid_id}/{stem}_proc.tif
    if grid_id:
        proc_path = settings.s1_sample_path / grid_id / f"{stem}_proc.tif"
        if proc_path.exists() and proc_path.stat().st_size > 0:
            return {"status": "processed", "progress": 100.0}

    # 2. 「処理中(Processing)」または「失敗(Failed)」をチェック
    # s1_samples/_status/{grid_id}___{stem}.json
    if grid_id:
        status_file = settings.s1_sample_path / "_status" / f"{grid_id}___{stem}.json"
        if status_file.exists():
            try:
                with open(status_file, "r") as f:
                    info = json.load(f)
                st = info.get("status", "processing")
                return {"status": st, "progress": 100.0}
            except:
                pass

    # 3. 「ダウンロード中(Downloading)」をチェック
    if product_id in download_tasks:
        return {"status": "downloading", "progress": download_tasks[product_id]}
    
    # 4. 「ダウンロード完了(Downloaded)」をチェック (Rawファイルがあるか)
    zip_name = stem + ".zip"
    raw_path = settings.s1_safe_path / zip_name
    if raw_path.exists() and raw_path.stat().st_size > 0:
        # rawはあるがprocessedがない場合 -> downloaded (waiting for process)
        return {"status": "downloaded", "progress": 100.0}
        
    return {"status": "not_started", "progress": 0.0}

def run_download(product_id: str, grid_id: str):
    client = S1CDSEClient()
    save_dir = settings.s1_safe_path
    
    # トリガー出力先
    trigger_dir = settings.s1_sample_path / "_triggers"
    trigger_dir.mkdir(parents=True, exist_ok=True)

    def progress_cb(current, total):
        if product_id in cancellation_requests:
            return False
        if total > 0:
            p = (current / total) * 100.0
            download_tasks[product_id] = round(p, 1)
        return True

    try:
        saved_path = client.download_product(product_id, save_dir, progress_callback=progress_cb)
        
        if saved_path:
            # 完了時: トリガーファイル作成
            safe_stem = saved_path.stem.replace(".SAFE", "")
            trigger_file = trigger_dir / f"{grid_id}___{safe_stem}.req"
            trigger_file.touch()
            
    finally:
        if product_id in download_tasks:
            del download_tasks[product_id]
        if product_id in cancellation_requests:
            cancellation_requests.remove(product_id)