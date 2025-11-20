from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func
from sqlalchemy.orm import Session, aliased

from app.dependencies.db import get_db
from app.db import models
from app.utils.japan import japan_bbox

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/gsmap", tags=["gsmap"])


class RainPoint(BaseModel):
    ts_utc: datetime
    gauge_mm_h: float

    class Config:
        from_attributes = True


class RainGridSummary(BaseModel):
    grid_id: str
    lat: float
    lon: float
    latest_ts_utc: datetime
    rain_hours: int
    max_gauge_mm_h: float
    mean_gauge_mm_h: float
    sum_gauge_mm_h: float


class RainGridDetail(RainGridSummary):
    rain_points: List[RainPoint]
    total_points: int
    next_offset: Optional[int] = None


@router.get("/grids", response_model=List[RainGridSummary])
def list_rain_grids(
    *,
    db: Session = Depends(get_db),
    start_ts_utc: Optional[datetime] = None,
    end_ts_utc: Optional[datetime] = None,
    hours_back: Optional[int] = Query(None, ge=1, le=24 * 30),
    limit: int = Query(400, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    min_gauge_mm_h: float = Query(0.0, ge=0.0),
    japan_only: bool = True,
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lon: Optional[float] = None,
    max_lon: Optional[float] = None,
    sort_by: Literal["latest", "max_gauge", "sum_gauge", "rain_hours"] = "latest",
    sort_order: Literal["asc", "desc"] = "desc",
) -> List[RainGridSummary]:
    """
    gsmap_points を grid_id 単位で集約し、任意の期間・順序でページネーションして返す。
    デフォルトでは期間指定を行わず、全期間を対象にする。
    """
    if japan_only:
        bbox = japan_bbox()
        if min_lat is None:
            min_lat = bbox["min_lat"]
        if max_lat is None:
            max_lat = bbox["max_lat"]
        if min_lon is None:
            min_lon = bbox["min_lon"]
        if max_lon is None:
            max_lon = bbox["max_lon"]

        land_exists = (
            db.query(models.GsmapGrid.grid_id)
            .filter(models.GsmapGrid.is_japan_land.is_(True))
            .limit(1)
            .first()
        )
        if land_exists is None:
            total_points = db.query(func.count(models.GsmapPoint.id)).scalar()
            logger.error(
                "Japan-only filter requested but gsmap_grids has no land entries. "
                "gsmap_points=%s. Run scripts.rebuild_gsmap_grids to refresh metadata.",
                total_points,
            )
            raise HTTPException(
                status_code=503,
                detail=(
                    "No gsmap_grids rows with is_japan_land=1. "
                    "Run `python -m scripts.rebuild_gsmap_grids` to populate grid metadata."
                ),
            )

    end_ts = end_ts_utc
    if hours_back is not None and end_ts is None:
        end_ts = datetime.now(timezone.utc)

    start_ts = start_ts_utc
    if start_ts is None and hours_back is not None and end_ts is not None:
        start_ts = end_ts - timedelta(hours=hours_back)

    if start_ts and end_ts and start_ts > end_ts:
        raise HTTPException(status_code=400, detail="start_ts_utc must be before end_ts_utc")

    latest_col = func.max(models.GsmapPoint.ts_utc)
    max_col = func.max(models.GsmapPoint.gauge_mm_h)
    mean_col = func.avg(models.GsmapPoint.gauge_mm_h)
    sum_col = func.sum(models.GsmapPoint.gauge_mm_h)
    count_col = func.count(models.GsmapPoint.id)

    grid_alias = aliased(models.GsmapGrid)
    base_query = (
        db.query(
            models.GsmapPoint.grid_id.label("grid_id"),
            grid_alias.lat.label("lat"),
            grid_alias.lon.label("lon"),
            latest_col.label("latest_ts_utc"),
            count_col.label("rain_hours"),
            max_col.label("max_gauge_mm_h"),
            mean_col.label("mean_gauge_mm_h"),
            sum_col.label("sum_gauge_mm_h"),
        )
        .filter(models.GsmapPoint.grid_id.isnot(None))
        .filter(models.GsmapPoint.gauge_mm_h >= min_gauge_mm_h)
        .join(grid_alias, grid_alias.grid_id == models.GsmapPoint.grid_id)
        .group_by(models.GsmapPoint.grid_id)
    )
    if min_lat is not None:
        base_query = base_query.filter(models.GsmapPoint.lat >= min_lat)
    if max_lat is not None:
        base_query = base_query.filter(models.GsmapPoint.lat <= max_lat)
    if min_lon is not None:
        base_query = base_query.filter(models.GsmapPoint.lon >= min_lon)
    if max_lon is not None:
        base_query = base_query.filter(models.GsmapPoint.lon <= max_lon)
    if start_ts is not None:
        base_query = base_query.filter(models.GsmapPoint.ts_utc >= start_ts)
    if end_ts is not None:
        base_query = base_query.filter(models.GsmapPoint.ts_utc <= end_ts)

    sort_map = {
        "latest": latest_col,
        "max_gauge": max_col,
        "sum_gauge": sum_col,
        "rain_hours": count_col,
    }
    order_expr = sort_map[sort_by].desc() if sort_order == "desc" else sort_map[sort_by].asc()
    base_query = base_query.order_by(order_expr, latest_col.desc())

    if japan_only:
        base_query = base_query.filter(grid_alias.is_japan_land.is_(True))

    rows = base_query.offset(offset).limit(limit).all()
    summaries: List[RainGridSummary] = []
    for row in rows:
        if row.lat is None or row.lon is None:
            continue
        lat_val = float(row.lat)
        lon_val = float(row.lon)
        summaries.append(
            RainGridSummary(
                grid_id=row.grid_id,
                lat=lat_val,
                lon=lon_val,
                latest_ts_utc=row.latest_ts_utc,
                rain_hours=int(row.rain_hours or 0),
                max_gauge_mm_h=float(row.max_gauge_mm_h or 0.0),
                mean_gauge_mm_h=float(row.mean_gauge_mm_h or 0.0),
                sum_gauge_mm_h=float(row.sum_gauge_mm_h or 0.0),
            )
        )
    return summaries


@router.get("/grids/{grid_id}", response_model=RainGridDetail)
def get_rain_grid_detail(
    *,
    grid_id: str,
    db: Session = Depends(get_db),
    start_ts_utc: Optional[datetime] = None,
    end_ts_utc: Optional[datetime] = None,
    hours_back: Optional[int] = Query(None, ge=1, le=24 * 365),
    limit: int = Query(240, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    min_gauge_mm_h: float = Query(0.0, ge=0.0),
    sort_order: Literal["asc", "desc"] = "desc",
) -> RainGridDetail:
    """
    grid_id ごとの降雨実績を全期間 (任意の時間フィルタ) でページネーションして返す。
    """
    end_ts = end_ts_utc
    if hours_back is not None and end_ts is None:
        end_ts = datetime.now(timezone.utc)

    start_ts = start_ts_utc
    if start_ts is None and hours_back is not None and end_ts is not None:
        start_ts = end_ts - timedelta(hours=hours_back)

    if start_ts and end_ts and start_ts > end_ts:
        raise HTTPException(status_code=400, detail="start_ts_utc must be before end_ts_utc")

    grid_info = (
        db.query(models.GsmapGrid.lat, models.GsmapGrid.lon)
        .filter(models.GsmapGrid.grid_id == grid_id)
        .first()
    )
    if grid_info is None:
        raise HTTPException(status_code=404, detail=f"grid_id={grid_id} not found")

    base_query = db.query(models.GsmapPoint).filter(models.GsmapPoint.grid_id == grid_id)
    base_query = base_query.filter(models.GsmapPoint.gauge_mm_h >= min_gauge_mm_h)
    if start_ts is not None:
        base_query = base_query.filter(models.GsmapPoint.ts_utc >= start_ts)
    if end_ts is not None:
        base_query = base_query.filter(models.GsmapPoint.ts_utc <= end_ts)

    total_points = base_query.count()
    if total_points == 0:
        raise HTTPException(status_code=404, detail=f"No rainfall data for grid_id={grid_id}")

    stats_row = base_query.with_entities(
        func.max(models.GsmapPoint.ts_utc),
        func.max(models.GsmapPoint.gauge_mm_h),
        func.avg(models.GsmapPoint.gauge_mm_h),
        func.sum(models.GsmapPoint.gauge_mm_h),
    ).first()

    order_clause = (
        models.GsmapPoint.ts_utc.desc()
        if sort_order == "desc"
        else models.GsmapPoint.ts_utc.asc()
    )
    points = base_query.order_by(order_clause).offset(offset).limit(limit).all()

    lat = float(grid_info[0])
    lon = float(grid_info[1])

    rain_points = [
        RainPoint(ts_utc=point.ts_utc, gauge_mm_h=float(point.gauge_mm_h))
        for point in points
    ]

    next_offset = offset + len(rain_points) if offset + len(rain_points) < total_points else None

    latest_ts = stats_row[0]
    max_gauge = float(stats_row[1] or 0.0)
    mean_gauge = float(stats_row[2] or 0.0)
    sum_gauge = float(stats_row[3] or 0.0)

    return RainGridDetail(
        grid_id=grid_id,
        lat=lat,
        lon=lon,
        latest_ts_utc=latest_ts,
        rain_hours=total_points,
        max_gauge_mm_h=max_gauge,
        mean_gauge_mm_h=mean_gauge,
        sum_gauge_mm_h=sum_gauge,
        rain_points=rain_points,
        total_points=total_points,
        next_offset=next_offset,
    )
