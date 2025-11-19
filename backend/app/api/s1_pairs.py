from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.dependencies.db import get_db
from app.db import models

router = APIRouter()


class S1PairResponse(BaseModel):
    id: int
    grid_id: str
    lat: float
    lon: float
    event_start_ts_utc: datetime
    after_scene_id: str
    after_platform: Optional[str] = None
    
    class Config:
        from_attributes = True


@router.get("/s1-pairs", response_model=List[S1PairResponse])
def get_s1_pairs(
    limit: int = 1000,
    db: Session = Depends(get_db)
):
    """
    S1Pair の一覧を取得する。
    地図表示用に必要なフィールドのみを返す。
    """
    pairs = (
        db.query(models.S1Pair)
        .order_by(models.S1Pair.event_start_ts_utc.desc())
        .limit(limit)
        .all()
    )
    return pairs
