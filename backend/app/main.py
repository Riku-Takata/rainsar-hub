from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from app.dependencies.db import get_db
from app.db import models

app = FastAPI(title="RainSAR Hub API")


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/db/health")
def db_health_check(db: Session = Depends(get_db)) -> dict[str, int]:
    """
    DB につながるかどうかを簡易的にチェックするエンドポイント。
    まだデータは入っていない想定なので、件数は 0 でも OK。
    """
    gsmap_count = db.query(models.GsmapPoint).count()
    event_count = db.query(models.RainEvent).count()
    return {
        "gsmap_points": gsmap_count,
        "rain_events": event_count,
    }