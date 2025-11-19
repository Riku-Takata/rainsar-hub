from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.dependencies.db import get_db
from app.db import models
from app.api import s1_pairs

app = FastAPI(title="RainSAR Hub API")

# CORS configuration
origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(s1_pairs.router, tags=["s1-pairs"])


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