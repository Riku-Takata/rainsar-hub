from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
)
from app.db.base import Base


class GsmapPoint(Base):
    __tablename__ = "gsmap_points"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_utc = Column(DateTime, nullable=False, index=True)  # 1時間ごとのUTC時刻
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    gauge_mm_h = Column(Float, nullable=False)  # Gauge-calibrated-Rain-Rate
    rain_mm_h = Column(Float, nullable=True)    # 必要なら通常 Rain-Rate も
    region = Column(String(32), nullable=True)  # 'Japan' など
    grid_id = Column(String(32), nullable=True)
    source_file = Column(String(128), nullable=True)

    # 追加のインデックスは Alembic の自動生成に任せる


class RainEvent(Base):
    __tablename__ = "rain_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_utc = Column(DateTime, nullable=False, index=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    gauge_mm_h = Column(Float, nullable=False)
    grid_id = Column(String(32), nullable=True)
    type = Column(
        Enum("rain10_30", "clear0", name="rain_event_type"),
        nullable=False,
    )
