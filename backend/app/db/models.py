from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    ForeignKey,
    func,
    JSON,
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

class GsmapGrid(Base):
    __tablename__ = "gsmap_grids"

    grid_id = Column(String(32), primary_key=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    is_japan_land = Column(Boolean, nullable=False, server_default="0")
    region = Column(String(32), nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


    # 追加のインデックスは Alembic の自動生成に任せる


class GsmapEvent(Base):
    """
    ある格子点 (lat, lon) における「連続した降雨イベント」のサマリ。

    - threshold_mm_h 以上の雨が続いた時間帯を 1 イベントとみなす
    - 連続判定は「隣り合うレコードの差が 1 時間以内」くらいで判定
    """

    __tablename__ = "gsmap_events"

    id = Column(BigInteger, primary_key=True, index=True)

    # グリッドID (検索・結合用)
    grid_id = Column(String(32), index=True, nullable=False)

    # 空間
    lat = Column(Float, index=True, nullable=False)
    lon = Column(Float, index=True, nullable=False)
    region = Column(String(50), index=True, nullable=True)

    # 時間範囲
    start_ts_utc = Column(DateTime, index=True, nullable=False)
    end_ts_utc = Column(DateTime, index=True, nullable=False)

    # イベントの統計値
    hit_hours = Column(Integer, nullable=False)            # 何時間ぶんヒットしたか
    max_gauge_mm_h = Column(Float, nullable=False)        # このイベント中の max(mm/h)
    sum_gauge_mm_h = Column(Float, nullable=False)        # 単純合計(mm/h * 時間数)
    mean_gauge_mm_h = Column(Float, nullable=False)       # 平均(mm/h)

    # どんな条件で作ったイベントか（再現性のため）
    threshold_mm_h = Column(Float, nullable=False)

    # 代表するソースファイル（最大雨量時など）
    repr_source_file = Column(String(255), nullable=True)
    
    # 詳細な時系列データなどを保持する場合
    rainfall_data = Column(JSON, nullable=True)

class JapanGrid(Base):
    """
    日本国土に含まれるメッシュ（Grid）のユニークなリスト。
    地図上の初期プロットに使用する。
    """
    __tablename__ = "japan_grids"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    grid_id = Column(String(32), nullable=False, unique=True, index=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    is_rice_paddy = Column(Boolean, nullable=False, server_default="0")
    is_highway = Column(Boolean, nullable=False, server_default="0")

class S1Pair(Base):
    """
    GSMaP の降雨イベントと、それに対応する Sentinel-1 after/before シーンのペア。

    - 一行 = 1 降雨イベント × 1 after/before セット
    """

    __tablename__ = "s1_pairs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # GSMaP イベント情報
    grid_id = Column(String(32), index=True, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    event_start_ts_utc = Column(DateTime, index=True, nullable=False)
    event_end_ts_utc = Column(DateTime, nullable=False)
    threshold_mm_h = Column(Float, nullable=False)
    hit_hours = Column(Integer, nullable=False)
    max_gauge_mm_h = Column(Float, nullable=False)

    # Sentinel-1 after シーン
    after_scene_id = Column(String(128), index=True, nullable=False)
    after_platform = Column(String(16), nullable=True)
    after_mission = Column(String(8), nullable=True)  # S1A / S1B
    after_pass_direction = Column(String(8), nullable=True)  # ASC / DSC
    after_relative_orbit = Column(Integer, nullable=True)
    after_start_ts_utc = Column(DateTime, nullable=False)
    after_end_ts_utc = Column(DateTime, nullable=False)

    # Sentinel-1 before シーン（存在しない場合もある）
    before_scene_id = Column(String(128), index=True, nullable=True)
    before_start_ts_utc = Column(DateTime, nullable=True)
    before_end_ts_utc = Column(DateTime, nullable=True)
    before_relative_orbit = Column(Integer, nullable=True)

    # after シーンがイベント終了から何時間後か
    delay_h = Column(Float, nullable=True)

    # データソース識別（"cdse" など）
    source = Column(String(32), nullable=False, default="cdse")