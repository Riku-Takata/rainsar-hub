from __future__ import annotations

import logging
from functools import lru_cache
import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Final

from shapely.geometry import Point, shape
from shapely.geometry.base import BaseGeometry
from shapely.prepared import PreparedGeometry, prep

logger = logging.getLogger(__name__)

JAPAN_BBOX: Final = {
    "min_lat": 24.0,
    "max_lat": 46.5,
    "min_lon": 123.0,
    "max_lon": 148.5,
}

JAPAN_GEOJSON_PATH = Path(__file__).resolve().parent / "data" / "japan.geojson"


@lru_cache(maxsize=1)
def _prepared_japan_geom() -> PreparedGeometry | None:
    try:
        with open(JAPAN_GEOJSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:  # noqa: BLE001
        logger.warning("failed to read %s, fallback to bbox: %s", JAPAN_GEOJSON_PATH, exc)
        return None

    # File is FeatureCollection with one feature
    if data.get("type") == "FeatureCollection":
        features = data.get("features", [])
        if not features:
            logger.warning("japan.geojson contains no features, fallback to bbox.")
            return None
        geometry = features[0].get("geometry")
    else:
        geometry = data.get("geometry", data)

    if not geometry:
        logger.warning("japan.geojson does not contain geometry; fallback to bbox.")
        return None

    geom = shape(geometry)
    return prep(geom)


def point_within_japan(lat: float, lon: float) -> bool:
    """
    緯度経度が日本ポリゴン内に含まれるかを判定する。
    """
    bbox = japan_bbox()
    if not (
        bbox["min_lat"] <= lat <= bbox["max_lat"]
        and bbox["min_lon"] <= lon <= bbox["max_lon"]
    ):
        return False

    prepared = _prepared_japan_geom()
    if prepared is None:
        return True
    return prepared.contains(Point(float(lon), float(lat)))


def japan_bbox() -> dict[str, float]:
    """
    デフォルトで使う日本周辺のBBoxを返す。
    """
    return JAPAN_BBOX.copy()
