#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/rebuild_gsmap_grids.py

gsmap_points の既存データから grid_id ごとの代表座標を取得し、
gsmap_grids テーブルを再構築する。
"""

from __future__ import annotations

import logging
from typing import Iterable

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db import models
from app.utils.japan import point_within_japan

logger = logging.getLogger(__name__)


def fetch_grid_candidates(session: Session) -> Iterable[tuple[str, float, float]]:
    stmt = (
        select(
            models.GsmapPoint.grid_id,
            func.min(models.GsmapPoint.lat),
            func.min(models.GsmapPoint.lon),
        )
        .where(models.GsmapPoint.grid_id.isnot(None))
        .group_by(models.GsmapPoint.grid_id)
    )
    for grid_id, lat, lon in session.execute(stmt):
        if lat is None or lon is None or grid_id is None:
            continue
        yield grid_id, float(lat), float(lon)


def rebuild_grids() -> None:
    with SessionLocal() as session:
        logger.info("clearing gsmap_grids ...")
        session.query(models.GsmapGrid).delete()
        session.commit()

        batch = []
        total = 0
        for grid_id, lat, lon in fetch_grid_candidates(session):
            is_land = point_within_japan(lat, lon)
            batch.append(
                models.GsmapGrid(
                    grid_id=grid_id,
                    lat=lat,
                    lon=lon,
                    is_japan_land=is_land,
                    region="Japan" if is_land else None,
                )
            )
            if len(batch) >= 2000:
                session.bulk_save_objects(batch)
                session.commit()
                total += len(batch)
                logger.info("inserted %d grids ...", total)
                batch.clear()

        if batch:
            session.bulk_save_objects(batch)
            session.commit()
            total += len(batch)

        logger.info("DONE. total grids stored: %d", total)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    rebuild_grids()


if __name__ == "__main__":
    main()


