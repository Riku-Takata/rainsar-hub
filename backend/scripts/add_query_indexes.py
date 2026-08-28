#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Add indexes used by spatial/rainfall lookup workflows.

This script is intentionally idempotent. It checks existing indexes before
issuing CREATE INDEX, so it can be rerun after restoring or rebuilding the DB.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.db.session import SessionLocal


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IndexSpec:
    table: str
    name: str
    columns: tuple[str, ...]

    @property
    def ddl(self) -> str:
        cols = ", ".join(self.columns)
        return f"CREATE INDEX {self.name} ON {self.table} ({cols})"


INDEXES = [
    # map7 bbox -> road grid lookup.
    IndexSpec("japan_road_grids", "ix_japan_road_grids_lat_lon", ("lat", "lon")),
    # Road source mesh code lookup. n13_008 is the mesh code-like field in this dataset.
    IndexSpec("road_polygons", "ix_road_polygons_n13_008", ("n13_008",)),
    # Useful when narrowing road class + mesh together.
    IndexSpec("road_polygons", "ix_road_polygons_n13_006_n13_008", ("n13_006", "n13_008")),
    # Rain day extraction by bbox, threshold, and time range.
    IndexSpec("gsmap_points", "ix_gsmap_points_lat_lon_ts_gauge", ("lat", "lon", "ts_utc", "gauge_mm_h")),
    IndexSpec("gsmap_points", "ix_gsmap_points_grid_ts_gauge", ("grid_id", "ts_utc", "gauge_mm_h")),
    # Future fude/paddy polygon imports.
    IndexSpec("fude_polygons", "ix_fude_polygons_point_lat_lng", ("point_lat", "point_lng")),
    IndexSpec("fude_polygons", "ix_fude_polygons_land_type_pref", ("land_type", "pref_id")),
]


def index_exists(db, table: str, index_name: str) -> bool:
    rows = db.execute(text(f"SHOW INDEX FROM {table} WHERE Key_name = :name"), {"name": index_name}).fetchall()
    return bool(rows)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db = SessionLocal()
    try:
        for spec in INDEXES:
            if index_exists(db, spec.table, spec.name):
                LOGGER.info("SKIP existing index: %s.%s", spec.table, spec.name)
                continue
            LOGGER.info("CREATE index: %s.%s (%s)", spec.table, spec.name, ", ".join(spec.columns))
            db.execute(text(spec.ddl))
            db.commit()
        LOGGER.info("Index setup completed.")
        return 0
    except Exception:
        db.rollback()
        LOGGER.exception("Index setup failed.")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
