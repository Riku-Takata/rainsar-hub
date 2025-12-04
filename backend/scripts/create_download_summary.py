#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/create_download_summary.py

各 grid_id ディレクトリ内に、ダウンロード済みファイルの遅延時間などをまとめた
サマリファイル (summary_delay.txt) を作成する。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db import models
from app.core.config import settings

logger = logging.getLogger(__name__)

SUMMARY_FILENAME = "summary_delay.txt"

def _normalize_name(name: str) -> str:
    """
    S1CDSEClient と同様の正規化を行う
    (ファイル名は _COG や .SAFE が除去されて保存されているため)
    """
    if name.endswith("_COG"):
        name = name[:-4]
    if name.endswith(".SAFE"):
        name = name[:-5]
    return name

def create_summary_for_grid(db: Session, grid_dir: Path):
    grid_id = grid_dir.name
    
    # このグリッドに関連するペアをDBから全取得
    pairs = db.query(models.S1Pair).filter(
        models.S1Pair.grid_id == grid_id
    ).order_by(models.S1Pair.event_start_ts_utc).all()

    if not pairs:
        return

    lines = []
    lines.append(f"=== Download Summary for Grid: {grid_id} ===")
    lines.append(f"Generated at: {settings.s1_safe_path}")
    lines.append("-" * 60)
    
    downloaded_count = 0

    for p in pairs:
        # ファイル名の正規化 (_COG除去など)
        after_stem = _normalize_name(p.after_scene_id)
        after_zip = grid_dir / f"{after_stem}.zip"
        
        before_stem = _normalize_name(p.before_scene_id) if p.before_scene_id else None
        before_zip = grid_dir / f"{before_stem}.zip" if before_stem else None

        # ファイルの存在チェック
        is_after_ok = after_zip.exists()
        is_before_ok = (before_zip and before_zip.exists())

        # After画像が存在すれば、このペアは「ダウンロード済み（または一部済み）」としてリストに載せる
        if is_after_ok:
            downloaded_count += 1
            
            lines.append(f"Event Start (UTC) : {p.event_start_ts_utc}")
            lines.append(f"Event End   (UTC) : {p.event_end_ts_utc}")
            lines.append(f"Rain Info         : {p.hit_hours}h duration, Max {p.max_gauge_mm_h:.1f} mm/h")
            lines.append(f"Delay (Hours)     : {p.delay_h:.2f} h")
            lines.append(f"After Scene       : {after_stem} {'[OK]' if is_after_ok else '[MISSING]'}")
            lines.append(f"Before Scene      : {before_stem or 'None'} {'[OK]' if is_before_ok else '[MISSING]'}")
            lines.append("-" * 60)

    if downloaded_count == 0:
        lines.append("No downloaded scenes found for this grid (or logic mismatch).")

    # ファイル書き込み
    out_path = grid_dir / SUMMARY_FILENAME
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")
    
    if downloaded_count > 0:
        logger.info(f"[{grid_id}] Created summary with {downloaded_count} pairs.")
    else:
        # デバッグ用にログを出す（ダウンロードフォルダはあるがファイルがない場合など）
        pass


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    safe_root = settings.s1_safe_path
    if not safe_root.exists():
        logger.error(f"Safe root not found: {safe_root}")
        return

    db = SessionLocal()
    try:
        grid_dirs = [d for d in safe_root.iterdir() if d.is_dir() and not d.name.startswith("_")]
        
        logger.info(f"Found {len(grid_dirs)} grid directories. Creating summaries...")
        
        for d in grid_dirs:
            create_summary_for_grid(db, d)
            
    finally:
        db.close()
        logger.info("Done.")


if __name__ == "__main__":
    main()