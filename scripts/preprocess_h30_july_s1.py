#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
preprocess_h30_july_s1.py

平成30年7月豪雨のダウンロード済みSentinel-1 GRDシーンをバッチ前処理する。

処理チェーン (ESA SNAP GPT):
  Read → Apply-Orbit-File → ThermalNoiseRemoval → Calibration (Sigma0, VV+VH)
       → Terrain-Correction (10m, WGS84) → LinearToFromdB → Write (GeoTIFF-BigTIFF)

出力: output/h30_july/s1_processed/<scene_name>_proc.tif
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

# -------------------------------------------------------------------------
# 設定
# -------------------------------------------------------------------------
GPT_EXE    = r"C:\Program Files\esa-snap\bin\gpt.exe"
GRAPH_XML  = Path(__file__).parent / "s1_preprocess_graph.xml"
INPUT_DIR  = Path("output/h30_july/s1_downloads")
OUTPUT_DIR = Path("output/h30_july/s1_processed")
LOG_FILE   = OUTPUT_DIR / "preprocess_log.txt"

# GPTのメモリ設定 (GB) — マシンRAMに合わせて調整
GPT_MAX_MEMORY = "16G"
# 並列タイル数
GPT_TILE_CACHE = "8G"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)


def run_gpt(zip_path: Path, out_tif: Path) -> bool:
    """1シーンをGPTで前処理する。成功したらTrue。"""
    cmd = [
        GPT_EXE,
        str(GRAPH_XML),
        f"-PsourceFile={zip_path}",
        f"-PtargetFile={out_tif.with_suffix('')}",  # GPTは拡張子を自動付加
        f"-J-Xmx{GPT_MAX_MEMORY}",
        f"-c", GPT_TILE_CACHE,
        "-e",
        "-q", "4",   # 並列スレッド数
    ]

    logger.info(f"  コマンド: gpt ... {zip_path.name}")
    result = subprocess.run(
        cmd,
        capture_output=False,
        text=True,
        cwd=str(Path(__file__).parent.parent),  # リポジトリルート
    )

    if result.returncode == 0:
        logger.info(f"  [OK] -> {out_tif.name}")
        return True
    else:
        logger.error(f"  [FAILED] exit code={result.returncode}: {zip_path.name}")
        return False


def main():
    # 前チェック
    if not Path(GPT_EXE).exists():
        logger.error(f"GPT not found: {GPT_EXE}")
        sys.exit(1)
    if not GRAPH_XML.exists():
        logger.error(f"Graph XML not found: {GRAPH_XML}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ファイルハンドラをログへ追加
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)

    # 入力ZIPリスト
    zips = sorted(INPUT_DIR.glob("*.zip"))
    if not zips:
        logger.error(f"ZIPファイルが見つかりません: {INPUT_DIR}")
        sys.exit(1)

    logger.info(f"処理対象: {len(zips)} シーン")
    logger.info(f"出力先:   {OUTPUT_DIR.resolve()}")
    logger.info("=" * 60)

    results = []
    for i, zip_path in enumerate(zips, 1):
        stem = zip_path.stem  # 拡張子なし
        out_tif = OUTPUT_DIR / f"{stem}_proc.tif"

        if out_tif.exists():
            logger.info(f"[{i:02d}/{len(zips)}] SKIP (既存): {stem}")
            results.append((stem, "skipped"))
            continue

        logger.info(f"[{i:02d}/{len(zips)}] 処理開始: {stem}")
        ok = run_gpt(zip_path, out_tif)
        results.append((stem, "success" if ok else "failed"))

    # サマリー
    logger.info("=" * 60)
    success = sum(1 for _, s in results if s == "success")
    skipped = sum(1 for _, s in results if s == "skipped")
    failed  = sum(1 for _, s in results if s == "failed")
    logger.info(f"完了: {success} 成功 / {skipped} スキップ / {failed} 失敗 (計 {len(results)})")

    if failed:
        logger.warning("失敗シーン:")
        for name, status in results:
            if status == "failed":
                logger.warning(f"  - {name}")

    # 結果CSVを出力
    import csv
    csv_path = OUTPUT_DIR / "preprocess_results.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["scene", "status"])
        w.writerows(results)
    logger.info(f"結果CSV: {csv_path}")


if __name__ == "__main__":
    main()
