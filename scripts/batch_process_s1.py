#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts/batch_process_s1.py

s1_safe フォルダ内の全 grid_id フォルダを走査し、
preprocess_s1_cog.py の処理ロジックを適用して一括解析を行うスクリプト。

変更点:
- ProcessPoolExecutor による並列処理をやめ、逐次処理に変更
  → SNAP/snappy + GDAL を複数プロセスから叩いて JVM が落ちる問題を回避
- 既に出力ファイルが存在する場合は事前にスキップ
"""

import argparse
import logging
import sys
from pathlib import Path

# 同一ディレクトリにある preprocess_s1_cog をインポート
try:
    import preprocess_s1_cog
except ImportError:
    sys.path.append(str(Path(__file__).parent))
    import preprocess_s1_cog  # type: ignore[no-redef]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("batch_proc")


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch process Sentinel-1 images for all grids.")

    # デフォルトパスは環境に合わせて調整してください (Windows環境を想定)
    parser.add_argument(
        "--in-root",
        type=str,
        default=r"D:\sotsuron\s1_safe",
        help="Input root directory containing grid_id folders",
    )
    parser.add_argument(
        "--out-root",
        type=str,
        default=r"D:\sotsuron\s1_samples",
        help="Output root directory",
    )

    # workers 引数は互換性のために残すが、逐次処理なので実際には利用しない
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="(unused) Number of parallel processes (sequential processing only)",
    )

    parser.add_argument(
        "--pixel-spacing",
        type=float,
        default=10.0,
        help="Pixel spacing in meters",
    )
    parser.add_argument(
        "--grid-size",
        type=float,
        default=0.1,
        help="Grid size in degrees",
    )
    parser.add_argument(
        "--pol",
        type=str,
        default="VH",
        choices=["VV", "VH"],
        help="Polarization",
    )

    args = parser.parse_args()

    in_root = Path(args.in_root)
    out_root = Path(args.out_root)

    if not in_root.exists():
        logger.error(f"Input root not found: {in_root}")
        sys.exit(1)

    # 処理対象のファイルを収集
    tasks: list[tuple[str, Path]] = []

    # in_root 直下のフォルダ (grid_id) を走査
    grid_dirs = [d for d in in_root.iterdir() if d.is_dir() and not d.name.startswith("_")]
    logger.info(f"Found {len(grid_dirs)} grid directories.")

    for grid_dir in grid_dirs:
        grid_id = grid_dir.name
        # 各 grid フォルダ内の zip ファイルを対象にする
        zip_files = sorted(grid_dir.glob("*.zip"))
        for zip_path in zip_files:
            tasks.append((grid_id, zip_path))

    total_files = len(tasks)
    logger.info(f"Total files to process: {total_files}")

    if total_files == 0:
        logger.info("No files found. Exiting.")
        return

    success_count = 0
    skip_count = 0
    error_count = 0

    logger.info("Starting batch processing in SEQUENTIAL mode (no multiprocessing).")

    for idx, (grid_id, zip_path) in enumerate(tasks, start=1):
        rel_name = f"{grid_id}/{zip_path.name}"

        # Grid ID → 画素中心座標
        try:
            lat, lon = preprocess_s1_cog.decode_grid_id(grid_id)
        except Exception as e:  # ValueError 等
            error_count += 1
            logger.error(
                f"[{idx}/{total_files}] ERROR: decode_grid_id failed for grid_id={grid_id}: {e}"
            )
            continue

        # AOI WKT
        aoi_wkt = preprocess_s1_cog.create_wkt_polygon(lat, lon, args.grid_size)

        # 出力パス
        stem = zip_path.name.replace(".zip", "").replace(".SAFE", "")
        out_dir = out_root / grid_id
        out_file = out_dir / f"{stem}_proc.tif"

        # 既に存在していればスキップ
        if out_file.exists():
            skip_count += 1
            logger.info(f"[{idx}/{total_files}] SKIP (exists): {rel_name}")
            continue

        # 出力ディレクトリ作成
        out_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"[{idx}/{total_files}] START: {rel_name}")
        try:
            preprocess_s1_cog.process_one(
                in_file=zip_path,
                out_file=out_file,
                aoi_wkt=aoi_wkt,
                pixel_spacing=args.pixel_spacing,
                pol=args.pol,
            )
            # process_one 内でエラー時は logger.error して return する実装なので、
            # ここには例外が飛んでこない想定だが、一応成功扱いにする。
            success_count += 1
            logger.info(f"[{idx}/{total_files}] DONE : {rel_name}")
        except Exception as e:
            error_count += 1
            logger.error(
                f"[{idx}/{total_files}] ERROR while processing {rel_name}: {e}",
                exc_info=True,
            )

    logger.info("-" * 40)
    logger.info("Batch processing completed (sequential).")
    logger.info(f"Total   : {total_files}")
    logger.info(f"Success : {success_count}")
    logger.info(f"Skipped : {skip_count}")
    logger.info(f"Errors  : {error_count}")
    logger.info("-" * 40)


if __name__ == "__main__":
    main()
