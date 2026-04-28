"""
detect_inundation.py
平成30年7月豪雨 Sentinel-1 後方散乱強度差分による内水氾濫域検出 (改訂版)

イベントシーン(2018-07-05〜09) と参照シーン(前日晴天日) の
Sigma0_VV/VH差分 (dB) を算出し、複合条件で浸水候補を抽出する。

改善点:
  1. 日本陸域ポリゴンマスクで海域・湖沼を除外
  2. 双偏波 (VV + VH) 両方の低下を要求
  3. 絶対値閾値を -15 dB に厳格化（開水面水準）
  4. 参照時点が既に水域の画素を除外 (ref_VV > -20 dB)
  5. temporal baseline > 60 days の軌道を警告・除外オプション

NoData: 0 (SNAP terrain correction出力)
Band1: Sigma0_VV [dB], Band2: Sigma0_VH [dB]
"""

import re
import logging
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import numpy as np
import rasterio
from rasterio.windows import Window, from_bounds
from rasterio.features import rasterize
from rasterio.merge import merge as rio_merge
import geopandas as gpd
from shapely.geometry import box, mapping
from scipy.ndimage import binary_opening, binary_closing

# ─────────────────────────────────────────────────────────────────
# 設定
# ─────────────────────────────────────────────────────────────────
EVENT_DIR    = Path("output/h30_july/s1_processed")
REF_DIR      = Path("output/h30_july/s1_ref_processed")
OUT_DIR      = Path("output/h30_july/inundation")

# 浸水判定 複合条件
DIFF_VV_THRESHOLD = -3.0   # VV 差分閾値 (dB): event - ref < この値
DIFF_VH_THRESHOLD = -2.0   # VH 差分閾値 (dB): event - ref < この値
WATER_VV_MAX_DB   = -15.0  # event VV の上限 (dB): 開水面の絶対値条件
PERM_WATER_DB     = -20.0  # ref VV がこれ以下なら参照時点から水域 → 除外

# temporal baseline が長い軌道は信頼性低下
MAX_RELIABLE_BASELINE_DAYS = 30  # これを超える軌道は警告を出して除外
EXCLUDE_UNRELIABLE = True         # True: 除外する / False: 警告のみ

# 形態学的フィルタ
MORPH_SIZE = 3  # 3×3 structuring element

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("detect_inundation.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# 日本陸域ポリゴン取得
# ─────────────────────────────────────────────────────────────────

def get_japan_polygon():
    """Natural Earth から日本陸域ポリゴンを取得 (cartopy 経由)"""
    # 1) cartopy natural_earth shapereader
    try:
        import cartopy.io.shapereader as shpreader
        from shapely.ops import unary_union
        shp_path = shpreader.natural_earth(
            resolution="10m", category="cultural", name="admin_0_countries"
        )
        reader = shpreader.Reader(shp_path)
        japan_geoms = [
            rec.geometry for rec in reader.records()
            if rec.attributes.get("ADM0_ISO") == "JPN"
            or rec.attributes.get("ISO_A3") == "JPN"
            or rec.attributes.get("NAME") == "Japan"
        ]
        if japan_geoms:
            geom = unary_union(japan_geoms)
            logger.info("  日本ポリゴン取得 (cartopy natural_earth 10m)")
            return geom
    except Exception as e:
        logger.warning(f"  cartopy 失敗: {e}")

    # 2) geopandas datasets (GeoPandas < 1.0)
    try:
        world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
        japan = world[world["iso_a3"] == "JPN"].union_all()
        logger.info("  日本ポリゴン取得 (geopandas naturalearth_lowres)")
        return japan
    except Exception as e:
        logger.warning(f"  geopandas datasets 失敗: {e}")

    # 3) フォールバック: 日本近似マルチポリゴン (本州・四国・九州・北海道)
    from shapely.geometry import Polygon
    from shapely.ops import unary_union
    logger.warning("  フォールバック: 日本手動近似ポリゴン")
    honshu   = Polygon([(130,31),(131,31),(132,33),(133,33),(134,34),(136,35),
                         (137,35),(138,36),(139,36),(141,37),(141,38),(141,40),
                         (141,41),(140,42),(138,42),(136,36),(135,35),(134,34),
                         (133,33),(132,33),(131,32),(130,31)])
    hokkaido = Polygon([(141,41),(142,42),(143,43),(144,44),(145,44),(145,43),
                         (143,42),(141,42),(141,41)])
    kyushu   = Polygon([(130,31),(131,31),(132,31),(132,32),(130,33),(130,32),(130,31)])
    shikoku  = Polygon([(132,33),(133,33),(134,33),(134,34),(133,34),(132,33)])
    return unary_union([honshu, hokkaido, kyushu, shikoku])


# ─────────────────────────────────────────────────────────────────
# ファイル名パース
# ─────────────────────────────────────────────────────────────────
FNAME_RE = re.compile(
    r"^(S1[AB])_IW_GRDH_1SDV_"
    r"(\d{8}T\d{6})_\d{8}T\d{6}_"
    r"(\d{6})_"
    r"([0-9A-F]{6})_"
    r"[0-9A-F]{4}_proc\.tif$",
    re.IGNORECASE,
)


@dataclass
class Scene:
    path: Path
    platform: str
    sensing_dt: datetime
    abs_orbit: int
    rel_orbit: int


def rel_orbit_from_abs(platform: str, abs_orbit: int) -> int:
    if platform.upper() == "S1A":
        return (abs_orbit - 73) % 175 + 1
    return (abs_orbit - 27) % 175 + 1


def parse_scene(path: Path) -> Optional[Scene]:
    m = FNAME_RE.match(path.name)
    if not m:
        return None
    plat, start_dt, abs_orb_str, _ = m.groups()
    abs_orb = int(abs_orb_str)
    rel_orb = rel_orbit_from_abs(plat, abs_orb)
    sensing_dt = datetime.strptime(start_dt, "%Y%m%dT%H%M%S")
    return Scene(
        path=path,
        platform=plat.upper(),
        sensing_dt=sensing_dt,
        abs_orbit=abs_orb,
        rel_orbit=rel_orb,
    )


def scan_dir(directory: Path) -> list[Scene]:
    scenes = []
    for p in sorted(directory.glob("*_proc.tif")):
        s = parse_scene(p)
        if s:
            scenes.append(s)
    logger.info(f"  {directory.name}: {len(scenes)} シーン検出")
    return scenes


# ─────────────────────────────────────────────────────────────────
# 陸域マスクのラスタライズ
# ─────────────────────────────────────────────────────────────────

def rasterize_land_mask(japan_geom, transform, height, width, crs) -> np.ndarray:
    """日本ポリゴンを画素グリッドにラスタライズ (陸域=1, 海域=0)"""
    if japan_geom is None:
        return np.ones((height, width), dtype=bool)

    shapes = [(mapping(japan_geom), 1)]
    mask = rasterize(
        shapes,
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype=np.uint8,
        all_touched=False,
    )
    return mask.astype(bool)


# ─────────────────────────────────────────────────────────────────
# 差分計算コア
# ─────────────────────────────────────────────────────────────────

def compute_diff_and_mask(
    event_path: Path,
    ref_path: Path,
    out_diff_path: Path,
    out_mask_path: Path,
    japan_geom,
) -> Optional[dict]:
    with rasterio.open(event_path) as ev_ds, rasterio.open(ref_path) as ref_ds:
        if ev_ds.crs != ref_ds.crs:
            raise ValueError(f"CRS不一致: {ev_ds.crs} vs {ref_ds.crs}")

        # 共通 bbox
        ev_b, ref_b = ev_ds.bounds, ref_ds.bounds
        left   = max(ev_b.left,   ref_b.left)
        bottom = max(ev_b.bottom, ref_b.bottom)
        right  = min(ev_b.right,  ref_b.right)
        top    = min(ev_b.top,    ref_b.top)

        if left >= right or bottom >= top:
            logger.warning(f"    空間重複なし: {event_path.name}")
            return None

        ev_win  = from_bounds(left, bottom, right, top, ev_ds.transform).round_lengths().round_offsets()
        ref_win = from_bounds(left, bottom, right, top, ref_ds.transform).round_lengths().round_offsets()

        rows = min(int(ev_win.height), int(ref_win.height))
        cols = min(int(ev_win.width),  int(ref_win.width))

        ev_win2  = Window(int(ev_win.col_off),  int(ev_win.row_off),  cols, rows)
        ref_win2 = Window(int(ref_win.col_off), int(ref_win.row_off), cols, rows)

        ev_vv  = ev_ds.read(1, window=ev_win2).astype(np.float32)
        ev_vh  = ev_ds.read(2, window=ev_win2).astype(np.float32)
        ref_vv = ref_ds.read(1, window=ref_win2).astype(np.float32)
        ref_vh = ref_ds.read(2, window=ref_win2).astype(np.float32)

        out_transform = ev_ds.window_transform(ev_win2)
        out_meta = ev_ds.meta.copy()
        out_meta.update(width=cols, height=rows, transform=out_transform, nodata=np.nan)

    # ── valid マスク (SNAP NoData = 0) ──
    valid = (ev_vv != 0) & (ref_vv != 0) & (ev_vh != 0) & (ref_vh != 0)

    # ── 陸域マスク ──
    land_mask = rasterize_land_mask(japan_geom, out_transform, rows, cols, out_meta["crs"])

    # ── dB 差分 ──
    diff_vv = np.where(valid, ev_vv - ref_vv, np.nan).astype(np.float32)
    diff_vh = np.where(valid, ev_vh - ref_vh, np.nan).astype(np.float32)

    # ── 差分ラスタ保存 ──
    out_diff_path.parent.mkdir(parents=True, exist_ok=True)
    diff_meta = out_meta.copy()
    diff_meta.update(count=2, dtype="float32")
    with rasterio.open(out_diff_path, "w", **diff_meta) as dst:
        dst.write(diff_vv, 1)
        dst.write(diff_vh, 2)

    # ── 複合浸水判定 ──
    # 1) 有効ピクセル
    # 2) 陸域のみ (海域除外)
    # 3) VV 差分 < -3 dB (有意な後方散乱低下)
    # 4) VH 差分 < -2 dB (双偏波確認)
    # 5) event VV < -15 dB (開水面の絶対値条件)
    # 6) ref VV > -20 dB (参照時点は水域でなかった = 変化検出)
    inund_raw = (
        valid
        & land_mask
        & (diff_vv < DIFF_VV_THRESHOLD)
        & (diff_vh < DIFF_VH_THRESHOLD)
        & (ev_vv   < WATER_VV_MAX_DB)
        & (ref_vv  > PERM_WATER_DB)
    )

    # 形態学的ノイズ除去
    struct = np.ones((MORPH_SIZE, MORPH_SIZE), dtype=bool)
    inund_clean = binary_opening(inund_raw, structure=struct)
    inund_clean = binary_closing(inund_clean, structure=struct)

    # uint8 保存 (0=非浸水, 1=浸水, 255=NoData/海域)
    mask_arr = inund_clean.astype(np.uint8)
    mask_arr[~valid] = 255

    out_mask_path.parent.mkdir(parents=True, exist_ok=True)
    mask_meta = out_meta.copy()
    mask_meta.update(count=1, dtype="uint8", nodata=255)
    with rasterio.open(out_mask_path, "w", **mask_meta) as dst:
        dst.write(mask_arr, 1)

    n_valid_land = int((valid & land_mask).sum())
    n_inund      = int(inund_clean.sum())
    area_km2     = n_inund * 100 / 1e6  # 10m × 10m = 100 m²
    logger.info(
        f"    陸域有効px: {n_valid_land:,}  浸水候補px: {n_inund:,}  "
        f"推定面積: {area_km2:.2f} km²"
    )
    return {"n_valid_land": n_valid_land, "n_inundated": n_inund, "area_km2": area_km2}


# ─────────────────────────────────────────────────────────────────
# メイン
# ─────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Sentinel-1 後方散乱差分 浸水域検出 (改訂版)")
    logger.info(f"  VV差分閾値    : {DIFF_VV_THRESHOLD} dB")
    logger.info(f"  VH差分閾値    : {DIFF_VH_THRESHOLD} dB")
    logger.info(f"  event VV上限  : {WATER_VV_MAX_DB} dB")
    logger.info(f"  常時水域除外  : ref VV > {PERM_WATER_DB} dB")
    logger.info("=" * 60)

    # 日本ポリゴン
    logger.info("日本陸域ポリゴン取得中...")
    japan_geom = get_japan_polygon()

    # シーン一覧
    logger.info("イベントシーン:")
    ev_scenes  = scan_dir(EVENT_DIR)
    logger.info("参照シーン:")
    ref_scenes = scan_dir(REF_DIR)

    # 相対軌道でグルーピング
    ev_by_orbit  = defaultdict(list)
    ref_by_orbit = defaultdict(list)
    for s in ev_scenes:
        ev_by_orbit[(s.platform, s.rel_orbit)].append(s)
    for s in ref_scenes:
        ref_by_orbit[(s.platform, s.rel_orbit)].append(s)

    common_orbits = sorted(set(ev_by_orbit) & set(ref_by_orbit))
    logger.info(f"\n対応軌道: {len(common_orbits)} 軌道")

    all_mask_paths = []
    csv_rows = []

    for (plat, rel_orb) in common_orbits:
        ev_list  = sorted(ev_by_orbit[(plat, rel_orb)],  key=lambda s: s.sensing_dt)
        ref_list = sorted(ref_by_orbit[(plat, rel_orb)], key=lambda s: s.sensing_dt)

        # temporal baseline (最初の ev - 最後の ref の日数差)
        baseline_days = (ev_list[0].sensing_dt - ref_list[-1].sensing_dt).days

        logger.info(f"\n[{plat} | orbit={rel_orb:03d}]")
        logger.info(f"  event: {ev_list[0].sensing_dt.date()}  ref: {ref_list[-1].sensing_dt.date()}")
        logger.info(f"  temporal baseline: {baseline_days} 日")

        if EXCLUDE_UNRELIABLE and abs(baseline_days) > MAX_RELIABLE_BASELINE_DAYS:
            logger.warning(
                f"  !! temporal baseline ({baseline_days}d) > {MAX_RELIABLE_BASELINE_DAYS}d → スキップ"
            )
            logger.warning(f"  (季節変化による偽陽性リスクが高いため除外)")
            continue

        orbit_dir = OUT_DIR / f"{plat.lower()}_orbit{rel_orb:03d}"
        orbit_dir.mkdir(parents=True, exist_ok=True)

        pair_count = 0
        for i, ev in enumerate(ev_list):
            ref = ref_list[min(i, len(ref_list) - 1)]
            out_diff = orbit_dir / f"diff_{ev.path.stem}.tif"
            out_mask = orbit_dir / f"mask_{ev.path.stem}.tif"

            logger.info(f"  [{i+1}/{len(ev_list)}] {ev.path.name}")
            logger.info(f"    vs  {ref.path.name}")

            stats = compute_diff_and_mask(ev.path, ref.path, out_diff, out_mask, japan_geom)
            if stats:
                all_mask_paths.append(out_mask)
                pair_count += 1
                csv_rows.append({
                    "platform": plat,
                    "rel_orbit": rel_orb,
                    "baseline_days": baseline_days,
                    "event_scene": ev.path.name,
                    "ref_scene": ref.path.name,
                    "n_valid_land": stats["n_valid_land"],
                    "n_inundated": stats["n_inundated"],
                    "area_km2": f"{stats['area_km2']:.2f}",
                    "diff_tif": str(out_diff),
                    "mask_tif": str(out_mask),
                })

        logger.info(f"  → {pair_count}/{len(ev_list)} ペア完了")

    # ── モザイク合成 ──
    if all_mask_paths:
        logger.info(f"\nモザイク合成: {len(all_mask_paths)} マスク")
        mosaic_path = OUT_DIR / "inundation_mosaic.tif"
        _build_mosaic(all_mask_paths, mosaic_path)

        with rasterio.open(mosaic_path) as ds:
            arr = ds.read(1)
        n_flooded = int((arr == 1).sum())
        area_km2  = n_flooded * 100 / 1e6
        logger.info(f"\n{'='*60}")
        logger.info(f"全体推定浸水面積 (VV<{DIFF_VV_THRESHOLD}dB, VH<{DIFF_VH_THRESHOLD}dB, ev<{WATER_VV_MAX_DB}dB)")
        logger.info(f"  浸水ピクセル: {n_flooded:,}")
        logger.info(f"  推定面積    : {area_km2:.2f} km²")
        logger.info(f"  出力        : {mosaic_path}")

    # CSV
    import csv
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / "inundation_results.csv"
    if csv_rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_rows[0].keys())
            writer.writeheader()
            writer.writerows(csv_rows)
    logger.info(f"結果CSV: {csv_path}")
    logger.info("完了")


def _build_mosaic(mask_paths: list[Path], out_path: Path):
    """
    タイル書き込みによるメモリ効率的モザイク合成。
    全シーンを一括読み込みせず、1シーンずつ出力ファイルに書き込む。
    """
    from rasterio.transform import from_origin
    from rasterio.windows import from_bounds as win_from_bounds

    # 出力範囲・解像度の決定
    all_bounds = []
    with rasterio.open(mask_paths[0]) as ref:
        ref_crs = ref.crs
        res = ref.transform.a  # degree/pixel

    for p in mask_paths:
        with rasterio.open(p) as ds:
            all_bounds.append(ds.bounds)

    left   = min(b.left   for b in all_bounds)
    bottom = min(b.bottom for b in all_bounds)
    right  = max(b.right  for b in all_bounds)
    top    = max(b.top    for b in all_bounds)

    total_cols = int(round((right - left) / res))
    total_rows = int(round((top  - bottom) / res))
    out_transform = from_origin(left, top, res, res)

    out_meta = {
        "driver": "GTiff", "dtype": "uint8",
        "width": total_cols, "height": total_rows,
        "count": 1, "crs": ref_crs,
        "transform": out_transform, "nodata": 255,
        "compress": "deflate", "tiled": True,
        "blockxsize": 512, "blockysize": 512,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # ── 初期化: nodata (255) で塗りつぶし ──
    # 512×512 ブロック単位で書き込み (メモリ節約)
    BLOCK = 512
    logger.info(f"  出力サイズ: {total_rows} × {total_cols} px")
    with rasterio.open(out_path, "w", **out_meta) as dst:
        for row_start in range(0, total_rows, BLOCK * 32):
            row_end = min(row_start + BLOCK * 32, total_rows)
            chunk_rows = row_end - row_start
            fill = np.full((chunk_rows, total_cols), 255, dtype=np.uint8)
            win = Window(0, row_start, total_cols, chunk_rows)
            dst.write(fill, 1, window=win)

    # ── 各マスクを出力に書き込み (max merge) ──
    for i, p in enumerate(mask_paths):
        with rasterio.open(p) as src:
            src_b = src.bounds
            dst_win = win_from_bounds(
                src_b.left, src_b.bottom, src_b.right, src_b.top,
                out_transform,
            ).round_offsets().round_lengths()

            col_off = int(dst_win.col_off)
            row_off = int(dst_win.row_off)
            w = min(int(dst_win.width),  total_cols - col_off)
            h = min(int(dst_win.height), total_rows - row_off)
            if w <= 0 or h <= 0:
                continue

            src_win = win_from_bounds(
                src_b.left, src_b.bottom, src_b.right, src_b.top,
                src.transform,
            ).round_offsets().round_lengths()
            src_w = min(int(src_win.width),  src.width  - int(src_win.col_off))
            src_h = min(int(src_win.height), src.height - int(src_win.row_off))
            read_w = min(w, src_w)
            read_h = min(h, src_h)

            src_data = src.read(
                1,
                window=Window(int(src_win.col_off), int(src_win.row_off), read_w, read_h),
            )

        dst_win2 = Window(col_off, row_off, read_w, read_h)
        with rasterio.open(out_path, "r+") as dst:
            existing = dst.read(1, window=dst_win2)
            merged = np.where(src_data == 1, 1,
                     np.where(existing == 1, 1,
                     np.where(src_data == 0, 0, existing))).astype(np.uint8)
            dst.write(merged, 1, window=dst_win2)

        logger.info(f"  [{i+1}/{len(mask_paths)}] {p.name} → 書き込み完了")


if __name__ == "__main__":
    main()
