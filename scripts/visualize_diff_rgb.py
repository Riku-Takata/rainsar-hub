"""
visualize_diff_rgb.py

平成30年7月豪雨 Sentinel-1 後方散乱差分画像の RGB カラー合成可視化

diff_*.tif (Band1=diff_VV, Band2=diff_VH, dB) と
mask_*.tif (0=非浸水, 1=浸水, 255=NoData) から:
  R = diff_VV (正規化)
  G = diff_VH (正規化)
  B = 浸水マスク強調 + diff_VH 負成分

フロー:
  - 各 orbit ディレクトリを処理
  - 同一 orbit 内の複数シーンを空間的にモザイク合成
  - PNG で保存 (output/h30_july/inundation/rgb_vis/)
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["font.family"] = "Yu Gothic"
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
from matplotlib.colors import Normalize
import rasterio
import rasterio.io
from rasterio.merge import merge as rio_merge
from rasterio.enums import Resampling

# ─── 設定 ──────────────────────────────────────────────────────────
INUND_DIR = Path("output/h30_july/inundation")
OUT_DIR   = INUND_DIR / "rgb_vis"
OUT_DIR.mkdir(exist_ok=True)

# 出力解像度 (短辺のピクセル数 上限)
MAX_PX = 1200

# dB クリッピング範囲 (パーセンタイル)
CLIP_PCT_LO = 2
CLIP_PCT_HI = 98

# 各軌道の日本語ラベル
ORBIT_LABEL = {
    "s1a_orbit046": "S1A 降軌道 046\n(2018-07-07 日本海側〜東北)",
    "s1a_orbit054": "S1A 昇軌道 054\n(2018-07-08 九州北部〜山口)",
    "s1b_orbit098": "S1B 降軌道 098\n(2018-07-05 沖縄・奄美)",
    "s1b_orbit112": "S1B 昇軌道 112\n(2018-07-06 中国地方)",
    "s1b_orbit119": "S1B 降軌道 119\n(2018-07-06 北海道)",
    "s1b_orbit127": "S1B 昇軌道 127\n(2018-07-07 沖縄)",
    "s1b_orbit141": "S1B 昇軌道 141\n(2018-07-08 関東〜東北)",
    "s1b_orbit156": "S1B 降軌道 156\n(2018-07-09 九州南部)",
}


def stretch(arr: np.ndarray, lo_pct: float, hi_pct: float) -> np.ndarray:
    """NaN を無視してパーセンタイルクリッピング → [0, 1] 正規化"""
    valid = arr[~np.isnan(arr)]
    if valid.size == 0:
        return np.zeros_like(arr)
    lo = np.percentile(valid, lo_pct)
    hi = np.percentile(valid, hi_pct)
    if hi == lo:
        return np.zeros_like(arr)
    out = np.clip((arr - lo) / (hi - lo), 0, 1)
    return out


def load_and_merge(diff_paths: list[Path], mask_paths: list[Path],
                   max_px: int = MAX_PX):
    """
    複数の diff TIF をメモリ効率よく読み込み・モザイク合成する。
    戻り値: (vv, vh, flood_mask, transform, crs)
    """
    # 個別ファイルをダウンサンプルして読み込む
    vv_arrays, vh_arrays, mask_arrays = [], [], []
    profiles = []

    for dp, mp in zip(diff_paths, mask_paths):
        with rasterio.open(dp) as src:
            h, w = src.height, src.width
            scale = max_px / max(h, w)
            oh = max(1, int(h * scale))
            ow = max(1, int(w * scale))
            vv = src.read(1, out_shape=(oh, ow), resampling=Resampling.average).astype(np.float32)
            vh = src.read(2, out_shape=(oh, ow), resampling=Resampling.average).astype(np.float32)
            transform = src.transform * src.transform.scale(w / ow, h / oh)
            meta = src.meta.copy()
            meta.update(height=oh, width=ow, transform=transform, count=2, nodata=np.nan)
            profiles.append(meta)
            vv_arrays.append(vv)
            vh_arrays.append(vh)

        with rasterio.open(mp) as src:
            mo = src.read(1, out_shape=(oh, ow), resampling=Resampling.nearest).astype(np.uint8)
            mask_arrays.append(mo)

    if len(diff_paths) == 1:
        return (vv_arrays[0], vh_arrays[0], mask_arrays[0],
                profiles[0]["transform"], profiles[0]["crs"])

    # 複数シーンはピクセルごとに有効データを優先しながらマージ
    # 各配列の bounds を取得してキャンバスに貼り合わせる (簡易実装)
    # rasterio.merge を使うため一時的にメモリデータセットを作成
    def to_memfile(data2: np.ndarray, meta: dict) -> rasterio.io.MemoryFile:
        mf = rasterio.io.MemoryFile()
        with mf.open(**meta) as ds:
            ds.write(data2[0], 1)
            ds.write(data2[1], 2)
        return mf

    mfs = []
    for vv, vh, meta in zip(vv_arrays, vh_arrays, profiles):
        stk = np.stack([vv, vh])
        mf = to_memfile(stk, meta)
        mfs.append(mf)

    opened = [mf.open() for mf in mfs]
    merged, out_transform = rio_merge(opened, nodata=np.nan, method="first")
    for ds in opened:
        ds.close()
    for mf in mfs:
        mf.close()

    merged_vv = merged[0]
    merged_vh = merged[1]

    # mask も同様にマージ (浸水=1 を優先)
    def to_memfile_mask(data: np.ndarray, meta: dict) -> rasterio.io.MemoryFile:
        m2 = meta.copy()
        m2.update(count=1, dtype="uint8", nodata=255)
        mf = rasterio.io.MemoryFile()
        with mf.open(**m2) as ds:
            ds.write(data, 1)
        return mf

    mfs_m = []
    for mo, meta in zip(mask_arrays, profiles):
        mf = to_memfile_mask(mo, meta)
        mfs_m.append(mf)

    opened_m = [mf.open() for mf in mfs_m]
    merged_m, _ = rio_merge(opened_m, nodata=255, method="max")
    for ds in opened_m:
        ds.close()
    for mf in mfs_m:
        mf.close()

    return (merged_vv, merged_vh, merged_m[0], out_transform, profiles[0]["crs"])


def make_rgb(vv: np.ndarray, vh: np.ndarray,
             flood_mask: np.ndarray) -> np.ndarray:
    """
    3チャンネル RGB 画像を生成 (uint8)。

    R = diff_VV 正規化 (高い=VV増加=赤, 低い=VV低下=暗)
    G = diff_VH 正規化 (高い=VH増加=緑, 低い=VH低下=暗)
    B = diff_VH の負成分強調 + 浸水マスク (浸水域=明青)

    解釈:
      - 暗色 (R↓G↓B↓): 両偏波とも低下 → 浸水候補
      - 明青 (R↓G↓B↑): 浸水マスクが検出した浸水域
      - 黄〜橙 (R↑G↑B↓): 後方散乱増加 (都市・農地)
    """
    r_f = stretch(vv, CLIP_PCT_LO, CLIP_PCT_HI)
    g_f = stretch(vh, CLIP_PCT_LO, CLIP_PCT_HI)

    # B: VH 低下成分を青で強調 (低下 → 1, 増加 → 0)
    vh_norm = stretch(vh, CLIP_PCT_LO, CLIP_PCT_HI)
    b_f = 1.0 - vh_norm  # VH が低いほど青く

    # 浸水マスクで確認された画素を鮮やかな青に上書き
    flood_px = (flood_mask == 1)
    b_f[flood_px] = 1.0
    r_f[flood_px] = 0.0
    g_f[flood_px] = 0.3

    # NaN → 灰色 (0.5)
    nan_px = np.isnan(vv) | np.isnan(vh)
    for ch in (r_f, g_f, b_f):
        ch[nan_px] = 0.5

    rgb = np.stack([r_f, g_f, b_f], axis=-1)
    return (np.clip(rgb, 0, 1) * 255).astype(np.uint8)


def process_orbit(orbit_dir: Path):
    orbit_name = orbit_dir.name
    diff_files = sorted(orbit_dir.glob("diff_*.tif"))
    mask_files = sorted(orbit_dir.glob("mask_*.tif"))

    if not diff_files:
        print(f"  [{orbit_name}] diff ファイルなし → スキップ")
        return

    # diff と mask のペアを合わせる
    diff_stems = {f.stem.replace("diff_", ""): f for f in diff_files}
    mask_stems = {f.stem.replace("mask_", ""): f for f in mask_files}
    common = sorted(set(diff_stems) & set(mask_stems))
    if not common:
        print(f"  [{orbit_name}] ペアなし → スキップ")
        return

    paired_diff = [diff_stems[k] for k in common]
    paired_mask = [mask_stems[k] for k in common]

    print(f"  [{orbit_name}] {len(common)} シーンを処理中...")
    vv, vh, flood_mask, transform, crs = load_and_merge(paired_diff, paired_mask)

    # 有効データ統計
    valid = ~np.isnan(vv)
    n_flood = int((flood_mask == 1).sum())
    vv_valid = vv[valid]
    vh_valid = vh[valid]

    print(f"    有効ピクセル: {valid.sum():,}  浸水検出: {n_flood:,}")
    if vv_valid.size > 0:
        print(f"    VV diff: mean={vv_valid.mean():.2f} dB  VH diff: mean={vh_valid.mean():.2f} dB")

    # RGB 生成
    rgb = make_rgb(vv, vh, flood_mask)

    # ── プロット ─────────────────────────────────────────────────────
    h, w = rgb.shape[:2]
    figw = 12
    figh = figw * h / w + 2.5
    fig, ax = plt.subplots(figsize=(figw, figh), dpi=150)

    # 空間座標で表示
    with rasterio.open(paired_diff[0]) as src0:
        bounds = src0.bounds
    extent = [bounds.left, bounds.right, bounds.bottom, bounds.top]

    ax.imshow(rgb, extent=extent, origin="upper", interpolation="bilinear", aspect="auto")

    # グリッド線
    ax.set_xlabel("経度 (°E)", fontsize=11)
    ax.set_ylabel("緯度 (°N)", fontsize=11)
    gl = ax.grid(True, linestyle="--", linewidth=0.4, color="white", alpha=0.5)

    # タイトル
    label = ORBIT_LABEL.get(orbit_name, orbit_name)
    sensing_dates = set()
    for p in paired_diff:
        m = re.search(r"(\d{8})T\d{6}", p.name)
        if m:
            d = m.group(1)
            sensing_dates.add(f"{d[:4]}-{d[4:6]}-{d[6:]}")
    date_str = ", ".join(sorted(sensing_dates))
    ax.set_title(
        f"Sentinel-1 後方散乱強度差分 RGB カラー合成\n"
        f"{label}  |  観測日: {date_str}\n"
        f"浸水検出画素数: {n_flood:,}",
        fontsize=12, pad=10
    )

    # 凡例
    legend_patches = [
        mpatches.Patch(facecolor=(0, 0.3, 1), label="浸水検出域 (VV↓ & VH↓ & VV<-15 dB)"),
        mpatches.Patch(facecolor=(0.1, 0.1, 0.1), label="後方散乱低下 (変化あり)"),
        mpatches.Patch(facecolor=(0.9, 0.8, 0.1), label="後方散乱増加"),
        mpatches.Patch(facecolor=(0.5, 0.5, 0.5), label="NoData / 海域"),
    ]
    ax.legend(handles=legend_patches, loc="lower right", fontsize=9,
              framealpha=0.85, edgecolor="gray")

    # カラーバー (diff_VV の参考スケール)
    sm_vv = plt.cm.ScalarMappable(
        cmap="RdYlGn_r",
        norm=Normalize(
            vmin=float(np.percentile(vv[~np.isnan(vv)], CLIP_PCT_LO)) if vv[~np.isnan(vv)].size else -10,
            vmax=float(np.percentile(vv[~np.isnan(vv)], CLIP_PCT_HI)) if vv[~np.isnan(vv)].size else 10,
        )
    )
    sm_vv.set_array([])
    cbar = fig.colorbar(sm_vv, ax=ax, orientation="vertical",
                        fraction=0.025, pad=0.01, shrink=0.6)
    cbar.set_label("参考: diff_VV (dB)", fontsize=9)

    plt.tight_layout()
    out_path = OUT_DIR / f"{orbit_name}_rgb.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"    → {out_path}")


def make_overview():
    """全 orbit の RGB を並べた概観図を生成"""
    pngs = sorted(OUT_DIR.glob("*_rgb.png"))
    if not pngs:
        return

    n = len(pngs)
    cols = min(3, n)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(cols * 7, rows * 5.5), dpi=120)
    axes = np.array(axes).flatten()

    for ax, png in zip(axes, pngs):
        img = plt.imread(png)
        ax.imshow(img)
        ax.set_title(png.stem.replace("_rgb", ""), fontsize=9)
        ax.axis("off")

    for ax in axes[len(pngs):]:
        ax.axis("off")

    fig.suptitle(
        "平成30年7月豪雨 Sentinel-1 後方散乱差分 RGB カラー合成\n"
        "R=diff_VV / G=diff_VH / B=VH低下強調+浸水マスク",
        fontsize=13, y=1.02
    )
    plt.tight_layout()
    out_path = OUT_DIR / "overview_all_orbits.png"
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"\n概観図: {out_path}")


def main():
    print("=" * 60)
    print("平成30年7月豪雨 差分 RGB 可視化")
    print("=" * 60)

    orbit_dirs = sorted(d for d in INUND_DIR.iterdir() if d.is_dir())
    print(f"\n対象 orbit: {len(orbit_dirs)} ディレクトリ\n")

    for od in orbit_dirs:
        process_orbit(od)

    make_overview()
    print("\n完了")


if __name__ == "__main__":
    main()
