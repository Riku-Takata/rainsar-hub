from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import from_bounds


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
BBOX_CSV = ROOT / "output/kurume_tif_bboxes/kurume_tif_valid_bboxes.csv"
OUT = ROOT / "output/kurume_tif_bboxes/kurume01_satellite_clips"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

RASTERS = {
    "0-3h": BASE / "map7_mean_diff_0_3h.tif",
    "3-6h": BASE / "map7_mean_diff_3_6h.tif",
    "6-12h": BASE / "map7_mean_diff_6_12h.tif",
    "12-24h": BASE / "map7_mean_diff_12_24h.tif",
}
TRUTH = BASE / "bbox_per_tif_paddy_pixel_polygon/per_tif/Kurume01_inun/Kurume01_inun_truth_mask.tif"


def setup_font() -> font_manager.FontProperties:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    else:
        prop = font_manager.FontProperties()
    plt.rcParams["axes.unicode_minus"] = False
    return prop


FONT = setup_font()


def read_bbox() -> tuple[float, float, float, float]:
    df = pd.read_csv(BBOX_CSV)
    row = df[df["tif"] == "Kurume01_inun.tif"].iloc[0]
    return float(row["valid_left"]), float(row["valid_bottom"]), float(row["valid_right"]), float(row["valid_top"])


def read_clip(path: Path, bbox: tuple[float, float, float, float]) -> tuple[np.ndarray, rasterio.Affine, rasterio.coords.BoundingBox]:
    left, bottom, right, top = bbox
    with rasterio.open(path) as src:
        window = from_bounds(left, bottom, right, top, transform=src.transform)
        window = window.round_offsets().round_lengths()
        arr = src.read(1, window=window).astype(np.float32)
        nodata = src.nodata
        transform = src.window_transform(window)
        bounds = rasterio.windows.bounds(window, src.transform)
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr, transform, rasterio.coords.BoundingBox(*bounds)


def save_clip_tif(path: Path, out_path: Path, bbox: tuple[float, float, float, float], dtype: str = "float32") -> None:
    left, bottom, right, top = bbox
    with rasterio.open(path) as src:
        window = from_bounds(left, bottom, right, top, transform=src.transform)
        window = window.round_offsets().round_lengths()
        arr = src.read(1, window=window)
        profile = src.profile.copy()
        profile.update(
            height=arr.shape[0],
            width=arr.shape[1],
            transform=src.window_transform(window),
            count=1,
            dtype=dtype,
            compress="deflate",
        )
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(arr.astype(dtype), 1)


def plot_diff(arr: np.ndarray, bounds: rasterio.coords.BoundingBox, title: str, out_path: Path) -> None:
    valid = arr[np.isfinite(arr)]
    if len(valid):
        lim = float(np.nanpercentile(np.abs(valid), 98))
        lim = max(lim, 1.0)
    else:
        lim = 1.0
    fig, ax = plt.subplots(figsize=(6.2, 6.0))
    im = ax.imshow(
        arr,
        extent=[bounds.left, bounds.right, bounds.bottom, bounds.top],
        origin="upper",
        cmap="RdBu_r",
        vmin=-lim,
        vmax=lim,
    )
    ax.set_title(title, fontproperties=FONT)
    ax.set_xlabel("経度", fontproperties=FONT)
    ax.set_ylabel("緯度", fontproperties=FONT)
    fig.colorbar(im, ax=ax, label="target - before (dB)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=240, bbox_inches="tight")
    plt.close()


def plot_truth(arr: np.ndarray, bounds: rasterio.coords.BoundingBox, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(6.2, 6.0))
    im = ax.imshow(
        arr,
        extent=[bounds.left, bounds.right, bounds.bottom, bounds.top],
        origin="upper",
        cmap="Reds",
        vmin=0,
        vmax=1,
    )
    ax.set_title("Kurume01 正解浸水域マスク", fontproperties=FONT)
    ax.set_xlabel("経度", fontproperties=FONT)
    ax.set_ylabel("緯度", fontproperties=FONT)
    fig.colorbar(im, ax=ax, label="truth")
    plt.tight_layout()
    plt.savefig(out_path, dpi=240, bbox_inches="tight")
    plt.close()


def plot_panel(clips: dict[str, tuple[np.ndarray, rasterio.coords.BoundingBox]], truth: tuple[np.ndarray, rasterio.coords.BoundingBox] | None) -> None:
    n = len(clips) + (1 if truth is not None else 0)
    cols = 3
    rows = int(np.ceil(n / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(5.3 * cols, 5.0 * rows))
    axes = np.atleast_1d(axes).ravel()
    all_valid = np.concatenate([arr[np.isfinite(arr)] for arr, _ in clips.values() if np.isfinite(arr).any()])
    lim = float(np.nanpercentile(np.abs(all_valid), 98)) if len(all_valid) else 1.0
    lim = max(lim, 1.0)
    for ax, (label, (arr, bounds)) in zip(axes, clips.items()):
        im = ax.imshow(
            arr,
            extent=[bounds.left, bounds.right, bounds.bottom, bounds.top],
            origin="upper",
            cmap="RdBu_r",
            vmin=-lim,
            vmax=lim,
        )
        ax.set_title(f"差分 {label}", fontproperties=FONT)
        ax.set_xlabel("経度", fontproperties=FONT)
        ax.set_ylabel("緯度", fontproperties=FONT)
    used = len(clips)
    if truth is not None:
        truth_arr, bounds = truth
        axes[used].imshow(
            truth_arr,
            extent=[bounds.left, bounds.right, bounds.bottom, bounds.top],
            origin="upper",
            cmap="Reds",
            vmin=0,
            vmax=1,
        )
        axes[used].set_title("正解浸水域", fontproperties=FONT)
        used += 1
    for ax in axes[used:]:
        ax.axis("off")
    fig.colorbar(im, ax=axes[: len(clips)], fraction=0.025, pad=0.02, label="target - before (dB)")
    fig.suptitle("Kurume01 bbox内 Sentinel-1 差分画像", fontproperties=FONT, fontsize=16)
    plt.tight_layout()
    plt.savefig(OUT / "Kurume01_bbox_satellite_diff_panel.png", dpi=240, bbox_inches="tight")
    plt.close()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    bbox = read_bbox()
    rows = []
    clips: dict[str, tuple[np.ndarray, rasterio.coords.BoundingBox]] = {}
    for label, path in RASTERS.items():
        arr, _, bounds = read_clip(path, bbox)
        clips[label] = (arr, bounds)
        safe = label.replace("-", "_")
        plot_diff(arr, bounds, f"Kurume01 bbox内 Sentinel-1 差分 {label}", OUT / f"Kurume01_bbox_diff_{safe}.png")
        save_clip_tif(path, OUT / f"Kurume01_bbox_diff_{safe}.tif", bbox)
        rows.append(
            {
                "label": label,
                "source": str(path),
                "png": f"Kurume01_bbox_diff_{safe}.png",
                "tif": f"Kurume01_bbox_diff_{safe}.tif",
                "height": arr.shape[0],
                "width": arr.shape[1],
                "mean": float(np.nanmean(arr)),
                "std": float(np.nanstd(arr)),
                "min": float(np.nanmin(arr)),
                "max": float(np.nanmax(arr)),
            }
        )

    truth_clip = None
    if TRUTH.exists():
        truth_arr, _, truth_bounds = read_clip(TRUTH, bbox)
        truth_clip = (truth_arr, truth_bounds)
        plot_truth(truth_arr, truth_bounds, OUT / "Kurume01_bbox_truth_mask.png")
        save_clip_tif(TRUTH, OUT / "Kurume01_bbox_truth_mask.tif", bbox, dtype="uint8")

    plot_panel(clips, truth_clip)
    pd.DataFrame(rows).to_csv(OUT / "Kurume01_bbox_satellite_clip_summary.csv", index=False, encoding="utf-8-sig")
    readme = [
        "# Kurume01 bbox clipped satellite images",
        "",
        f"- bbox: left={bbox[0]:.8f}, bottom={bbox[1]:.8f}, right={bbox[2]:.8f}, top={bbox[3]:.8f}",
        "- PNGはSentinel-1の `target - before` 差分画像です。",
        "- `*_panel.png` は4時間帯の差分と正解浸水域をまとめた図です。",
        "",
    ]
    (OUT / "README.md").write_text("\n".join(readme), encoding="utf-8-sig")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
