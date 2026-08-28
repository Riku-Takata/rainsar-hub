from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Rectangle
import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window, bounds as window_bounds
from rasterio.warp import transform_bounds


ROOT = Path(__file__).resolve().parents[1]
KURUME_DIR = Path(r"D:\sotsuron\kurume")
OUT_DIR = ROOT / "output" / "kurume_tif_bboxes"
FIG_DIR = OUT_DIR / "figures"
BBOX_DIR = OUT_DIR / "geojson"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")


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


def polygon_from_bounds(bounds: tuple[float, float, float, float]) -> dict:
    left, bottom, right, top = bounds
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [left, bottom],
                [right, bottom],
                [right, top],
                [left, top],
                [left, bottom],
            ]
        ],
    }


def valid_bbox(arr: np.ndarray, transform) -> tuple[tuple[float, float, float, float], tuple[int, int, int, int]]:
    rows, cols = np.where(arr)
    if rows.size == 0:
        raise ValueError("valid pixels are empty")
    row_min = int(rows.min())
    row_max = int(rows.max())
    col_min = int(cols.min())
    col_max = int(cols.max())
    win = Window(col_min, row_min, col_max - col_min + 1, row_max - row_min + 1)
    b = window_bounds(win, transform)
    return (float(b[0]), float(b[1]), float(b[2]), float(b[3])), (row_min, row_max, col_min, col_max)


def read_valid(path: Path) -> tuple[np.ndarray, np.ndarray, dict]:
    with rasterio.open(path) as src:
        data = src.read(1).astype(np.float32)
        profile = src.profile.copy()
        nodata = src.nodata
        full_bounds = tuple(float(v) for v in src.bounds)
        crs = src.crs
        transform = src.transform
    valid = np.isfinite(data)
    if nodata is not None:
        valid &= data != nodata
    profile.update({"full_bounds": full_bounds, "crs": crs, "transform": transform, "nodata": nodata})
    data[~valid] = np.nan
    return data, valid, profile


def plot_single(path: Path, data: np.ndarray, full_bounds: tuple[float, float, float, float], bbox: tuple[float, float, float, float]) -> None:
    left, bottom, right, top = full_bounds
    b_left, b_bottom, b_right, b_top = bbox
    fig, ax = plt.subplots(figsize=(6, 6))
    im = ax.imshow(data, extent=(left, right, bottom, top), origin="upper", cmap="viridis")
    ax.add_patch(
        Rectangle(
            (left, bottom),
            right - left,
            top - bottom,
            fill=False,
            edgecolor="white",
            linewidth=1.4,
            linestyle="--",
            label="TIF全体範囲",
        )
    )
    ax.add_patch(
        Rectangle(
            (b_left, b_bottom),
            b_right - b_left,
            b_top - b_bottom,
            fill=False,
            edgecolor="red",
            linewidth=2.2,
            label="有効画素bbox",
        )
    )
    ax.set_title(f"{path.name} と bbox", fontproperties=FONT)
    ax.set_xlabel("経度", fontproperties=FONT)
    ax.set_ylabel("緯度", fontproperties=FONT)
    ax.legend(prop=FONT, loc="upper right")
    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_fontproperties(FONT)
    fig.colorbar(im, ax=ax, shrink=0.75, label="TIF値")
    fig.tight_layout()
    fig.savefig(FIG_DIR / f"{path.stem}_bbox.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def plot_overview(rows: list[dict]) -> None:
    fig, ax = plt.subplots(figsize=(9, 7))
    for row in rows:
        left = row["valid_left"]
        bottom = row["valid_bottom"]
        right = row["valid_right"]
        top = row["valid_top"]
        ax.add_patch(
            Rectangle(
                (left, bottom),
                right - left,
                top - bottom,
                facecolor="#4c78a8",
                edgecolor="#1f3f5b",
                alpha=0.25,
                linewidth=1.5,
            )
        )
        ax.text(
            (left + right) / 2,
            (bottom + top) / 2,
            row["tif"].replace("_inun.tif", "").replace("Kurume", "K"),
            ha="center",
            va="center",
            fontsize=9,
            fontproperties=FONT,
        )
    min_x = min(r["valid_left"] for r in rows)
    max_x = max(r["valid_right"] for r in rows)
    min_y = min(r["valid_bottom"] for r in rows)
    max_y = max(r["valid_top"] for r in rows)
    pad_x = (max_x - min_x) * 0.08
    pad_y = (max_y - min_y) * 0.08
    ax.set_xlim(min_x - pad_x, max_x + pad_x)
    ax.set_ylim(min_y - pad_y, max_y + pad_y)
    ax.set_aspect("equal", adjustable="box")
    ax.set_title("Kurume TIF 有効画素bbox 一覧", fontproperties=FONT)
    ax.set_xlabel("経度", fontproperties=FONT)
    ax.set_ylabel("緯度", fontproperties=FONT)
    ax.grid(alpha=0.3)
    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_fontproperties(FONT)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "kurume_tif_bbox_overview.png", dpi=200, bbox_inches="tight")
    plt.close(fig)


def md_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: f"{x:.6f}")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    BBOX_DIR.mkdir(parents=True, exist_ok=True)

    features = []
    rows = []
    paths = sorted(KURUME_DIR.glob("*.tif"))
    if not paths:
        raise FileNotFoundError(f"No TIF files found: {KURUME_DIR}")

    for path in paths:
        data, valid, profile = read_valid(path)
        full_bounds = profile["full_bounds"]
        bbox, rc_bbox = valid_bbox(valid, profile["transform"])
        crs = profile["crs"]
        if crs:
            bbox_wgs84 = tuple(float(v) for v in transform_bounds(crs, "EPSG:4326", *bbox, densify_pts=21))
            full_wgs84 = tuple(float(v) for v in transform_bounds(crs, "EPSG:4326", *full_bounds, densify_pts=21))
            crs_text = crs.to_string()
        else:
            bbox_wgs84 = bbox
            full_wgs84 = full_bounds
            crs_text = ""

        feature = {
            "type": "Feature",
            "geometry": polygon_from_bounds(bbox_wgs84),
            "properties": {
                "tif": path.name,
                "bbox_type": "valid_pixels",
                "source_crs": crs_text,
                "valid_pixel_count": int(valid.sum()),
                "raster_height": int(profile["height"]),
                "raster_width": int(profile["width"]),
            },
        }
        features.append(feature)
        (BBOX_DIR / f"{path.stem}_valid_bbox.geojson").write_text(
            json.dumps({"type": "FeatureCollection", "features": [feature]}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        rows.append(
            {
                "tif": path.name,
                "source_crs": crs_text,
                "raster_height": int(profile["height"]),
                "raster_width": int(profile["width"]),
                "valid_pixel_count": int(valid.sum()),
                "full_left": full_wgs84[0],
                "full_bottom": full_wgs84[1],
                "full_right": full_wgs84[2],
                "full_top": full_wgs84[3],
                "valid_left": bbox_wgs84[0],
                "valid_bottom": bbox_wgs84[1],
                "valid_right": bbox_wgs84[2],
                "valid_top": bbox_wgs84[3],
                "valid_row_min": rc_bbox[0],
                "valid_row_max": rc_bbox[1],
                "valid_col_min": rc_bbox[2],
                "valid_col_max": rc_bbox[3],
            }
        )
        plot_single(path, data, full_wgs84, bbox_wgs84)

    all_geojson = {"type": "FeatureCollection", "features": features}
    (OUT_DIR / "kurume_tif_valid_bboxes.geojson").write_text(
        json.dumps(all_geojson, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    bbox_df = pd.DataFrame(rows)
    bbox_df.to_csv(OUT_DIR / "kurume_tif_valid_bboxes.csv", index=False, encoding="utf-8-sig")
    plot_overview(rows)

    report = [
        "# Kurume TIF bbox 作成結果",
        "",
        "## 作成したbbox",
        "",
        "各TIFの `nodata` を除いた有効画素を囲う bbox を作成しました。",
        "GeoJSONはEPSG:4326相当の経度緯度で出力しています。",
        "",
        "## 出力",
        "",
        "- `kurume_tif_valid_bboxes.geojson`: 全TIFのbboxをまとめたGeoJSON",
        "- `kurume_tif_valid_bboxes.csv`: 各TIFのbbox座標表",
        "- `geojson/*_valid_bbox.geojson`: TIFごとのbbox GeoJSON",
        "- `figures/*_bbox.png`: 各TIFとbboxの重ね合わせ画像",
        "- `figures/kurume_tif_bbox_overview.png`: 全bboxの位置関係",
        "",
        "## bbox一覧",
        "",
        md_table(
            bbox_df[
                ["tif", "valid_pixel_count", "valid_left", "valid_bottom", "valid_right", "valid_top"]
            ]
        ),
        "",
    ]
    try:
        report_text = "\n".join(report)
    except Exception:
        report_text = "\n".join(str(x) for x in report)
    (OUT_DIR / "README.md").write_text(report_text, encoding="utf-8")
    print(f"Wrote {OUT_DIR}")


if __name__ == "__main__":
    main()
