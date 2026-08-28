from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd
import rasterio
from scipy import ndimage


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_error_distance_to_truth"

TRUTH = BASE / "map7_inundation_truth_mask.tif"
BBOX = BASE / "bbox_balanced_classification/GIS_bbox_union_mask.tif"
PADDY = BASE / "landmask_filter/map7_paddy_mask.tif"
PRED = BASE / "bbox_paddy_pixel_polygon_20000/GIS_田んぼ画素_浸水判定.tif"
PROB = BASE / "bbox_paddy_pixel_polygon_20000/GIS_田んぼ画素_浸水確率.tif"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


def read_bool(path: Path) -> tuple[np.ndarray, dict]:
    with rasterio.open(path) as src:
        return src.read(1) > 0, src.profile.copy()


def read_float(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def pixel_size_m(profile: dict) -> tuple[float, float]:
    transform = profile["transform"]
    lat = transform.f + transform.e * profile["height"] / 2
    dx = abs(transform.a) * 111_320.0 * np.cos(np.deg2rad(lat))
    dy = abs(transform.e) * 111_320.0
    return float(dy), float(dx)


def truth_boundary(truth: np.ndarray) -> np.ndarray:
    eroded = ndimage.binary_erosion(truth, structure=np.ones((3, 3)), border_value=0)
    dilated = ndimage.binary_dilation(truth, structure=np.ones((3, 3)), border_value=0)
    return dilated ^ eroded


def summarize_group(name: str, mask: np.ndarray, distance_m: np.ndarray, prob: np.ndarray) -> dict:
    vals = distance_m[mask]
    probs = prob[mask]
    return {
        "group": name,
        "pixel_count": int(mask.sum()),
        "distance_mean_m": float(np.nanmean(vals)) if vals.size else np.nan,
        "distance_median_m": float(np.nanmedian(vals)) if vals.size else np.nan,
        "distance_p25_m": float(np.nanpercentile(vals, 25)) if vals.size else np.nan,
        "distance_p75_m": float(np.nanpercentile(vals, 75)) if vals.size else np.nan,
        "within_30m_pct": float((vals <= 30).mean() * 100) if vals.size else np.nan,
        "within_50m_pct": float((vals <= 50).mean() * 100) if vals.size else np.nan,
        "within_100m_pct": float((vals <= 100).mean() * 100) if vals.size else np.nan,
        "probability_mean": float(np.nanmean(probs)) if vals.size else np.nan,
        "probability_median": float(np.nanmedian(probs)) if vals.size else np.nan,
    }


def crop_slices(mask: np.ndarray, pad: int = 20) -> tuple[slice, slice]:
    rows, cols = np.where(mask)
    return (
        slice(max(int(rows.min()) - pad, 0), min(int(rows.max()) + pad + 1, mask.shape[0])),
        slice(max(int(cols.min()) - pad, 0), min(int(cols.max()) + pad + 1, mask.shape[1])),
    )


def plot_distance_hist(groups: dict[str, np.ndarray], distance_m: np.ndarray) -> None:
    fig, ax = plt.subplots(figsize=(9, 5))
    bins = np.linspace(0, 500, 51)
    colors = {"TP": "#59a14f", "FN": "#4e79a7", "FP": "#f28e2b", "TN": "#999999"}
    for name in ["TP", "FN", "FP", "TN"]:
        vals = distance_m[groups[name]]
        vals = vals[np.isfinite(vals)]
        ax.hist(vals, bins=bins, density=True, alpha=0.45, label=name, color=colors[name])
    ax.set_xlabel("正解浸水域境界からの距離 (m)")
    ax.set_ylabel("密度")
    ax.set_title("判定群ごとの正解浸水域境界からの距離分布")
    ax.grid(alpha=0.25)
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT / "図_判定群別_浸水域境界からの距離分布.png", dpi=220, bbox_inches="tight")
    plt.close()


def plot_error_map(truth: np.ndarray, bbox: np.ndarray, groups: dict[str, np.ndarray], distance_m: np.ndarray) -> None:
    crop = crop_slices(bbox)
    arr = np.zeros(truth.shape, dtype=np.uint8)
    arr[groups["TN"]] = 1
    arr[groups["FP"]] = 2
    arr[groups["FN"]] = 3
    arr[groups["TP"]] = 4
    from matplotlib.colors import ListedColormap, BoundaryNorm

    cmap = ListedColormap(["#ffffff", "#dddddd", "#f28e2b", "#4e79a7", "#59a14f"])
    norm = BoundaryNorm([-0.5, 0.5, 1.5, 2.5, 3.5, 4.5], cmap.N)
    fig, ax = plt.subplots(figsize=(8, 8))
    shown = np.ma.masked_where(~bbox[crop], arr[crop])
    im = ax.imshow(shown, cmap=cmap, norm=norm, interpolation="nearest")
    ax.contour(truth[crop].astype(np.uint8), levels=[0.5], colors=["#1f77b4"], linewidths=0.8)
    ax.set_title("bbox内田んぼ画素: FP/FNと正解浸水域の位置関係")
    ax.set_axis_off()
    cbar = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02, ticks=[0, 1, 2, 3, 4])
    cbar.ax.set_yticklabels(["対象外", "TN", "FP", "FN", "TP"])
    plt.tight_layout()
    plt.savefig(OUT / "図_FP_FN_浸水域との位置関係.png", dpi=250, bbox_inches="tight")
    plt.close()


def md_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def write_report(summary: pd.DataFrame) -> None:
    fp = summary[summary["group"] == "FP"].iloc[0]
    fn = summary[summary["group"] == "FN"].iloc[0]
    lines = [
        "# FP/FNと正解浸水域の位置関係",
        "",
        "## 対象",
        "",
        "- 標高データは使用していません。",
        "- bbox内かつ田んぼ画素に限定した、衛星データのみの判定結果を使用しました。",
        "- FP: 非浸水域だが浸水域として検出された画素。",
        "- FN: 浸水域だが非浸水域として検出された画素。",
        "",
        "## 距離集計",
        "",
        md_table(summary),
        "",
        "## 判断",
        "",
        f"- FPの距離中央値は {fp['distance_median_m']:.1f} m、100m以内の割合は {fp['within_100m_pct']:.1f}% です。",
        f"- FNの距離中央値は {fn['distance_median_m']:.1f} m、100m以内の割合は {fn['within_100m_pct']:.1f}% です。",
        "- FPが正解浸水域境界の近くに集中していれば、非浸水域の中でも浸水域近傍のSAR挙動が似ている画素を誤検出していると解釈できます。",
        "- FNが境界付近に多ければ、正解浸水域内でも端部や混合画素が非浸水側に近い挙動を示している可能性があります。",
        "",
        "## 出力図",
        "",
        "- `図_判定群別_浸水域境界からの距離分布.png`",
        "- `図_FP_FN_浸水域との位置関係.png`",
        "",
    ]
    (OUT / "FP_FN_浸水域距離分析レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    truth, profile = read_bool(TRUTH)
    bbox, _ = read_bool(BBOX)
    paddy, _ = read_bool(PADDY)
    pred, _ = read_bool(PRED)
    prob = read_float(PROB)
    candidate = bbox & paddy & np.isfinite(prob)

    boundary = truth_boundary(truth)
    dy, dx = pixel_size_m(profile)
    distance_m = ndimage.distance_transform_edt(~boundary, sampling=(dy, dx)).astype(np.float32)

    groups = {
        "TP": candidate & truth & pred,
        "FN": candidate & truth & ~pred,
        "FP": candidate & ~truth & pred,
        "TN": candidate & ~truth & ~pred,
    }
    summary = pd.DataFrame([summarize_group(name, mask, distance_m, prob) for name, mask in groups.items()])
    summary.to_csv(OUT / "判定群別_浸水域境界距離集計.csv", index=False, encoding="utf-8-sig")
    plot_distance_hist(groups, distance_m)
    plot_error_map(truth, candidate, groups, distance_m)
    write_report(summary)
    print(summary.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
