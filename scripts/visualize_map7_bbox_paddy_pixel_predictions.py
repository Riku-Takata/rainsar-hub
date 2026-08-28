from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.colors import ListedColormap, BoundaryNorm
import numpy as np
import pandas as pd
import rasterio


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_paddy_pixel_prediction_maps"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

TRUTH = BASE / "map7_inundation_truth_mask.tif"
BBOX = BASE / "bbox_balanced_classification/GIS_bbox_union_mask.tif"
PADDY = BASE / "landmask_filter/map7_paddy_mask.tif"
SAT_PRED = BASE / "bbox_paddy_pixel_polygon_20000/GIS_田んぼ画素_浸水判定.tif"
SAT_PROB = BASE / "bbox_paddy_pixel_polygon_20000/GIS_田んぼ画素_浸水確率.tif"
DEM_PRED = BASE / "bbox_paddy_dem5m_probabilities/GIS_田んぼ画素_DEM5m浸水判定.tif"
DEM_PROB = BASE / "bbox_paddy_dem5m_probabilities/GIS_田んぼ画素_DEM5m浸水確率.tif"


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


def read_bool(path: Path) -> tuple[np.ndarray, dict]:
    with rasterio.open(path) as src:
        arr = src.read(1) > 0
        profile = src.profile.copy()
    return arr, profile


def read_float(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def crop_slices(mask: np.ndarray, pad: int = 20) -> tuple[slice, slice]:
    rows, cols = np.where(mask)
    r0 = max(int(rows.min()) - pad, 0)
    r1 = min(int(rows.max()) + pad + 1, mask.shape[0])
    c0 = max(int(cols.min()) - pad, 0)
    c1 = min(int(cols.max()) + pad + 1, mask.shape[1])
    return slice(r0, r1), slice(c0, c1)


def classify(truth: np.ndarray, pred: np.ndarray, candidate: np.ndarray) -> np.ndarray:
    out = np.zeros(truth.shape, dtype=np.uint8)
    out[candidate] = 1  # TN or candidate background
    out[candidate & pred & ~truth] = 2  # FP
    out[candidate & ~pred & truth] = 3  # FN
    out[candidate & pred & truth] = 4  # TP
    return out


def metrics(truth: np.ndarray, pred: np.ndarray, candidate: np.ndarray, label: str) -> dict:
    tp = int((candidate & truth & pred).sum())
    fp = int((candidate & ~truth & pred).sum())
    fn = int((candidate & truth & ~pred).sum())
    tn = int((candidate & ~truth & ~pred).sum())
    recall = tp / (tp + fn) if tp + fn else np.nan
    precision = tp / (tp + fp) if tp + fp else np.nan
    specificity = tn / (tn + fp) if tn + fp else np.nan
    ba = (recall + specificity) / 2
    return {
        "method": label,
        "candidate_pixels": int(candidate.sum()),
        "truth_inundated_pixels": int((candidate & truth).sum()),
        "predicted_inundated_pixels": int((candidate & pred).sum()),
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "precision": precision,
        "recall": recall,
        "specificity": specificity,
        "balanced_accuracy_on_all_candidates": ba,
    }


def plot_class_maps(
    sat_cls: np.ndarray,
    dem_cls: np.ndarray,
    bbox: np.ndarray,
    truth: np.ndarray,
    crop: tuple[slice, slice],
) -> None:
    cmap = ListedColormap(["#ffffff", "#dddddd", "#f28e2b", "#4e79a7", "#59a14f"])
    norm = BoundaryNorm([-0.5, 0.5, 1.5, 2.5, 3.5, 4.5], cmap.N)
    labels = ["対象外", "田んぼ候補(TN)", "FP: 非浸水を浸水", "FN: 浸水を未検出", "TP: 浸水を検出"]
    fig, axes = plt.subplots(1, 2, figsize=(13, 6), constrained_layout=True)
    for ax, arr, title in [
        (axes[0], sat_cls, "bbox内・田んぼ画素判定（衛星データのみ）"),
        (axes[1], dem_cls, "bbox内・田んぼ画素判定（衛星 + DEM5m）"),
    ]:
        shown = np.ma.masked_where(~bbox[crop], arr[crop])
        im = ax.imshow(shown, cmap=cmap, norm=norm, interpolation="nearest")
        ax.contour(truth[crop].astype(np.uint8), levels=[0.5], colors=["#1f77b4"], linewidths=0.7)
        ax.set_title(title)
        ax.set_axis_off()
    cbar = fig.colorbar(im, ax=axes, fraction=0.03, pad=0.02, ticks=[0, 1, 2, 3, 4])
    cbar.ax.set_yticklabels(labels)
    fig.suptitle("bbox内で田んぼ画素に限定した浸水判定結果（青線: 正解浸水域境界）", fontsize=14)
    fig.savefig(OUT / "図_bbox内_田んぼ画素_判定結果比較.png", dpi=250, bbox_inches="tight")
    plt.close(fig)


def plot_probability_maps(sat_prob: np.ndarray, dem_prob: np.ndarray, candidate: np.ndarray, crop: tuple[slice, slice]) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.8), constrained_layout=True)
    for ax, arr, title in [
        (axes[0], sat_prob, "衛星データのみ: 浸水確率"),
        (axes[1], dem_prob, "衛星 + DEM5m: 浸水確率"),
    ]:
        shown = np.ma.masked_where(~candidate[crop] | ~np.isfinite(arr[crop]), arr[crop])
        im = ax.imshow(shown, cmap="magma", vmin=0, vmax=1, interpolation="nearest")
        ax.set_title(title)
        ax.set_axis_off()
    cbar = fig.colorbar(im, ax=axes, fraction=0.03, pad=0.02)
    cbar.set_label("浸水確率")
    fig.suptitle("bbox内・田んぼ画素に限定した浸水確率", fontsize=14)
    fig.savefig(OUT / "図_bbox内_田んぼ画素_浸水確率比較.png", dpi=250, bbox_inches="tight")
    plt.close(fig)


def md_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    lines = ["| " + " | ".join(shown.columns) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def write_report(summary: pd.DataFrame) -> None:
    lines = [
        "# bbox内・田んぼ画素に限定した判定画素の可視化",
        "",
        "## 内容",
        "",
        "- bbox内かつ田んぼマスク内の画素だけを対象に、浸水判定された画素を可視化しました。",
        "- `衛星データのみ` は `bbox_paddy_pixel_polygon_20000/GIS_田んぼ画素_浸水判定.tif` を使用しています。",
        "- `衛星 + DEM5m` は `bbox_paddy_dem5m_probabilities/GIS_田んぼ画素_DEM5m浸水判定.tif` を使用しています。",
        "- 図中の青線は正解浸水域境界です。",
        "",
        "## 全候補画素に適用した場合の集計",
        "",
        md_table(summary),
        "",
        "## 図",
        "",
        "- `図_bbox内_田んぼ画素_判定結果比較.png`",
        "- `図_bbox内_田んぼ画素_浸水確率比較.png`",
        "",
        "## 精度が下がる理由の見方",
        "",
        "bboxで空間範囲を絞ると、無関係な遠方画素は減りますが、同時に正解浸水域の近傍にある似た条件の非浸水画素が多く残ります。",
        "そのため、全域よりも分類が簡単になるとは限りません。特に衛星データのみでは、浸水域と非浸水域の後方散乱強度差分が重なりやすく、FPとFNが残ります。",
        "DEM5mを加えると、低標高条件を使えるため候補画素全体での分離は大きく改善しています。",
        "",
    ]
    (OUT / "bbox内_田んぼ画素_判定結果可視化レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    truth, profile = read_bool(TRUTH)
    bbox, _ = read_bool(BBOX)
    paddy, _ = read_bool(PADDY)
    sat_pred, _ = read_bool(SAT_PRED)
    dem_pred, _ = read_bool(DEM_PRED)
    sat_prob = read_float(SAT_PROB)
    dem_prob = read_float(DEM_PROB)

    sat_candidate = bbox & paddy & np.isfinite(sat_prob)
    dem_candidate = bbox & paddy & np.isfinite(dem_prob)
    candidate = sat_candidate | dem_candidate
    crop = crop_slices(bbox & candidate)

    sat_cls = classify(truth, sat_pred, sat_candidate)
    dem_cls = classify(truth, dem_pred, dem_candidate)
    summary = pd.DataFrame(
        [
            metrics(truth, sat_pred, sat_candidate, "衛星データのみ・田んぼ画素"),
            metrics(truth, dem_pred, dem_candidate, "衛星+DEM5m・田んぼ画素"),
        ]
    )
    summary.to_csv(OUT / "bbox内_田んぼ画素_判定結果集計.csv", index=False, encoding="utf-8-sig")
    plot_class_maps(sat_cls, dem_cls, bbox & candidate, truth, crop)
    plot_probability_maps(sat_prob, dem_prob, candidate, crop)
    write_report(summary)
    print(summary.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
