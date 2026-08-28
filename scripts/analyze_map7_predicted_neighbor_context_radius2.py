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
OUT = BASE / "bbox_paddy_satellite_neighbor_context_radius2"

TRUTH = BASE / "map7_inundation_truth_mask.tif"
BBOX = BASE / "bbox_balanced_classification/GIS_bbox_union_mask.tif"
PADDY = BASE / "landmask_filter/map7_paddy_mask.tif"
PRED_DIR = BASE / "bbox_paddy_pixel_polygon_20000"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


def find_tif(pattern: str) -> Path:
    matches = sorted(PRED_DIR.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No file matched: {PRED_DIR / pattern}")
    return matches[0]


def read_bool(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1) > 0


def read_float(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def manhattan_radius2_count(mask: np.ndarray) -> np.ndarray:
    kernel = np.array(
        [
            [0, 0, 1, 0, 0],
            [0, 1, 1, 1, 0],
            [1, 1, 0, 1, 1],
            [0, 1, 1, 1, 0],
            [0, 0, 1, 0, 0],
        ],
        dtype=np.uint8,
    )
    return ndimage.convolve(mask.astype(np.uint8), kernel, mode="constant", cval=0)


def summarize_counts(group: str, group_mask: np.ndarray, pred_count: np.ndarray, truth_count: np.ndarray, prob: np.ndarray) -> dict:
    pred_vals = pred_count[group_mask]
    truth_vals = truth_count[group_mask]
    prob_vals = prob[group_mask]
    return {
        "group": group,
        "pixel_count": int(group_mask.sum()),
        "pred_radius2_mean": float(pred_vals.mean()) if pred_vals.size else np.nan,
        "pred_radius2_median": float(np.median(pred_vals)) if pred_vals.size else np.nan,
        "pred_radius2_0_pct": float((pred_vals == 0).mean() * 100) if pred_vals.size else np.nan,
        "pred_radius2_ge1_pct": float((pred_vals >= 1).mean() * 100) if pred_vals.size else np.nan,
        "pred_radius2_ge4_pct": float((pred_vals >= 4).mean() * 100) if pred_vals.size else np.nan,
        "pred_radius2_ge8_pct": float((pred_vals >= 8).mean() * 100) if pred_vals.size else np.nan,
        "truth_radius2_mean": float(truth_vals.mean()) if truth_vals.size else np.nan,
        "truth_radius2_median": float(np.median(truth_vals)) if truth_vals.size else np.nan,
        "truth_radius2_0_pct": float((truth_vals == 0).mean() * 100) if truth_vals.size else np.nan,
        "truth_radius2_ge1_pct": float((truth_vals >= 1).mean() * 100) if truth_vals.size else np.nan,
        "truth_radius2_ge4_pct": float((truth_vals >= 4).mean() * 100) if truth_vals.size else np.nan,
        "probability_mean": float(np.nanmean(prob_vals)) if prob_vals.size else np.nan,
        "probability_median": float(np.nanmedian(prob_vals)) if prob_vals.size else np.nan,
    }


def count_distribution(group: str, group_mask: np.ndarray, pred_count: np.ndarray, truth_count: np.ndarray) -> pd.DataFrame:
    rows = []
    for neighbor_type, counts in [
        ("predicted_inundated_radius2", pred_count[group_mask]),
        ("truth_inundated_radius2", truth_count[group_mask]),
    ]:
        total = counts.size
        for n in range(13):
            c = int((counts == n).sum())
            rows.append(
                {
                    "group": group,
                    "neighbor_type": neighbor_type,
                    "neighbor_count": n,
                    "pixel_count": c,
                    "pct": c / total * 100 if total else np.nan,
                }
            )
    return pd.DataFrame(rows)


def plot_distribution(dist: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.8), sharey=True)
    pairs = [
        ("predicted_inundated_radius2", "2段階4近傍内の浸水予測ピクセル数"),
        ("truth_inundated_radius2", "2段階4近傍内の正解浸水ピクセル数"),
    ]
    for ax, (neighbor_type, title) in zip(axes, pairs):
        sub = dist[dist["neighbor_type"] == neighbor_type]
        x = np.arange(13)
        width = 0.36
        for i, group in enumerate(["TP", "FP"]):
            vals = sub[sub["group"] == group].set_index("neighbor_count").reindex(range(13))["pct"].fillna(0).to_numpy()
            ax.bar(x + (i - 0.5) * width, vals, width=width, label=group)
        ax.set_title(title)
        ax.set_xlabel("近傍ピクセル数")
        ax.set_xticks(x)
        ax.grid(axis="y", alpha=0.3)
        ax.legend()
    axes[0].set_ylabel("割合 (%)")
    fig.suptitle("TPとFPにおける2段階4近傍の状態比較")
    plt.tight_layout()
    plt.savefig(OUT / "図_TP_FP_2段階4近傍ピクセル数比較.png", dpi=220, bbox_inches="tight")
    plt.close()


def markdown_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda v: "" if pd.isna(v) else f"{v:.3f}")
    lines = ["| " + " | ".join(shown.columns) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in shown.columns) + " |")
    return "\n".join(lines)


def write_report(summary: pd.DataFrame, dist: pd.DataFrame) -> None:
    lines = [
        "# TP/FPの2段階4近傍ピクセル分析",
        "",
        "## 対象",
        "",
        "- bbox内かつ田んぼ画素に限定した、衛星データのみの浸水判定を対象にしました。",
        "- 2段階4近傍は、中心画素から上下左右方向に2ステップ以内の画素です。",
        "- 斜め方向のみの画素は含めず、中心画素も含めません。最大12画素です。",
        "",
        "## 要約",
        "",
        markdown_table(summary),
        "",
        "## 近傍ピクセル数分布",
        "",
        markdown_table(dist),
        "",
        "## 図",
        "",
        "- `図_TP_FP_2段階4近傍ピクセル数比較.png`",
        "",
    ]
    (OUT / "TP_FP_2段階4近傍ピクセル分析レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    pred_path = find_tif("*判定.tif")
    prob_path = find_tif("*確率.tif")

    truth = read_bool(TRUTH)
    bbox = read_bool(BBOX)
    paddy = read_bool(PADDY)
    pred = read_bool(pred_path)
    prob = read_float(prob_path)
    candidate = bbox & paddy & np.isfinite(prob)

    tp = candidate & truth & pred
    fp = candidate & ~truth & pred
    pred_count = manhattan_radius2_count(pred & candidate)
    truth_count = manhattan_radius2_count(truth & candidate)

    summary = pd.DataFrame(
        [
            summarize_counts("TP", tp, pred_count, truth_count, prob),
            summarize_counts("FP", fp, pred_count, truth_count, prob),
        ]
    )
    dist = pd.concat(
        [
            count_distribution("TP", tp, pred_count, truth_count),
            count_distribution("FP", fp, pred_count, truth_count),
        ],
        ignore_index=True,
    )

    summary.to_csv(OUT / "TP_FP_2段階4近傍要約.csv", index=False, encoding="utf-8-sig")
    dist.to_csv(OUT / "TP_FP_2段階4近傍ピクセル数分布.csv", index=False, encoding="utf-8-sig")
    plot_distribution(dist)
    write_report(summary, dist)
    print(summary.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
