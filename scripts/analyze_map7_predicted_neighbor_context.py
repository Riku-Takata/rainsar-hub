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
OUT = BASE / "bbox_paddy_satellite_neighbor_context"

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


def four_neighbor_count(mask: np.ndarray) -> np.ndarray:
    kernel = np.array([[0, 1, 0], [1, 0, 1], [0, 1, 0]], dtype=np.uint8)
    return ndimage.convolve(mask.astype(np.uint8), kernel, mode="constant", cval=0)


def summarize_group(
    group_name: str,
    group_mask: np.ndarray,
    pred_neighbor_count: np.ndarray,
    truth_neighbor_count: np.ndarray,
    candidate_neighbor_count: np.ndarray,
    prob: np.ndarray,
) -> dict:
    pred_vals = pred_neighbor_count[group_mask]
    truth_vals = truth_neighbor_count[group_mask]
    valid_vals = candidate_neighbor_count[group_mask]
    prob_vals = prob[group_mask]
    return {
        "group": group_name,
        "pixel_count": int(group_mask.sum()),
        "pred_neighbor_mean": float(pred_vals.mean()) if pred_vals.size else np.nan,
        "pred_neighbor_median": float(np.median(pred_vals)) if pred_vals.size else np.nan,
        "pred_neighbor_0_pct": float((pred_vals == 0).mean() * 100) if pred_vals.size else np.nan,
        "pred_neighbor_ge1_pct": float((pred_vals >= 1).mean() * 100) if pred_vals.size else np.nan,
        "pred_neighbor_ge2_pct": float((pred_vals >= 2).mean() * 100) if pred_vals.size else np.nan,
        "truth_neighbor_mean": float(truth_vals.mean()) if truth_vals.size else np.nan,
        "truth_neighbor_median": float(np.median(truth_vals)) if truth_vals.size else np.nan,
        "truth_neighbor_0_pct": float((truth_vals == 0).mean() * 100) if truth_vals.size else np.nan,
        "truth_neighbor_ge1_pct": float((truth_vals >= 1).mean() * 100) if truth_vals.size else np.nan,
        "candidate_neighbor_mean": float(valid_vals.mean()) if valid_vals.size else np.nan,
        "probability_mean": float(np.nanmean(prob_vals)) if prob_vals.size else np.nan,
        "probability_median": float(np.nanmedian(prob_vals)) if prob_vals.size else np.nan,
    }


def neighbor_count_distribution(group_name: str, group_mask: np.ndarray, pred_neighbor_count: np.ndarray, truth_neighbor_count: np.ndarray) -> pd.DataFrame:
    rows = []
    for kind, counts in [
        ("predicted_inundated_neighbors", pred_neighbor_count[group_mask]),
        ("truth_inundated_neighbors", truth_neighbor_count[group_mask]),
    ]:
        total = len(counts)
        for n in range(5):
            c = int((counts == n).sum())
            rows.append(
                {
                    "group": group_name,
                    "neighbor_type": kind,
                    "neighbor_count": n,
                    "pixel_count": c,
                    "pct": c / total * 100 if total else np.nan,
                }
            )
    return pd.DataFrame(rows)


def plot_distributions(dist: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8), sharey=True)
    for ax, neighbor_type, title in [
        (axes[0], "predicted_inundated_neighbors", "周囲4近傍の浸水判定ピクセル数"),
        (axes[1], "truth_inundated_neighbors", "周囲4近傍の正解浸水ピクセル数"),
    ]:
        sub = dist[dist["neighbor_type"] == neighbor_type]
        width = 0.36
        x = np.arange(5)
        for i, group in enumerate(["TP", "FP"]):
            vals = sub[sub["group"] == group].set_index("neighbor_count").reindex(range(5))["pct"].fillna(0).to_numpy()
            ax.bar(x + (i - 0.5) * width, vals, width=width, label=group)
        ax.set_xticks(x)
        ax.set_xlabel("隣接ピクセル数")
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.3)
        ax.legend()
    axes[0].set_ylabel("割合 (%)")
    fig.suptitle("TPとFPにおける4近傍の状態比較")
    plt.tight_layout()
    plt.savefig(OUT / "図_TP_FP_4近傍ピクセル数比較.png", dpi=220, bbox_inches="tight")
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


def write_report(summary: pd.DataFrame, dist: pd.DataFrame) -> None:
    lines = [
        "# TP/FPの4近傍ピクセル分析",
        "",
        "## 対象",
        "",
        "- 標高データは使用していません。",
        "- bbox内かつ田んぼ画素に限定した、衛星データのみの浸水判定を対象にしました。",
        "- 4近傍は上下左右に隣接する4ピクセルです。斜め方向は含めていません。",
        "",
        "## 要約",
        "",
        md_table(summary),
        "",
        "## 隣接ピクセル数の分布",
        "",
        md_table(dist),
        "",
        "## 判断",
        "",
        "- TPは、周囲に同じく浸水判定されたピクセルを持つ割合が高いかを確認します。",
        "- FPも、周囲に浸水判定ピクセルや正解浸水ピクセルを持つか確認します。",
        "- FPで `pred_neighbor_0_pct` が高ければ、孤立した誤検出が多いと判断できます。",
        "- FPで `truth_neighbor_0_pct` が高ければ、正解浸水域から離れた非浸水画素を誤検出していると判断できます。",
        "",
        "## 出力図",
        "",
        "- `図_TP_FP_4近傍ピクセル数比較.png`",
        "",
    ]
    (OUT / "TP_FP_4近傍ピクセル分析レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    truth = read_bool(TRUTH)
    bbox = read_bool(BBOX)
    paddy = read_bool(PADDY)
    pred = read_bool(PRED)
    prob = read_float(PROB)
    candidate = bbox & paddy & np.isfinite(prob)

    tp = candidate & truth & pred
    fp = candidate & ~truth & pred
    pred_neighbor_count = four_neighbor_count(pred & candidate)
    truth_neighbor_count = four_neighbor_count(truth & candidate)
    candidate_neighbor_count = four_neighbor_count(candidate)

    summary = pd.DataFrame(
        [
            summarize_group("TP", tp, pred_neighbor_count, truth_neighbor_count, candidate_neighbor_count, prob),
            summarize_group("FP", fp, pred_neighbor_count, truth_neighbor_count, candidate_neighbor_count, prob),
        ]
    )
    dist = pd.concat(
        [
            neighbor_count_distribution("TP", tp, pred_neighbor_count, truth_neighbor_count),
            neighbor_count_distribution("FP", fp, pred_neighbor_count, truth_neighbor_count),
        ],
        ignore_index=True,
    )
    summary.to_csv(OUT / "TP_FP_4近傍要約.csv", index=False, encoding="utf-8-sig")
    dist.to_csv(OUT / "TP_FP_4近傍ピクセル数分布.csv", index=False, encoding="utf-8-sig")
    plot_distributions(dist)
    write_report(summary, dist)
    print(summary.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
