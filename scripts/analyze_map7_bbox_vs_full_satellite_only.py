from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from scipy.stats import ks_2samp
from sklearn.metrics import roc_auc_score

import analyze_map7_bbox_paddy_pixel_polygon_20000 as paddy_base


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
LAND = BASE / "landmask_filter"
OUT = BASE / "bbox_vs_full_satellite_only_diagnostics"
LABEL_COL = "label"
SEED = 42
MAX_PER_CLASS = 20000

FEATURES = paddy_base.PIXEL_FEATURES
DIFF_RASTERS = paddy_base.DIFF_RASTERS


def read_float(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def read_bool(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1) > 0


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    return paddy_base.add_pixel_features(df)


def build_frame(mask_path: Path, area_name: str, bbox_only: bool) -> pd.DataFrame:
    d0 = read_float(DIFF_RASTERS["0_3h"])
    d3 = read_float(DIFF_RASTERS["3_6h"])
    d6 = read_float(DIFF_RASTERS["6_12h"])
    d12 = read_float(DIFF_RASTERS["12_24h"])
    truth = read_bool(BASE / "map7_inundation_truth_mask.tif")
    land = read_bool(mask_path)
    bbox = read_bool(BASE / "bbox_balanced_classification/GIS_bbox_union_mask.tif")
    valid = land & np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12)
    if bbox_only:
        valid &= bbox
    rows, cols = np.where(valid)
    df = pd.DataFrame(
        {
            "area_name": area_name,
            "scope": "bbox内" if bbox_only else "bboxなし",
            "row": rows,
            "col": cols,
            LABEL_COL: truth[valid].astype(np.uint8),
            "diff_0_3h": d0[valid],
            "diff_3_6h": d3[valid],
            "diff_6_12h": d6[valid],
            "diff_12_24h": d12[valid],
        }
    )
    return add_features(df)


def balanced_sample(df: pd.DataFrame) -> pd.DataFrame:
    valid = df.dropna(subset=FEATURES + [LABEL_COL]).copy()
    pos = valid[valid[LABEL_COL] == 1]
    neg = valid[valid[LABEL_COL] == 0]
    n = min(len(pos), len(neg), MAX_PER_CLASS)
    return pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    )


def summarize_counts(frames: list[pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for df in frames:
        rows.append(
            {
                "area_name": df["area_name"].iloc[0],
                "scope": df["scope"].iloc[0],
                "positive_pixels": int((df[LABEL_COL] == 1).sum()),
                "negative_pixels": int((df[LABEL_COL] == 0).sum()),
                "positive_ratio": float((df[LABEL_COL] == 1).mean()),
                "sampled_per_class": int(min((df[LABEL_COL] == 1).sum(), (df[LABEL_COL] == 0).sum(), MAX_PER_CLASS)),
            }
        )
    return pd.DataFrame(rows)


def feature_stats(frames: list[pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for df in frames:
        sampled = balanced_sample(df)
        for feat in FEATURES:
            pos = sampled.loc[sampled[LABEL_COL] == 1, feat].to_numpy()
            neg = sampled.loc[sampled[LABEL_COL] == 0, feat].to_numpy()
            auc = roc_auc_score(sampled[LABEL_COL], sampled[feat])
            auc_sep = max(auc, 1.0 - auc)
            ks = ks_2samp(pos, neg)
            rows.append(
                {
                    "area_name": df["area_name"].iloc[0],
                    "scope": df["scope"].iloc[0],
                    "feature": feat,
                    "positive_mean": float(np.mean(pos)),
                    "negative_mean": float(np.mean(neg)),
                    "mean_diff_pos_minus_neg": float(np.mean(pos) - np.mean(neg)),
                    "positive_median": float(np.median(pos)),
                    "negative_median": float(np.median(neg)),
                    "single_feature_auc_separation": float(auc_sep),
                    "ks_statistic": float(ks.statistic),
                    "ks_pvalue": float(ks.pvalue),
                }
            )
    return pd.DataFrame(rows)


def compare_negative_pool(full_df: pd.DataFrame, bbox_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    full_neg = full_df[full_df[LABEL_COL] == 0].sample(
        n=min(MAX_PER_CLASS, int((full_df[LABEL_COL] == 0).sum())),
        random_state=SEED,
    )
    bbox_neg = bbox_df[bbox_df[LABEL_COL] == 0].sample(
        n=min(MAX_PER_CLASS, int((bbox_df[LABEL_COL] == 0).sum())),
        random_state=SEED,
    )
    for feat in FEATURES:
        f = full_neg[feat].to_numpy()
        b = bbox_neg[feat].to_numpy()
        ks = ks_2samp(f, b)
        rows.append(
            {
                "feature": feat,
                "full_negative_mean": float(np.mean(f)),
                "bbox_negative_mean": float(np.mean(b)),
                "bbox_minus_full_negative_mean": float(np.mean(b) - np.mean(f)),
                "full_negative_median": float(np.median(f)),
                "bbox_negative_median": float(np.median(b)),
                "ks_statistic_fullneg_vs_bboxneg": float(ks.statistic),
                "ks_pvalue": float(ks.pvalue),
            }
        )
    return pd.DataFrame(rows)


def plot_top_features(stats: pd.DataFrame, area_name: str) -> None:
    sub = stats[stats["area_name"] == area_name].copy()
    pivot = sub.pivot(index="feature", columns="scope", values="single_feature_auc_separation")
    pivot = pivot.sort_values("bboxなし", ascending=False)
    shown = pivot.head(10).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 6))
    y = np.arange(len(shown))
    ax.barh(y - 0.18, shown["bboxなし"], height=0.36, label="bboxなし")
    ax.barh(y + 0.18, shown["bbox内"], height=0.36, label="bbox内")
    ax.set_yticks(y)
    ax.set_yticklabels(shown.index)
    ax.set_xlim(0.45, 0.75)
    ax.set_xlabel("単一特徴量のAUC分離度")
    ax.set_title(f"{area_name}: bbox有無による特徴量分離度の違い")
    ax.grid(axis="x", alpha=0.3)
    ax.legend()
    plt.tight_layout()
    safe = area_name.replace("+", "_plus_")
    plt.savefig(OUT / f"図_{safe}_bbox有無_特徴量分離度.png", dpi=220, bbox_inches="tight")
    plt.close()


def md_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.4f}")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def write_report(counts: pd.DataFrame, stats: pd.DataFrame, neg_paddy: pd.DataFrame, neg_road_paddy: pd.DataFrame) -> None:
    top = (
        stats.sort_values(["area_name", "scope", "single_feature_auc_separation"], ascending=[True, True, False])
        .groupby(["area_name", "scope"])
        .head(6)
        .reset_index(drop=True)
    )
    lines = [
        "# 衛星データのみ: bbox有無で精度が変わる理由の調査",
        "",
        "## 調査対象",
        "",
        "- 標高データは使用していません。",
        "- `bboxなし` は、対象地物マスク全体から正解/非正解画素を同数抽出した場合です。",
        "- `bbox内` は、久留米浸水TIFのbbox和集合内だけに限定した場合です。",
        "- 田んぼ画素と道路+田んぼ画素の2条件で比較しました。",
        "",
        "## 母数の違い",
        "",
        md_table(counts),
        "",
        "## 主な結論",
        "",
        "- bboxで絞ると正解浸水画素数はほぼ変わらない一方、非浸水画素が大幅に減ります。",
        "- 減った非浸水画素は、浸水域から遠い画素や明らかに異なる画素が中心です。",
        "- その結果、bbox内に残る非浸水画素は浸水域近傍の似たSAR差分を持つ画素になり、分類が難しくなります。",
        "- つまりbboxは余計な画素を除外しますが、機械学習の評価上は「簡単なnegative」を除外して「難しいnegative」を残すため、精度が下がることがあります。",
        "",
        "## 分離しやすい特徴量の変化",
        "",
        md_table(top[["area_name", "scope", "feature", "single_feature_auc_separation", "ks_statistic", "positive_mean", "negative_mean", "mean_diff_pos_minus_neg"]]),
        "",
        "## 非浸水画素の母集団変化: 田んぼ画素",
        "",
        md_table(neg_paddy.sort_values("ks_statistic_fullneg_vs_bboxneg", ascending=False).head(8)),
        "",
        "## 非浸水画素の母集団変化: 道路+田んぼ画素",
        "",
        md_table(neg_road_paddy.sort_values("ks_statistic_fullneg_vs_bboxneg", ascending=False).head(8)),
        "",
        "## 出力",
        "",
        "- `bbox有無_母数比較.csv`",
        "- `bbox有無_特徴量分離度.csv`",
        "- `bbox有無_非浸水画素分布差_田んぼ.csv`",
        "- `bbox有無_非浸水画素分布差_道路田んぼ.csv`",
        "- `図_田んぼ画素_bbox有無_特徴量分離度.png`",
        "- `図_道路_plus_田んぼ画素_bbox有無_特徴量分離度.png`",
        "",
    ]
    (OUT / "衛星データのみ_bbox有無_精度差調査レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    frames = []
    for area_name, mask_path in [
        ("田んぼ画素", LAND / "map7_paddy_mask.tif"),
        ("道路+田んぼ画素", LAND / "map7_paddy_or_road_mask.tif"),
    ]:
        frames.append(build_frame(mask_path, area_name, bbox_only=False))
        frames.append(build_frame(mask_path, area_name, bbox_only=True))

    counts = summarize_counts(frames)
    stats = feature_stats(frames)
    paddy_full = next(df for df in frames if df["area_name"].iloc[0] == "田んぼ画素" and df["scope"].iloc[0] == "bboxなし")
    paddy_bbox = next(df for df in frames if df["area_name"].iloc[0] == "田んぼ画素" and df["scope"].iloc[0] == "bbox内")
    road_full = next(df for df in frames if df["area_name"].iloc[0] == "道路+田んぼ画素" and df["scope"].iloc[0] == "bboxなし")
    road_bbox = next(df for df in frames if df["area_name"].iloc[0] == "道路+田んぼ画素" and df["scope"].iloc[0] == "bbox内")
    neg_paddy = compare_negative_pool(paddy_full, paddy_bbox)
    neg_road = compare_negative_pool(road_full, road_bbox)

    counts.to_csv(OUT / "bbox有無_母数比較.csv", index=False, encoding="utf-8-sig")
    stats.to_csv(OUT / "bbox有無_特徴量分離度.csv", index=False, encoding="utf-8-sig")
    neg_paddy.to_csv(OUT / "bbox有無_非浸水画素分布差_田んぼ.csv", index=False, encoding="utf-8-sig")
    neg_road.to_csv(OUT / "bbox有無_非浸水画素分布差_道路田んぼ.csv", index=False, encoding="utf-8-sig")
    plot_top_features(stats, "田んぼ画素")
    plot_top_features(stats, "道路+田んぼ画素")
    write_report(counts, stats, neg_paddy, neg_road)
    print(counts.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
