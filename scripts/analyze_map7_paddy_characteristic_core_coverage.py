from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = (
    ROOT
    / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
)
CHAR_DIR = DETECTION_DIR / "paddy_characteristic_5000_model_report"
OUT_DIR = DETECTION_DIR / "paddy_characteristic_core_coverage"
CHAR_PIXELS = CHAR_DIR / "kurume_paddy_characteristic_5000_pixels.csv"

LABEL_COL = "正解浸水域"
CORE_FEATURES = [
    "early_minus_late",
    "drop_3_6_to_6_12",
    "late_mean_6_24h",
    "diff_6_12h",
    "early_mean_0_6h",
    "drop_0_3_to_6_12",
    "change_0_3_to_6_12",
]
ALL_FEATURES = [
    "diff_0_3h",
    "diff_3_6h",
    "diff_6_12h",
    "diff_12_24h",
    "early_mean_0_6h",
    "late_mean_6_24h",
    "early_minus_late",
    "drop_0_3_to_3_6",
    "drop_3_6_to_6_12",
    "recovery_6_12_to_12_24",
    "drop_0_3_to_6_12",
    "change_0_3_to_6_12",
    "profile_mean",
    "profile_std",
    "profile_range",
    "negative_bin_count",
    "monotonic_drop_score",
]


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


def build_feature_frame() -> pd.DataFrame:
    d0 = read_float(DETECTION_DIR / "map7_mean_diff_0_3h.tif")
    d3 = read_float(DETECTION_DIR / "map7_mean_diff_3_6h.tif")
    d6 = read_float(DETECTION_DIR / "map7_mean_diff_6_12h.tif")
    d12 = read_float(DETECTION_DIR / "map7_mean_diff_12_24h.tif")
    truth = read_bool(DETECTION_DIR / "map7_inundation_truth_mask.tif")
    paddy = read_bool(DETECTION_DIR / "landmask_filter" / "map7_paddy_mask.tif")
    valid = np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12) & paddy

    row, col = np.where(valid)
    df = pd.DataFrame(
        {
            "row": row,
            "col": col,
            LABEL_COL: truth[valid].astype(np.uint8),
            "diff_0_3h": d0[valid],
            "diff_3_6h": d3[valid],
            "diff_6_12h": d6[valid],
            "diff_12_24h": d12[valid],
        }
    )
    df["early_mean_0_6h"] = (df["diff_0_3h"] + df["diff_3_6h"]) / 2.0
    df["late_mean_6_24h"] = (df["diff_6_12h"] + df["diff_12_24h"]) / 2.0
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df["diff_0_3h"] - df["diff_3_6h"]
    df["drop_3_6_to_6_12"] = df["diff_3_6h"] - df["diff_6_12h"]
    df["recovery_6_12_to_12_24"] = df["diff_12_24h"] - df["diff_6_12h"]
    df["drop_0_3_to_6_12"] = df["diff_0_3h"] - df["diff_6_12h"]
    df["change_0_3_to_6_12"] = df["diff_6_12h"] - df["diff_0_3h"]
    profile = df[["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]].to_numpy(np.float32)
    df["profile_mean"] = profile.mean(axis=1)
    df["profile_std"] = profile.std(axis=1)
    df["profile_range"] = profile.max(axis=1) - profile.min(axis=1)
    df["negative_bin_count"] = (profile < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h"] >= df["diff_3_6h"]).astype(int)
        + (df["diff_3_6h"] >= df["diff_6_12h"]).astype(int)
        + (df["diff_6_12h"] <= df["diff_12_24h"]).astype(int)
    )
    return df


def robust_distance(values: pd.DataFrame, median: pd.Series, iqr: pd.Series) -> np.ndarray:
    scale = iqr.replace(0, np.nan)
    fallback = scale[scale > 0].median()
    scale = scale.fillna(fallback)
    z = (values - median) / scale
    return np.sqrt(np.nanmean(np.square(z.to_numpy(np.float64)), axis=1))


def md_table(df: pd.DataFrame, digits: int = 3) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.{digits}f}")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def plot_coverage(summary: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    labels = summary["threshold_name"].tolist()
    x = np.arange(len(labels))
    width = 0.35
    ax.bar(x - width / 2, summary["truth_recall_percent"], width, label="truth coverage")
    ax.bar(x + width / 2, summary["all_paddy_percent"], width, label="all paddy share")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("percent")
    ax.set_title("Characteristic inundation core coverage")
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "characteristic_core_coverage.png", dpi=180)
    plt.close(fig)


def plot_distance_hist(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for label, name, color in [(1, "truth inundated", "#d62728"), (0, "non-inundated", "#1f77b4")]:
        s = df.loc[df[LABEL_COL] == label, "dist_to_characteristic_inundated_core"]
        ax.hist(s, bins=100, range=(0, np.nanpercentile(df["dist_to_characteristic_inundated_core"], 99)), density=True, alpha=0.45, label=name, color=color)
    ax.set_xlabel("robust distance to characteristic inundated core")
    ax.set_ylabel("density")
    ax.set_title("Distance distribution in all paddy pixels")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "distance_to_inundated_core_distribution.png", dpi=180)
    plt.close(fig)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_df = build_feature_frame()
    char = pd.read_csv(CHAR_PIXELS)
    char_pos = char[char[LABEL_COL] == 1]
    char_neg = char[char[LABEL_COL] == 0]

    pos_median = char_pos[CORE_FEATURES].median()
    pos_iqr = char_pos[CORE_FEATURES].quantile(0.75) - char_pos[CORE_FEATURES].quantile(0.25)
    neg_median = char_neg[CORE_FEATURES].median()
    neg_iqr = char_neg[CORE_FEATURES].quantile(0.75) - char_neg[CORE_FEATURES].quantile(0.25)

    char_pos_dist = robust_distance(char_pos[CORE_FEATURES], pos_median, pos_iqr)
    char_neg_dist = robust_distance(char_neg[CORE_FEATURES], neg_median, neg_iqr)
    all_df["dist_to_characteristic_inundated_core"] = robust_distance(all_df[CORE_FEATURES], pos_median, pos_iqr)
    all_df["dist_to_characteristic_non_inundated_core"] = robust_distance(all_df[CORE_FEATURES], neg_median, neg_iqr)
    all_df["closer_to_inundated_core"] = (
        all_df["dist_to_characteristic_inundated_core"]
        < all_df["dist_to_characteristic_non_inundated_core"]
    )

    thresholds = [
        ("p50_of_characteristic_inundated", float(np.quantile(char_pos_dist, 0.50))),
        ("p75_of_characteristic_inundated", float(np.quantile(char_pos_dist, 0.75))),
        ("p90_of_characteristic_inundated", float(np.quantile(char_pos_dist, 0.90))),
        ("p95_of_characteristic_inundated", float(np.quantile(char_pos_dist, 0.95))),
        ("max_of_characteristic_inundated", float(np.max(char_pos_dist))),
    ]

    total = len(all_df)
    truth_total = int((all_df[LABEL_COL] == 1).sum())
    non_total = int((all_df[LABEL_COL] == 0).sum())
    rows = []
    for name, th in thresholds:
        near = all_df["dist_to_characteristic_inundated_core"] <= th
        near_and_closer = near & all_df["closer_to_inundated_core"]
        for use_closer, candidate, suffix in [
            (False, near, "distance_only"),
            (True, near_and_closer, "distance_and_closer_than_non_inundated_core"),
        ]:
            tp = int((candidate & (all_df[LABEL_COL] == 1)).sum())
            fp = int((candidate & (all_df[LABEL_COL] == 0)).sum())
            fn = truth_total - tp
            tn = non_total - fp
            rows.append(
                {
                    "threshold_name": name,
                    "candidate_rule": suffix,
                    "distance_threshold": th,
                    "candidate_pixels": int(candidate.sum()),
                    "all_paddy_percent": float(candidate.mean() * 100),
                    "truth_candidate_pixels": tp,
                    "nontruth_candidate_pixels": fp,
                    "truth_recall_percent": float(tp / truth_total * 100),
                    "nontruth_selected_percent": float(fp / non_total * 100),
                    "candidate_precision_percent": float(tp / (tp + fp) * 100) if tp + fp else 0.0,
                    "FN": fn,
                    "TN": tn,
                }
            )
    summary = pd.DataFrame(rows)

    dist_summary = []
    for label, zone in [(1, "truth_inundated"), (0, "non_inundated")]:
        sub = all_df[all_df[LABEL_COL] == label]
        for col in ["dist_to_characteristic_inundated_core", "dist_to_characteristic_non_inundated_core"]:
            s = sub[col]
            dist_summary.append(
                {
                    "zone": zone,
                    "distance": col,
                    "count": len(s),
                    "mean": s.mean(),
                    "std": s.std(),
                    "p05": s.quantile(0.05),
                    "p25": s.quantile(0.25),
                    "median": s.median(),
                    "p75": s.quantile(0.75),
                    "p95": s.quantile(0.95),
                }
            )
    dist_summary = pd.DataFrame(dist_summary)

    char_dist_summary = pd.DataFrame(
        [
            {
                "reference_group": "characteristic_inundated_5000",
                "distance_to_own_core_p50": np.quantile(char_pos_dist, 0.50),
                "distance_to_own_core_p75": np.quantile(char_pos_dist, 0.75),
                "distance_to_own_core_p90": np.quantile(char_pos_dist, 0.90),
                "distance_to_own_core_p95": np.quantile(char_pos_dist, 0.95),
                "distance_to_own_core_max": np.max(char_pos_dist),
            },
            {
                "reference_group": "characteristic_non_inundated_5000",
                "distance_to_own_core_p50": np.quantile(char_neg_dist, 0.50),
                "distance_to_own_core_p75": np.quantile(char_neg_dist, 0.75),
                "distance_to_own_core_p90": np.quantile(char_neg_dist, 0.90),
                "distance_to_own_core_p95": np.quantile(char_neg_dist, 0.95),
                "distance_to_own_core_max": np.max(char_neg_dist),
            },
        ]
    )

    summary.to_csv(OUT_DIR / "characteristic_core_coverage_summary.csv", index=False, encoding="utf-8-sig")
    dist_summary.to_csv(OUT_DIR / "all_paddy_distance_distribution_summary.csv", index=False, encoding="utf-8-sig")
    char_dist_summary.to_csv(OUT_DIR / "characteristic_reference_distance_thresholds.csv", index=False, encoding="utf-8-sig")
    all_df[
        [
            "row",
            "col",
            LABEL_COL,
            "dist_to_characteristic_inundated_core",
            "dist_to_characteristic_non_inundated_core",
            "closer_to_inundated_core",
        ]
    ].to_csv(OUT_DIR / "all_paddy_characteristic_core_distances.csv", index=False, encoding="utf-8-sig")

    plot_coverage(summary[summary["candidate_rule"] == "distance_and_closer_than_non_inundated_core"])
    plot_distance_hist(all_df)

    chosen = summary[summary["candidate_rule"] == "distance_and_closer_than_non_inundated_core"].copy()
    report = [
        "# 全田んぼ画素における特徴的5000近傍画素の存在量",
        "",
        "## 定義",
        "",
        "特徴的5000の正解浸水域を基準に、抽出時に使った7特徴量でロバスト距離を計算した。",
        "",
        "- 距離基準: 特徴的5000の正解浸水域自身の距離分布の p50 / p75 / p90 / p95 / max",
        "- 厳しめ条件: 正解浸水域コアに近く、かつ非浸水域コアよりも正解浸水域コアに近い画素",
        "",
        "## 全田んぼ画素の母数",
        "",
        f"- 全田んぼ有効画素: {total:,}",
        f"- 正解浸水域: {truth_total:,}",
        f"- 非浸水域: {non_total:,}",
        "",
        "## 特徴的浸水域コアに近い画素数",
        "",
        md_table(
            chosen[
                [
                    "threshold_name",
                    "distance_threshold",
                    "candidate_pixels",
                    "all_paddy_percent",
                    "truth_candidate_pixels",
                    "truth_recall_percent",
                    "nontruth_candidate_pixels",
                    "candidate_precision_percent",
                ]
            ]
        ),
        "",
        "## 解釈",
        "",
        "- p50基準は、特徴的5000の中でも中心的な浸水パターンに近い画素だけを拾う厳しい条件である。",
        "- p95基準は、特徴的5000の大半を含む緩い条件である。",
        "- `candidate_precision_percent` が低い場合、特徴的浸水パターンに近い非浸水域画素も多いことを意味する。",
        "- `truth_recall_percent` が低い場合、正解浸水域全体の中でも特徴的5000のような時系列変化を持つ画素は一部に限られる。",
        "",
        "## 出力図",
        "",
        "- `characteristic_core_coverage.png`",
        "- `distance_to_inundated_core_distribution.png`",
        "",
    ]
    (OUT_DIR / "characteristic_core_coverage_report.md").write_text("\n".join(report), encoding="utf-8")
    print(f"Wrote {OUT_DIR}")


if __name__ == "__main__":
    main()
