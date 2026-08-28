#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Summarize map7 input data counts and plot inundated/non-inundated features."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import rasterio


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
STAT_DIR = DETECTION_DIR / "statistical_feature_analysis"
LAND_DIR = DETECTION_DIR / "landmask_filter"
OUT_DIR = DETECTION_DIR / "dataset_summary_zone_features"

ELAPSED = ["0-3h", "3-6h", "6-12h", "12-24h"]
DIFF_FILES = {
    "0-3h": "map7_mean_diff_0_3h.tif",
    "3-6h": "map7_mean_diff_3_6h.tif",
    "6-12h": "map7_mean_diff_6_12h.tif",
    "12-24h": "map7_mean_diff_12_24h.tif",
}


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


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


def describe(values: np.ndarray) -> dict[str, float | int]:
    values = values[np.isfinite(values)]
    if values.size == 0:
        return {"画素数": 0}
    q = np.percentile(values, [5, 25, 50, 75, 95])
    return {
        "画素数": int(values.size),
        "平均": float(np.mean(values)),
        "標準偏差": float(np.std(values)),
        "p05": float(q[0]),
        "p25": float(q[1]),
        "中央値": float(q[2]),
        "p75": float(q[3]),
        "p95": float(q[4]),
        "負の画素割合": float(np.mean(values < 0)),
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plt = setup_matplotlib()

    pairs = pd.read_csv(DETECTION_DIR / "map7_detection_pairs.csv", encoding="utf-8-sig")
    usable_pairs = pairs[pairs["valid_pixel_count"] > 0].copy()

    diff = {label: read_float(DETECTION_DIR / filename) for label, filename in DIFF_FILES.items()}
    profile = np.stack([diff[label] for label in ELAPSED], axis=0)
    valid = np.all(np.isfinite(profile), axis=0)

    truth = read_bool(DETECTION_DIR / "map7_inundation_truth_mask.tif")
    detection = read_bool(DETECTION_DIR / "map7_detection_mask.tif")
    paddy = read_bool(LAND_DIR / "map7_paddy_mask.tif")
    road = read_bool(LAND_DIR / "map7_road_mask.tif")

    sample = pd.read_csv(STAT_DIR / "map7_statistical_feature_pixel_sample.csv", encoding="utf-8-sig")
    truth_count_csv = pd.read_csv(DETECTION_DIR / "map7_truth_mask_pixel_counts.csv", encoding="utf-8-sig")

    total_valid = int(valid.sum())
    truth_valid = truth & valid
    non_truth_valid = ~truth & valid

    dataset_rows = [
        {"項目": "検索・整理されたペア数", "値": int(len(pairs)), "備考": "map7_detection_pairs.csv の行数"},
        {"項目": "解析に使えたペア数", "値": int(len(usable_pairs)), "備考": "対象領域で有効画素があるペア"},
        {"項目": "対象領域の有効画素数", "値": total_valid, "備考": "4つの経過時間帯すべてで差分値がある画素"},
        {"項目": "正解浸水域画素数", "値": int(truth_valid.sum()), "備考": "浸水TIF値 0.5-1.7 のunion"},
        {"項目": "非浸水域画素数", "値": int(non_truth_valid.sum()), "備考": "対象領域 - 正解浸水域"},
        {"項目": "田んぼ画素数", "値": int((paddy & valid).sum()), "備考": "DB筆ポリゴン land_type=100"},
        {"項目": "道路画素数", "値": int((road & valid).sum()), "備考": "DB道路ポリゴン"},
        {"項目": "田んぼ内の正解浸水域画素数", "値": int((paddy & truth_valid).sum()), "備考": ""},
        {"項目": "田んぼ内の非浸水域画素数", "値": int((paddy & non_truth_valid).sum()), "備考": ""},
        {"項目": "道路内の正解浸水域画素数", "値": int((road & truth_valid).sum()), "備考": ""},
        {"項目": "道路内の非浸水域画素数", "値": int((road & non_truth_valid).sum()), "備考": ""},
        {"項目": "統計分析サンプル画素数", "値": int(len(sample)), "備考": "Before/Target強度を含む層化サンプル"},
        {"項目": "統計分析サンプル内の正解浸水域画素数", "値": int(sample["正解浸水域"].sum()), "備考": ""},
        {"項目": "統計分析サンプル内の非浸水域画素数", "値": int((~sample["正解浸水域"]).sum()), "備考": ""},
    ]
    dataset_summary = pd.DataFrame(dataset_rows)
    dataset_summary.to_csv(OUT_DIR / "使用データ_全体数_summary.csv", index=False, encoding="utf-8-sig")

    pair_summary = (
        pairs.assign(解析使用=np.where(pairs["valid_pixel_count"] > 0, "使用", "未使用"))
        .groupby(["elapsed_bin", "解析使用"], dropna=False)
        .agg(ペア数=("pair_no", "count"), 平均経過時間_h=("elapsed_h", "mean"), 平均有効画素数=("valid_pixel_count", "mean"))
        .reset_index()
    )
    pair_summary.to_csv(OUT_DIR / "ペア数_経過時間帯別.csv", index=False, encoding="utf-8-sig")

    tif_counts = truth_count_csv.rename(columns={"再投影後_正解浸水画素数": "正解浸水画素数"})
    tif_counts.to_csv(OUT_DIR / "正解浸水TIF別_画素数.csv", index=False, encoding="utf-8-sig")

    zone_masks = {
        "正解浸水域": truth_valid,
        "非浸水域": non_truth_valid,
        "田んぼ内_正解浸水域": truth_valid & paddy,
        "田んぼ内_非浸水域": non_truth_valid & paddy,
        "TP_正解浸水域かつ検出": truth_valid & detection,
        "FN_正解浸水域だが未検出": truth_valid & ~detection,
        "FP_正解外だが誤検出": non_truth_valid & detection,
        "TN_正解外かつ非検出": non_truth_valid & ~detection,
    }
    dist_rows = []
    for zone, mask in zone_masks.items():
        for label in ELAPSED:
            row = {"領域": zone, "経過時間帯": label, "特徴量": "差分_target_minus_before"}
            row.update(describe(diff[label][mask]))
            dist_rows.append(row)
    dist_stats = pd.DataFrame(dist_rows)
    dist_stats.to_csv(OUT_DIR / "領域別_経過時間帯別_差分統計.csv", index=False, encoding="utf-8-sig")

    # Pixel count charts.
    fig, ax = plt.subplots(figsize=(8.2, 4.8), dpi=180)
    use_counts = pair_summary.pivot(index="elapsed_bin", columns="解析使用", values="ペア数").reindex(ELAPSED).fillna(0)
    bottom = np.zeros(len(use_counts))
    colors = {"使用": "#4c78a8", "未使用": "#bab0ac"}
    for col in ["使用", "未使用"]:
        if col in use_counts:
            ax.bar(use_counts.index, use_counts[col], bottom=bottom, label=col, color=colors[col])
            bottom += use_counts[col].to_numpy()
    ax.set_title("経過時間帯別のペア数")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("ペア数")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図1_経過時間帯別_ペア数.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.5, 4.8), dpi=180)
    pixel_items = pd.DataFrame(
        [
            {"領域": "正解浸水域", "画素数": int(truth_valid.sum())},
            {"領域": "非浸水域", "画素数": int(non_truth_valid.sum())},
            {"領域": "田んぼ内_正解", "画素数": int((paddy & truth_valid).sum())},
            {"領域": "田んぼ内_非浸水", "画素数": int((paddy & non_truth_valid).sum())},
            {"領域": "道路内_正解", "画素数": int((road & truth_valid).sum())},
            {"領域": "道路内_非浸水", "画素数": int((road & non_truth_valid).sum())},
        ]
    )
    ax.bar(pixel_items["領域"], pixel_items["画素数"], color=["#d62728", "#4c78a8", "#e377c2", "#59a14f", "#ff9d00", "#9c755f"])
    ax.set_yscale("log")
    ax.set_title("対象領域の画素数")
    ax.set_ylabel("画素数（log）")
    ax.tick_params(axis="x", rotation=25)
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図2_対象領域_画素数.png")
    plt.close(fig)

    # Separate inundated/non-inundated feature plots.
    for zone, color, filename_prefix in [
        ("正解浸水域", "#d62728", "正解浸水域"),
        ("非浸水域", "#4c78a8", "非浸水域"),
    ]:
        sub = dist_stats[(dist_stats["領域"] == zone)].set_index("経過時間帯").reindex(ELAPSED)
        fig, ax = plt.subplots(figsize=(7.8, 4.8), dpi=180)
        ax.plot(ELAPSED, sub["中央値"], marker="o", color=color, label="中央値")
        ax.fill_between(ELAPSED, sub["p25"], sub["p75"], color=color, alpha=0.16, label="p25-p75")
        ax.plot(ELAPSED, sub["平均"], marker="s", linestyle="--", color="#333333", label="平均")
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title(f"{zone}: 経過時間別の差分推移")
        ax.set_xlabel("降雨開始からの経過時間")
        ax.set_ylabel("差分 target - before (dB)")
        ax.grid(True, alpha=0.25)
        ax.legend()
        fig.tight_layout()
        fig.savefig(OUT_DIR / f"図3_{filename_prefix}_差分時系列.png")
        plt.close(fig)

        fig, axes = plt.subplots(2, 2, figsize=(10.5, 7.0), dpi=180, sharey=True)
        mask = zone_masks[zone]
        for ax, label in zip(axes.ravel(), ELAPSED):
            vals = diff[label][mask]
            vals = vals[np.isfinite(vals)]
            lo, hi = np.percentile(vals, [1, 99])
            bins = np.linspace(lo, hi, 55)
            ax.hist(vals, bins=bins, color=color, alpha=0.76)
            ax.axvline(np.median(vals), color="black", linewidth=1.0, label="中央値")
            ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
            ax.set_title(label)
            ax.grid(True, axis="y", alpha=0.25)
        fig.suptitle(f"{zone}: 経過時間帯別の差分分布")
        fig.supxlabel("差分 target - before (dB)")
        fig.supylabel("画素数")
        fig.tight_layout()
        fig.savefig(OUT_DIR / f"図4_{filename_prefix}_差分ヒストグラム.png")
        plt.close(fig)

    # Direct comparison plot.
    fig, ax = plt.subplots(figsize=(8.2, 5.0), dpi=180)
    for zone, color in [("正解浸水域", "#d62728"), ("非浸水域", "#4c78a8")]:
        sub = dist_stats[dist_stats["領域"] == zone].set_index("経過時間帯").reindex(ELAPSED)
        ax.plot(ELAPSED, sub["中央値"], marker="o", color=color, label=f"{zone} 中央値")
        ax.fill_between(ELAPSED, sub["p25"], sub["p75"], color=color, alpha=0.12)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("正解浸水域と非浸水域の差分時系列比較")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before (dB)")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図5_正解浸水域_vs_非浸水域_差分時系列比較.png")
    plt.close(fig)

    # Feature comparison from sample including before/target.
    feature_cols = [
        "差分_早期_minus_後期",
        "差分_0_3h_minus_6_12h",
        "差分_3_6h_minus_6_12h",
        "差分_6-12h",
        "target_平均",
        "before_平均",
        "target_before_平均差",
    ]
    fig, axes = plt.subplots(2, 4, figsize=(15.5, 7.0), dpi=180)
    for ax, feat in zip(axes.ravel(), feature_cols):
        a = sample.loc[sample["正解浸水域"], feat].dropna().to_numpy()
        b = sample.loc[~sample["正解浸水域"], feat].dropna().to_numpy()
        lo, hi = np.nanpercentile(np.concatenate([a, b]), [1, 99])
        bins = np.linspace(lo, hi, 50)
        ax.hist(b, bins=bins, density=True, alpha=0.45, label="非浸水域", color="#4c78a8")
        ax.hist(a, bins=bins, density=True, alpha=0.45, label="正解浸水域", color="#d62728")
        ax.set_title(feat, fontsize=9)
        ax.grid(True, axis="y", alpha=0.25)
    axes.ravel()[-1].axis("off")
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("正解浸水域と非浸水域の特徴量分布", y=1.02)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図6_特徴量分布_正解浸水域_vs_非浸水域.png", bbox_inches="tight")
    plt.close(fig)

    print(dataset_summary.to_string(index=False))
    print(pair_summary.to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
