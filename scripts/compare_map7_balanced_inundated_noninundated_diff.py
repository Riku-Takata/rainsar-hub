#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Compare backscatter differences with equal pixel counts for inundated/non-inundated areas."""

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
OUT_DIR = DETECTION_DIR / "balanced_inundated_noninundated_diff"

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


def summarize(values: np.ndarray) -> dict[str, float | int]:
    values = values[np.isfinite(values)]
    if values.size == 0:
        return {"画素数": 0}
    qs = np.percentile(values, [1, 5, 10, 25, 50, 75, 90, 95, 99])
    return {
        "画素数": int(values.size),
        "平均": float(np.mean(values)),
        "標準偏差": float(np.std(values)),
        "p01": float(qs[0]),
        "p05": float(qs[1]),
        "p10": float(qs[2]),
        "p25": float(qs[3]),
        "中央値": float(qs[4]),
        "p75": float(qs[5]),
        "p90": float(qs[6]),
        "p95": float(qs[7]),
        "p99": float(qs[8]),
        "負の画素割合": float(np.mean(values < 0)),
    }


def cohens_d(a: np.ndarray, b: np.ndarray) -> float:
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if a.size < 2 or b.size < 2:
        return float("nan")
    pooled = np.sqrt(((a.size - 1) * np.var(a, ddof=1) + (b.size - 1) * np.var(b, ddof=1)) / (a.size + b.size - 2))
    if pooled == 0:
        return float("nan")
    return float((np.mean(a) - np.mean(b)) / pooled)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)
    plt = setup_matplotlib()

    diff = {label: read_float(DETECTION_DIR / path) for label, path in DIFF_FILES.items()}
    profile = np.stack([diff[label] for label in ELAPSED], axis=0)
    valid = np.all(np.isfinite(profile), axis=0)
    truth = read_bool(DETECTION_DIR / "map7_inundation_truth_mask.tif")

    truth_mask = truth & valid
    nontruth_mask = (~truth) & valid
    truth_y, truth_x = np.where(truth_mask)
    non_y, non_x = np.where(nontruth_mask)

    if truth_y.size == 0:
        raise RuntimeError("正解浸水域の有効画素がありません。")
    if non_y.size < truth_y.size:
        raise RuntimeError("非浸水域の画素数が正解浸水域より少ないため、同数抽出できません。")

    take = truth_y.size
    idx = rng.choice(non_y.size, size=take, replace=False)
    non_y = non_y[idx]
    non_x = non_x[idx]

    rows = []
    for zone, ys, xs in [
        ("正解浸水域", truth_y, truth_x),
        ("非浸水域_同数抽出", non_y, non_x),
    ]:
        for label in ELAPSED:
            vals = diff[label][ys, xs]
            for value in vals:
                rows.append({"領域": zone, "経過時間帯": label, "差分_target_minus_before": float(value)})
    values_df = pd.DataFrame(rows)
    values_df.to_csv(OUT_DIR / "同数画素_経過時間別_差分値.csv", index=False, encoding="utf-8-sig")

    stats_rows = []
    for label in ELAPSED:
        truth_vals = diff[label][truth_y, truth_x]
        non_vals = diff[label][non_y, non_x]
        for zone, vals in [("正解浸水域", truth_vals), ("非浸水域_同数抽出", non_vals)]:
            row = {"領域": zone, "経過時間帯": label}
            row.update(summarize(vals))
            stats_rows.append(row)
        stats_rows.append(
            {
                "領域": "正解浸水域_minus_非浸水域",
                "経過時間帯": label,
                "画素数": take,
                "平均": float(np.nanmean(truth_vals) - np.nanmean(non_vals)),
                "中央値": float(np.nanmedian(truth_vals) - np.nanmedian(non_vals)),
                "Cohen_d": cohens_d(truth_vals, non_vals),
            }
        )
    stats_df = pd.DataFrame(stats_rows)
    stats_df.to_csv(OUT_DIR / "同数画素_経過時間別_差分統計.csv", index=False, encoding="utf-8-sig")

    summary = pd.DataFrame(
        [
            {"項目": "正解浸水域_有効画素数", "値": int(truth_y.size)},
            {"項目": "非浸水域_全有効画素数", "値": int(nontruth_mask.sum())},
            {"項目": "非浸水域_抽出画素数", "値": int(take)},
            {"項目": "乱数seed", "値": 42},
        ]
    )
    summary.to_csv(OUT_DIR / "同数画素_使用画素数_summary.csv", index=False, encoding="utf-8-sig")

    # Time-series comparison.
    fig, ax = plt.subplots(figsize=(8.2, 5.0), dpi=180)
    colors = {"正解浸水域": "#d62728", "非浸水域_同数抽出": "#4c78a8"}
    for zone in ["正解浸水域", "非浸水域_同数抽出"]:
        sub = stats_df[(stats_df["領域"] == zone)].set_index("経過時間帯").reindex(ELAPSED)
        ax.plot(ELAPSED, sub["中央値"], marker="o", color=colors[zone], label=f"{zone} 中央値")
        ax.plot(ELAPSED, sub["平均"], marker="s", linestyle="--", color=colors[zone], alpha=0.75, label=f"{zone} 平均")
        ax.fill_between(ELAPSED, sub["p25"], sub["p75"], color=colors[zone], alpha=0.12)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("同数画素で比較した後方散乱強度差分")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before (dB)")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図1_同数画素_差分時系列比較.png")
    plt.close(fig)

    # Histograms by elapsed time.
    fig, axes = plt.subplots(2, 2, figsize=(11.0, 7.4), dpi=180, sharey=True)
    for ax, label in zip(axes.ravel(), ELAPSED):
        truth_vals = diff[label][truth_y, truth_x]
        non_vals = diff[label][non_y, non_x]
        both = np.concatenate([truth_vals[np.isfinite(truth_vals)], non_vals[np.isfinite(non_vals)]])
        lo, hi = np.percentile(both, [1, 99])
        bins = np.linspace(lo, hi, 55)
        ax.hist(non_vals, bins=bins, density=True, alpha=0.48, color="#4c78a8", label="非浸水域")
        ax.hist(truth_vals, bins=bins, density=True, alpha=0.48, color="#d62728", label="正解浸水域")
        ax.axvline(np.nanmedian(truth_vals), color="#d62728", linewidth=1.2)
        ax.axvline(np.nanmedian(non_vals), color="#4c78a8", linewidth=1.2)
        ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
        ax.set_title(label)
        ax.grid(True, axis="y", alpha=0.25)
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("同数画素で比較した経過時間別の差分分布", y=1.02)
    fig.supxlabel("差分 target - before (dB)")
    fig.supylabel("密度")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図2_同数画素_経過時間別_差分ヒストグラム.png", bbox_inches="tight")
    plt.close(fig)

    # Boxplot.
    fig, ax = plt.subplots(figsize=(9.2, 5.2), dpi=180)
    data = []
    labels = []
    positions = []
    pos = 1
    for label in ELAPSED:
        data.append(diff[label][truth_y, truth_x])
        labels.append(f"{label}\n正解")
        positions.append(pos)
        data.append(diff[label][non_y, non_x])
        labels.append(f"{label}\n非浸水")
        positions.append(pos + 0.38)
        pos += 1.2
    bp = ax.boxplot(data, positions=positions, widths=0.3, tick_labels=labels, showfliers=False, patch_artist=True)
    for i, patch in enumerate(bp["boxes"]):
        patch.set_facecolor("#d62728" if i % 2 == 0 else "#4c78a8")
        patch.set_alpha(0.45)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("同数画素で比較した差分の箱ひげ図")
    ax.set_ylabel("差分 target - before (dB)")
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図3_同数画素_差分箱ひげ図.png")
    plt.close(fig)

    print(summary.to_string(index=False))
    print(stats_df.to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
