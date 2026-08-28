#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Balance pixel counts between truth inundation and full-scene background."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
    / "full_scene_without_truth_vs_truth"
)
OUT_DIR = BASE_DIR / "balanced_pixel_count"
SAMPLE_CSV = BASE_DIR / "ペア別_正解浸水域_vs_シーン全体除外_差分サンプル.csv"
ELAPSED = ["0-3h", "3-6h", "6-12h", "12-24h"]


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def summarize(values: np.ndarray) -> dict[str, float | int]:
    values = values[np.isfinite(values)]
    if values.size == 0:
        return {"画素数": 0}
    q = np.percentile(values, [1, 5, 10, 25, 50, 75, 90, 95, 99])
    return {
        "画素数": int(values.size),
        "平均": float(np.mean(values)),
        "標準偏差": float(np.std(values)),
        "p01": float(q[0]),
        "p05": float(q[1]),
        "p10": float(q[2]),
        "p25": float(q[3]),
        "中央値": float(q[4]),
        "p75": float(q[5]),
        "p90": float(q[6]),
        "p95": float(q[7]),
        "p99": float(q[8]),
        "負の画素割合": float(np.mean(values < 0)),
    }


def cohens_d(a: np.ndarray, b: np.ndarray) -> float:
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if a.size < 2 or b.size < 2:
        return np.nan
    pooled = np.sqrt(((a.size - 1) * np.var(a, ddof=1) + (b.size - 1) * np.var(b, ddof=1)) / (a.size + b.size - 2))
    return float((np.mean(a) - np.mean(b)) / pooled) if pooled > 0 else np.nan


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)
    plt = setup_matplotlib()

    df = pd.read_csv(SAMPLE_CSV, encoding="utf-8-sig")
    df = df[np.isfinite(df["diff_target_minus_before"])]

    balanced_rows = []
    stats_rows = []
    comp_rows = []
    for elapsed in ELAPSED:
        truth = df[(df["elapsed_bin"] == elapsed) & (df["領域"] == "正解浸水域")][
            "diff_target_minus_before"
        ].to_numpy(dtype=np.float32)
        bg = df[(df["elapsed_bin"] == elapsed) & (df["領域"] == "シーン全体_正解浸水域除外")][
            "diff_target_minus_before"
        ].to_numpy(dtype=np.float32)
        n = min(truth.size, bg.size)
        if n == 0:
            continue
        truth_use = truth if truth.size == n else truth[rng.choice(truth.size, size=n, replace=False)]
        bg_use = bg if bg.size == n else bg[rng.choice(bg.size, size=n, replace=False)]

        for zone, values in [("正解浸水域", truth_use), ("背景_正解浸水域除外_同数抽出", bg_use)]:
            row = {"領域": zone, "経過時間帯": elapsed}
            row.update(summarize(values))
            stats_rows.append(row)
            for value in values:
                balanced_rows.append({"領域": zone, "経過時間帯": elapsed, "差分_target_minus_before": float(value)})

        comp_rows.append(
            {
                "経過時間帯": elapsed,
                "比較画素数_each": int(n),
                "正解平均": float(np.mean(truth_use)),
                "背景平均": float(np.mean(bg_use)),
                "平均差_正解minus背景": float(np.mean(truth_use) - np.mean(bg_use)),
                "正解中央値": float(np.median(truth_use)),
                "背景中央値": float(np.median(bg_use)),
                "中央値差_正解minus背景": float(np.median(truth_use) - np.median(bg_use)),
                "正解_負の画素割合": float(np.mean(truth_use < 0)),
                "背景_負の画素割合": float(np.mean(bg_use < 0)),
                "負の画素割合差_正解minus背景": float(np.mean(truth_use < 0) - np.mean(bg_use < 0)),
                "Cohen_d": cohens_d(truth_use, bg_use),
            }
        )

    balanced = pd.DataFrame(balanced_rows)
    stats = pd.DataFrame(stats_rows)
    comp = pd.DataFrame(comp_rows)
    balanced.to_csv(OUT_DIR / "同数画素_正解浸水域_vs_背景_差分値.csv", index=False, encoding="utf-8-sig")
    stats.to_csv(OUT_DIR / "同数画素_正解浸水域_vs_背景_差分統計.csv", index=False, encoding="utf-8-sig")
    comp.to_csv(OUT_DIR / "同数画素_正解浸水域_minus_背景_比較.csv", index=False, encoding="utf-8-sig")

    fig, ax = plt.subplots(figsize=(8.6, 5.0), dpi=180)
    colors = {"正解浸水域": "#d62728", "背景_正解浸水域除外_同数抽出": "#4c78a8"}
    for zone in ["正解浸水域", "背景_正解浸水域除外_同数抽出"]:
        sub = stats[stats["領域"] == zone].set_index("経過時間帯").reindex(ELAPSED)
        ax.plot(ELAPSED, sub["平均"], marker="o", color=colors[zone], label=f"{zone} 平均")
        ax.plot(ELAPSED, sub["中央値"], marker="s", linestyle="--", color=colors[zone], alpha=0.75, label=f"{zone} 中央値")
        ax.fill_between(ELAPSED, sub["p25"], sub["p75"], color=colors[zone], alpha=0.12)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("同数画素で比較した正解浸水域と背景の差分時系列")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before (dB)")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図1_同数画素_正解浸水域_vs_背景_差分時系列.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.2, 4.8), dpi=180)
    ax.bar(comp["経過時間帯"], comp["平均差_正解minus背景"], color="#d62728", alpha=0.72, label="平均差")
    ax.plot(comp["経過時間帯"], comp["中央値差_正解minus背景"], color="#333333", marker="o", label="中央値差")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("同数画素: 正解浸水域 - 背景")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分差 (dB)")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図2_同数画素_正解浸水域_minus_背景.png")
    plt.close(fig)

    fig, axes = plt.subplots(2, 2, figsize=(11.0, 7.4), dpi=180, sharey=True)
    for ax, elapsed in zip(axes.ravel(), ELAPSED):
        sub = balanced[balanced["経過時間帯"] == elapsed]
        vals_t = sub[sub["領域"] == "正解浸水域"]["差分_target_minus_before"].to_numpy()
        vals_b = sub[sub["領域"] == "背景_正解浸水域除外_同数抽出"]["差分_target_minus_before"].to_numpy()
        both = np.concatenate([vals_t, vals_b])
        lo, hi = np.percentile(both, [1, 99])
        bins = np.linspace(lo, hi, 55)
        ax.hist(vals_b, bins=bins, density=True, color="#4c78a8", alpha=0.45, label="背景")
        ax.hist(vals_t, bins=bins, density=True, color="#d62728", alpha=0.45, label="正解浸水域")
        ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
        ax.set_title(elapsed)
        ax.grid(True, axis="y", alpha=0.25)
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("同数画素: 経過時間帯別の差分分布")
    fig.supxlabel("差分 target - before (dB)")
    fig.supylabel("密度")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図3_同数画素_正解浸水域_vs_背景_差分ヒストグラム.png", bbox_inches="tight")
    plt.close(fig)

    print(comp.to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
