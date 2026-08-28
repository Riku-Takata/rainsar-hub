#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Plot pixel-wise changes of backscatter differences between elapsed bins."""

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
OUT_DIR = DETECTION_DIR / "pixelwise_diff_change_distribution"

DIFF_FILES = {
    "0-3h": "map7_mean_diff_0_3h.tif",
    "3-6h": "map7_mean_diff_3_6h.tif",
    "6-12h": "map7_mean_diff_6_12h.tif",
    "12-24h": "map7_mean_diff_12_24h.tif",
}
TRANSITIONS = [
    ("0-3h", "3-6h", "3-6h_minus_0-3h"),
    ("3-6h", "6-12h", "6-12h_minus_3-6h"),
    ("6-12h", "12-24h", "12-24h_minus_6-12h"),
]
N_PER_ZONE = 10000
RANDOM_SEED = 42
ABS_CHANGE_LIMIT_DB = 15.0
MAD_K = 4.5


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
        "低下画素割合": float(np.mean(values < 0)),
        "増加画素割合": float(np.mean(values > 0)),
    }


def denoise_values(values: np.ndarray) -> np.ndarray:
    values = values[np.isfinite(values)]
    values = values[np.abs(values) <= ABS_CHANGE_LIMIT_DB]
    if values.size < 20:
        return values
    med = np.median(values)
    mad = np.median(np.abs(values - med))
    if mad > 1.0e-6:
        sigma = 1.4826 * mad
        lo = med - MAD_K * sigma
        hi = med + MAD_K * sigma
    else:
        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1
        lo = q1 - 3.0 * iqr
        hi = q3 + 3.0 * iqr
    return values[(values >= lo) & (values <= hi)]


def sample_equal(values: np.ndarray, n: int, rng: np.random.Generator) -> np.ndarray:
    if values.size <= n:
        return values.copy()
    idx = rng.choice(values.size, size=n, replace=False)
    return values[idx]


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(RANDOM_SEED)
    plt = setup_matplotlib()

    diff = {label: read_float(DETECTION_DIR / path) for label, path in DIFF_FILES.items()}
    stack = np.stack([diff[label] for label in DIFF_FILES], axis=0)
    valid = np.all(np.isfinite(stack), axis=0)
    truth = read_bool(DETECTION_DIR / "map7_inundation_truth_mask.tif")
    zones = {
        "正解浸水域": truth & valid,
        "非浸水域": (~truth) & valid,
    }

    value_rows = []
    stats_rows = []
    noise_rows = []
    for start, end, transition in TRANSITIONS:
        change = diff[end] - diff[start]
        for zone, mask in zones.items():
            raw = change[mask]
            raw = raw[np.isfinite(raw)]
            clean = denoise_values(raw)
            sampled = sample_equal(clean, N_PER_ZONE, rng)
            noise_rows.append(
                {
                    "変化": transition,
                    "領域": zone,
                    "除去前画素数": int(raw.size),
                    "除去後画素数": int(clean.size),
                    "除去画素数": int(raw.size - clean.size),
                    "採用画素数": int(sampled.size),
                }
            )
            row = {"変化": transition, "領域": zone}
            row.update(summarize(sampled))
            stats_rows.append(row)
            for value in sampled:
                value_rows.append({"変化": transition, "領域": zone, "変化量_dB": float(value)})

    values_df = pd.DataFrame(value_rows)
    stats_df = pd.DataFrame(stats_rows)
    noise_df = pd.DataFrame(noise_rows)
    values_df.to_csv(OUT_DIR / "同数10000_画素ごと_差分変化量.csv", index=False, encoding="utf-8-sig")
    stats_df.to_csv(OUT_DIR / "同数10000_画素ごと_差分変化量統計.csv", index=False, encoding="utf-8-sig")
    noise_df.to_csv(OUT_DIR / "画素ごと_差分変化量_ノイズ除去_summary.csv", index=False, encoding="utf-8-sig")

    comp_rows = []
    for transition in [x[2] for x in TRANSITIONS]:
        t = values_df[(values_df["変化"] == transition) & (values_df["領域"] == "正解浸水域")]["変化量_dB"].to_numpy()
        b = values_df[(values_df["変化"] == transition) & (values_df["領域"] == "非浸水域")]["変化量_dB"].to_numpy()
        comp_rows.append(
            {
                "変化": transition,
                "画素数_each": int(min(t.size, b.size)),
                "正解平均": float(np.mean(t)),
                "非浸水平均": float(np.mean(b)),
                "平均差_正解minus非浸水": float(np.mean(t) - np.mean(b)),
                "正解中央値": float(np.median(t)),
                "非浸水中央値": float(np.median(b)),
                "中央値差_正解minus非浸水": float(np.median(t) - np.median(b)),
                "正解_低下画素割合": float(np.mean(t < 0)),
                "非浸水_低下画素割合": float(np.mean(b < 0)),
            }
        )
    comp_df = pd.DataFrame(comp_rows)
    comp_df.to_csv(OUT_DIR / "同数10000_画素ごと_正解minus非浸水_比較.csv", index=False, encoding="utf-8-sig")

    colors = {"正解浸水域": "#d62728", "非浸水域": "#4c78a8"}
    labels = [x[2] for x in TRANSITIONS]

    fig, ax = plt.subplots(figsize=(9.2, 5.0), dpi=180)
    for zone in ["正解浸水域", "非浸水域"]:
        sub = stats_df[stats_df["領域"] == zone].set_index("変化").reindex(labels)
        ax.plot(labels, sub["平均"], marker="o", color=colors[zone], label=f"{zone} 平均")
        ax.plot(labels, sub["中央値"], marker="s", linestyle="--", color=colors[zone], alpha=0.75, label=f"{zone} 中央値")
        ax.fill_between(labels, sub["p25"], sub["p75"], color=colors[zone], alpha=0.12)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("画素ごとの後方散乱強度差分の変化")
    ax.set_xlabel("経過時間帯間の変化")
    ax.set_ylabel("変化量 dB: 後の時間帯 - 前の時間帯")
    ax.tick_params(axis="x", rotation=12)
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図1_画素ごと_差分変化量_時系列.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.6, 4.8), dpi=180)
    ax.bar(comp_df["変化"], comp_df["平均差_正解minus非浸水"], color="#d62728", alpha=0.72, label="平均差")
    ax.plot(comp_df["変化"], comp_df["中央値差_正解minus非浸水"], marker="o", color="#333333", label="中央値差")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("画素ごとの変化量: 正解浸水域 - 非浸水域")
    ax.set_xlabel("経過時間帯間の変化")
    ax.set_ylabel("変化量の差 (dB)")
    ax.tick_params(axis="x", rotation=12)
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図2_画素ごと_変化量_正解minus非浸水.png")
    plt.close(fig)

    fig, axes = plt.subplots(1, 3, figsize=(15.0, 4.8), dpi=180, sharey=True)
    for ax, transition in zip(axes, labels):
        sub = values_df[values_df["変化"] == transition]
        t = sub[sub["領域"] == "正解浸水域"]["変化量_dB"].to_numpy()
        b = sub[sub["領域"] == "非浸水域"]["変化量_dB"].to_numpy()
        both = np.concatenate([t, b])
        lo, hi = np.percentile(both, [1, 99])
        bins = np.linspace(lo, hi, 55)
        ax.hist(b, bins=bins, density=True, alpha=0.45, color=colors["非浸水域"], label="非浸水域")
        ax.hist(t, bins=bins, density=True, alpha=0.45, color=colors["正解浸水域"], label="正解浸水域")
        ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
        ax.set_title(transition)
        ax.grid(True, axis="y", alpha=0.25)
    handles, legend_labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, legend_labels, loc="upper center", ncol=2)
    fig.suptitle("画素ごとの差分変化量の分布")
    fig.supxlabel("変化量 dB: 後の時間帯 - 前の時間帯")
    fig.supylabel("密度")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図3_画素ごと_差分変化量ヒストグラム.png", bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9.0, 5.0), dpi=180)
    width = 0.35
    x = np.arange(len(labels))
    t_rate = comp_df["正解_低下画素割合"].to_numpy() * 100
    b_rate = comp_df["非浸水_低下画素割合"].to_numpy() * 100
    ax.bar(x - width / 2, t_rate, width, color=colors["正解浸水域"], label="正解浸水域")
    ax.bar(x + width / 2, b_rate, width, color=colors["非浸水域"], label="非浸水域")
    ax.set_xticks(x, labels, rotation=12)
    ax.set_ylabel("低下した画素割合 (%)")
    ax.set_title("後方散乱強度差分が低下した画素の割合")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図4_画素ごと_低下画素割合.png")
    plt.close(fig)

    print(noise_df.to_string(index=False))
    print(comp_df.to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
