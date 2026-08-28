#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Analyze false positives/negatives in the map7 inundation detection test."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_DIR = (
    ROOT_DIR
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)

ELAPSED_LABELS = ["0-3h", "3-6h", "6-12h", "12-24h"]


def read_raster(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1)


def normalize_profile(values: np.ndarray) -> np.ndarray:
    std = float(np.nanstd(values))
    if not np.isfinite(std) or std <= 0:
        return np.full(values.shape, np.nan, dtype=np.float64)
    return (values - float(np.nanmean(values))) / std


def corr_with_signature(profile_stack: np.ndarray, signature: np.ndarray) -> np.ndarray:
    sig = normalize_profile(signature)
    out = np.full(profile_stack.shape[1:], np.nan, dtype=np.float32)
    valid = np.all(np.isfinite(profile_stack), axis=0)
    values = profile_stack[:, valid]
    centered = values - np.mean(values, axis=0, keepdims=True)
    std = np.std(values, axis=0)
    corr = np.sum(centered * sig[:, None], axis=0) / (std * profile_stack.shape[0])
    corr[~np.isfinite(corr)] = np.nan
    out[valid] = corr.astype(np.float32)
    return out


def zrmse(profile_stack: np.ndarray, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    std = np.where(std > 0, std, 1.0)
    out = np.full(profile_stack.shape[1:], np.nan, dtype=np.float32)
    valid = np.all(np.isfinite(profile_stack), axis=0)
    values = profile_stack[:, valid]
    z = (values - mean[:, None]) / std[:, None]
    out[valid] = np.sqrt(np.mean(z**2, axis=0)).astype(np.float32)
    return out


def summarize(values: np.ndarray) -> dict[str, float | int]:
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
    }


def metrics(pred: np.ndarray, truth: np.ndarray, valid: np.ndarray) -> dict[str, float | int]:
    tp = int(np.sum(pred & truth & valid))
    fp = int(np.sum(pred & ~truth & valid))
    fn = int(np.sum(~pred & truth & valid))
    tn = int(np.sum(~pred & ~truth & valid))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "適合率_precision": precision,
        "再現率_recall": recall,
        "F1": f1,
        "検出画素数": tp + fp,
    }


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    args = parser.parse_args()
    out_dir = args.input_dir / "error_analysis"
    out_dir.mkdir(parents=True, exist_ok=True)

    d0 = read_raster(args.input_dir / "map7_mean_diff_0_3h.tif").astype(np.float32)
    d3 = read_raster(args.input_dir / "map7_mean_diff_3_6h.tif").astype(np.float32)
    d6 = read_raster(args.input_dir / "map7_mean_diff_6_12h.tif").astype(np.float32)
    d12 = read_raster(args.input_dir / "map7_mean_diff_12_24h.tif").astype(np.float32)
    profile = np.stack([d0, d3, d6, d12], axis=0)
    valid = np.all(np.isfinite(profile), axis=0)

    truth = read_raster(args.input_dir / "map7_inundation_truth_mask.tif") > 0
    detected = read_raster(args.input_dir / "map7_detection_mask.tif") > 0
    criteria = pd.read_csv(args.input_dir / "map7_detection_criteria.csv", encoding="utf-8-sig")
    inun_mean = criteria["久留米浸水域_平均差分"].to_numpy(dtype=np.float64)
    inun_std = criteria["久留米浸水域_標準偏差"].to_numpy(dtype=np.float64)
    scene_mean = criteria["久留米シーン全体_平均差分"].to_numpy(dtype=np.float64)
    scene_std = criteria["久留米シーン全体_標準偏差"].to_numpy(dtype=np.float64)

    corr = corr_with_signature(profile, inun_mean)
    z_inun = zrmse(profile, inun_mean, inun_std)
    z_scene = zrmse(profile, scene_mean, scene_std)
    early = (d0 + d3) / 2.0
    late = (d6 + d12) / 2.0
    features = {
        "差分_0_3h": d0,
        "差分_3_6h": d3,
        "差分_6_12h": d6,
        "差分_12_24h": d12,
        "早期平均_0_6h": early,
        "後期平均_6_24h": late,
        "早期_minus_後期": early - late,
        "0_3h_minus_6_12h": d0 - d6,
        "3_6h_minus_6_12h": d3 - d6,
        "12_24h_minus_6_12h": d12 - d6,
        "時系列レンジ": np.nanmax(profile, axis=0) - np.nanmin(profile, axis=0),
        "久留米浸水域_corr": corr,
        "久留米浸水域_zRMSE": z_inun,
        "久留米シーン_zRMSE": z_scene,
        "zRMSE差_scene_minus_inun": z_scene - z_inun,
    }

    zones = {
        "TP_正解浸水域かつ検出": truth & detected & valid,
        "FN_正解浸水域だが未検出": truth & ~detected & valid,
        "FP_正解外だが誤検出": ~truth & detected & valid,
        "TN_正解外かつ非検出": ~truth & ~detected & valid,
        "正解浸水域_all": truth & valid,
        "正解外_all": ~truth & valid,
    }

    rows = []
    for zone_name, mask in zones.items():
        for feature_name, arr in features.items():
            row = {"領域": zone_name, "特徴量": feature_name}
            row.update(summarize(arr[mask]))
            rows.append(row)
    feature_stats = pd.DataFrame(rows)
    feature_stats.to_csv(out_dir / "map7_error_feature_stats.csv", index=False, encoding="utf-8-sig")

    current_metrics = metrics(detected, truth, valid)
    current_metrics["条件"] = "現行条件"
    metric_rows = [current_metrics]

    # Candidate rule search. Keep it interpretable: thresholds on similarity, dip after rain, and 6-12h low response.
    for z_th in [0.5, 0.75, 1.0, 1.25]:
        for corr_th in [0.5, 0.7, 0.85]:
            for dip_th in [0.0, 0.75, 1.5, 2.25]:
                for mid_max in [-1.0, -0.5, 0.0, 0.5]:
                    pred = (
                        valid
                        & (corr >= corr_th)
                        & (z_inun <= z_th)
                        & (z_inun < z_scene)
                        & ((early - late) >= dip_th)
                        & (d6 <= mid_max)
                    )
                    row = metrics(pred, truth, valid)
                    row["条件"] = f"z<={z_th}, corr>={corr_th}, early-late>={dip_th}, 6-12h<={mid_max}"
                    row["zRMSE閾値"] = z_th
                    row["corr閾値"] = corr_th
                    row["早期後期差閾値"] = dip_th
                    row["6_12h上限"] = mid_max
                    metric_rows.append(row)
    metric_df = pd.DataFrame(metric_rows)
    metric_df.to_csv(out_dir / "map7_candidate_rule_metrics.csv", index=False, encoding="utf-8-sig")

    # Pick practical candidates: highest F1, highest precision with at least 5% recall, highest recall with precision >= current.
    current_precision = current_metrics["適合率_precision"]
    ranked = []
    ranked.append(metric_df.sort_values("F1", ascending=False).head(1).assign(選定理由="F1最大"))
    ranked.append(
        metric_df[metric_df["再現率_recall"] >= 0.05]
        .sort_values("適合率_precision", ascending=False)
        .head(1)
        .assign(選定理由="再現率5%以上で適合率最大")
    )
    ranked.append(
        metric_df[metric_df["適合率_precision"] >= current_precision]
        .sort_values("再現率_recall", ascending=False)
        .head(1)
        .assign(選定理由="現行以上の適合率で再現率最大")
    )
    pd.concat(ranked, ignore_index=True).to_csv(out_dir / "map7_recommended_rules.csv", index=False, encoding="utf-8-sig")

    plt = setup_matplotlib()
    profile_rows = []
    for zone in ["TP_正解浸水域かつ検出", "FN_正解浸水域だが未検出", "FP_正解外だが誤検出", "TN_正解外かつ非検出"]:
        for label, name in zip(["0-3h", "3-6h", "6-12h", "12-24h"], ["差分_0_3h", "差分_3_6h", "差分_6_12h", "差分_12_24h"]):
            sub = feature_stats[(feature_stats["領域"] == zone) & (feature_stats["特徴量"] == name)].iloc[0]
            profile_rows.append({"領域": zone, "経過時間帯": label, "平均差分": sub["平均"], "p25": sub["p25"], "p75": sub["p75"]})
    profile_df = pd.DataFrame(profile_rows)
    profile_df.to_csv(out_dir / "map7_error_profile_summary.csv", index=False, encoding="utf-8-sig")

    fig, ax = plt.subplots(figsize=(9.5, 5.2), dpi=180)
    colors = {
        "TP_正解浸水域かつ検出": "#2ca02c",
        "FN_正解浸水域だが未検出": "#1f77b4",
        "FP_正解外だが誤検出": "#d62728",
        "TN_正解外かつ非検出": "#7f7f7f",
    }
    for zone, color in colors.items():
        g = profile_df[profile_df["領域"] == zone].set_index("経過時間帯").reindex(["0-3h", "3-6h", "6-12h", "12-24h"])
        ax.plot(g.index, g["平均差分"], marker="o", label=zone, color=color)
        ax.fill_between(g.index, g["p25"], g["p75"], color=color, alpha=0.12)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("map7 誤検出分析：分類別の差分時系列")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("平均差分 target - before")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out_dir / "図_分類別_差分時系列.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.2, 5.0), dpi=180)
    plot_df = metric_df.copy()
    ax.scatter(plot_df["再現率_recall"] * 100, plot_df["適合率_precision"] * 100, s=20, alpha=0.55, color="#4c78a8")
    ax.scatter(current_metrics["再現率_recall"] * 100, current_metrics["適合率_precision"] * 100, s=80, color="#d62728", label="現行条件")
    ax.set_xlabel("再現率 recall (%)")
    ax.set_ylabel("適合率 precision (%)")
    ax.set_title("候補ルールの適合率・再現率")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "図_候補ルール_precision_recall.png")
    plt.close(fig)

    print("current")
    print(pd.DataFrame([current_metrics]).to_string(index=False))
    print("recommended")
    print(pd.read_csv(out_dir / "map7_recommended_rules.csv", encoding="utf-8-sig").to_string(index=False))
    print(f"saved: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
