#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Analyze statistical features of known inundation pixels in map7."""

from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
PAIR_DIR = ROOT / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed_by_date"
LAND_DIR = DETECTION_DIR / "landmask_filter"
OUT_DIR = DETECTION_DIR / "statistical_feature_analysis"

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


def read_raster(path: Path) -> tuple[np.ndarray, dict]:
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32), {
            "crs": src.crs,
            "transform": src.transform,
            "height": src.height,
            "width": src.width,
            "profile": src.profile.copy(),
        }


def read_mask(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1) > 0


def read_manifest(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    out: dict[tuple[str, str], dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            key = (row["rain_day_jst"], row["pair_no"])
            out.setdefault(key, {})[row["role"]] = row["organized_path"]
    return out


def aligned_read(path: Path, template: dict) -> np.ndarray:
    with rasterio.open(path) as src:
        with WarpedVRT(
            src,
            crs=template["crs"],
            transform=template["transform"],
            width=template["width"],
            height=template["height"],
            resampling=Resampling.bilinear,
            nodata=src.nodata,
        ) as vrt:
            arr = vrt.read(1).astype(np.float32)
            nodata = vrt.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def mean_by_elapsed(pairs: pd.DataFrame, manifest: dict[tuple[str, str], dict[str, str]], template: dict):
    target_means: dict[str, np.ndarray] = {}
    before_means: dict[str, np.ndarray] = {}
    counts: dict[str, np.ndarray] = {}

    for label in ELAPSED:
        target_sum = np.zeros((template["height"], template["width"]), dtype=np.float64)
        before_sum = np.zeros_like(target_sum)
        count = np.zeros((template["height"], template["width"]), dtype=np.uint16)

        sub = pairs[(pairs["elapsed_bin"] == label) & (pairs["valid_pixel_count"] > 0)]
        for _, row in sub.iterrows():
            paths = manifest.get((str(row["rain_day_jst"]), str(row["pair_no"])), {})
            if "target" not in paths or "pair" not in paths:
                continue
            target = aligned_read(Path(paths["target"]), template)
            before = aligned_read(Path(paths["pair"]), template)
            valid = np.isfinite(target) & np.isfinite(before)
            target_sum[valid] += target[valid]
            before_sum[valid] += before[valid]
            count[valid] += 1

        target = np.full_like(target_sum, np.nan, dtype=np.float32)
        before = np.full_like(before_sum, np.nan, dtype=np.float32)
        valid = count > 0
        target[valid] = (target_sum[valid] / count[valid]).astype(np.float32)
        before[valid] = (before_sum[valid] / count[valid]).astype(np.float32)
        target_means[label] = target
        before_means[label] = before
        counts[label] = count

    return target_means, before_means, counts


def add_profile_features(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    cols = [f"{prefix}_{x}" for x in ELAPSED]
    vals = df[cols].to_numpy(dtype=np.float32)
    df[f"{prefix}_平均"] = np.nanmean(vals, axis=1)
    df[f"{prefix}_中央値"] = np.nanmedian(vals, axis=1)
    df[f"{prefix}_標準偏差"] = np.nanstd(vals, axis=1)
    df[f"{prefix}_範囲"] = np.nanmax(vals, axis=1) - np.nanmin(vals, axis=1)
    df[f"{prefix}_早期平均_0_6h"] = np.nanmean(vals[:, 0:2], axis=1)
    df[f"{prefix}_後期平均_6_24h"] = np.nanmean(vals[:, 2:4], axis=1)
    df[f"{prefix}_早期_minus_後期"] = df[f"{prefix}_早期平均_0_6h"] - df[f"{prefix}_後期平均_6_24h"]
    df[f"{prefix}_0_3h_minus_6_12h"] = vals[:, 0] - vals[:, 2]
    df[f"{prefix}_3_6h_minus_6_12h"] = vals[:, 1] - vals[:, 2]
    df[f"{prefix}_6_12h_minus_12_24h"] = vals[:, 2] - vals[:, 3]
    if prefix == "差分":
        df["差分_負の時間帯数"] = np.sum(vals < 0, axis=1)
        df["差分_単調低下スコア"] = (vals[:, 0] >= vals[:, 1]).astype(int) + (vals[:, 1] >= vals[:, 2]).astype(int)
    return df


def stratified_sample(mask: np.ndarray, truth: np.ndarray, paddy: np.ndarray, n_each: int = 80000) -> np.ndarray:
    rng = np.random.default_rng(42)
    selected = np.zeros(mask.shape, dtype=bool)
    strata = [
        truth & paddy & mask,
        truth & ~paddy & mask,
        ~truth & paddy & mask,
        ~truth & ~paddy & mask,
    ]
    for s in strata:
        ys, xs = np.where(s)
        if ys.size == 0:
            continue
        take = min(n_each, ys.size)
        idx = rng.choice(ys.size, size=take, replace=False)
        selected[ys[idx], xs[idx]] = True
    return selected


def summarize(values: pd.Series) -> dict[str, float | int]:
    arr = values.to_numpy(dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return {"画素数": 0}
    qs = np.percentile(arr, [1, 5, 10, 25, 50, 75, 90, 95, 99])
    return {
        "画素数": int(arr.size),
        "平均": float(np.mean(arr)),
        "標準偏差": float(np.std(arr)),
        "p01": float(qs[0]),
        "p05": float(qs[1]),
        "p10": float(qs[2]),
        "p25": float(qs[3]),
        "中央値": float(qs[4]),
        "p75": float(qs[5]),
        "p90": float(qs[6]),
        "p95": float(qs[7]),
        "p99": float(qs[8]),
    }


def auc_score(feature: np.ndarray, label: np.ndarray) -> float:
    ok = np.isfinite(feature)
    x = feature[ok]
    y = label[ok].astype(bool)
    if x.size == 0 or y.sum() == 0 or (~y).sum() == 0:
        return np.nan
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty_like(order, dtype=np.float64)
    ranks[order] = np.arange(1, x.size + 1)
    # Average ranks for ties.
    sx = x[order]
    start = 0
    while start < sx.size:
        end = start + 1
        while end < sx.size and sx[end] == sx[start]:
            end += 1
        if end - start > 1:
            ranks[order[start:end]] = (start + 1 + end) / 2.0
        start = end
    n_pos = y.sum()
    n_neg = (~y).sum()
    rank_sum = ranks[y].sum()
    return float((rank_sum - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg))


def threshold_scan(values: np.ndarray, label: np.ndarray) -> pd.DataFrame:
    ok = np.isfinite(values)
    x = values[ok]
    y = label[ok].astype(bool)
    if x.size == 0:
        return pd.DataFrame()
    qs = np.unique(np.percentile(x, np.linspace(1, 99, 99)))
    rows = []
    for direction in [">=", "<="]:
        for th in qs:
            pred = x >= th if direction == ">=" else x <= th
            tp = int(np.sum(pred & y))
            fp = int(np.sum(pred & ~y))
            fn = int(np.sum(~pred & y))
            tn = int(np.sum(~pred & ~y))
            precision = tp / (tp + fp) if tp + fp else 0.0
            recall = tp / (tp + fn) if tp + fn else 0.0
            specificity = tn / (tn + fp) if tn + fp else 0.0
            f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
            rows.append(
                {
                    "方向": direction,
                    "閾値": float(th),
                    "precision": precision,
                    "recall": recall,
                    "specificity": specificity,
                    "balanced_accuracy": (recall + specificity) / 2.0,
                    "F1": f1,
                    "TP": tp,
                    "FP": fp,
                    "FN": fn,
                    "TN": tn,
                }
            )
    return pd.DataFrame(rows)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    diff = {}
    first, template = read_raster(DETECTION_DIR / DIFF_FILES["0-3h"])
    diff["0-3h"] = first
    for label in ELAPSED[1:]:
        diff[label], _ = read_raster(DETECTION_DIR / DIFF_FILES[label])

    truth = read_mask(DETECTION_DIR / "map7_inundation_truth_mask.tif")
    detection = read_mask(DETECTION_DIR / "map7_detection_mask.tif")
    paddy = read_mask(LAND_DIR / "map7_paddy_mask.tif")
    road = read_mask(LAND_DIR / "map7_road_mask.tif")

    profile = np.stack([diff[x] for x in ELAPSED], axis=0)
    valid = np.all(np.isfinite(profile), axis=0)

    pairs = pd.read_csv(DETECTION_DIR / "map7_detection_pairs.csv", encoding="utf-8-sig")
    manifest = read_manifest(PAIR_DIR / "manifest.csv")
    target, before, counts = mean_by_elapsed(pairs, manifest, template)

    intensity_valid = valid.copy()
    for label in ELAPSED:
        intensity_valid &= np.isfinite(target[label]) & np.isfinite(before[label])
    sample_mask = stratified_sample(intensity_valid, truth, paddy)
    y, x = np.where(sample_mask)

    data = {
        "row": y,
        "col": x,
        "正解浸水域": truth[y, x].astype(bool),
        "既存条件で検出": detection[y, x].astype(bool),
        "田んぼ": paddy[y, x].astype(bool),
        "道路": road[y, x].astype(bool),
    }
    for label in ELAPSED:
        data[f"差分_{label}"] = diff[label][y, x]
        data[f"target_{label}"] = target[label][y, x]
        data[f"before_{label}"] = before[label][y, x]
    df = pd.DataFrame(data)
    for prefix in ["差分", "target", "before"]:
        df = add_profile_features(df, prefix)
    df["target_before_平均差"] = df["target_平均"] - df["before_平均"]
    df["before_早期_minus_後期"] = df["before_早期平均_0_6h"] - df["before_後期平均_6_24h"]

    def zone_name(row: pd.Series) -> str:
        if row["正解浸水域"] and row["既存条件で検出"]:
            return "TP_正解浸水域かつ検出"
        if row["正解浸水域"] and not row["既存条件で検出"]:
            return "FN_正解浸水域だが未検出"
        if not row["正解浸水域"] and row["既存条件で検出"]:
            return "FP_正解外だが誤検出"
        return "TN_正解外かつ非検出"

    df["分類"] = df.apply(zone_name, axis=1)
    df["土地分類"] = np.select(
        [df["田んぼ"] & df["道路"], df["田んぼ"], df["道路"]],
        ["田んぼ+道路", "田んぼ", "道路"],
        default="その他",
    )
    df.to_csv(OUT_DIR / "map7_statistical_feature_pixel_sample.csv", index=False, encoding="utf-8-sig")

    feature_cols = [
        c
        for c in df.columns
        if c.startswith("差分_") or c.startswith("target_") or c.startswith("before_")
    ]
    feature_cols += ["target_before_平均差"]
    feature_cols = [c for c in feature_cols if c not in {"差分_負の時間帯数", "差分_単調低下スコア"}] + [
        "差分_負の時間帯数",
        "差分_単調低下スコア",
    ]

    stats_rows = []
    group_defs = {
        "正解浸水域": df["正解浸水域"],
        "非浸水域": ~df["正解浸水域"],
        "田んぼ内_正解浸水域": df["正解浸水域"] & df["田んぼ"],
        "田んぼ内_非浸水域": ~df["正解浸水域"] & df["田んぼ"],
        "TP_正解浸水域かつ検出": df["分類"].eq("TP_正解浸水域かつ検出"),
        "FN_正解浸水域だが未検出": df["分類"].eq("FN_正解浸水域だが未検出"),
        "FP_正解外だが誤検出": df["分類"].eq("FP_正解外だが誤検出"),
        "TN_正解外かつ非検出": df["分類"].eq("TN_正解外かつ非検出"),
    }
    for group, mask in group_defs.items():
        for feature in feature_cols:
            row = {"グループ": group, "特徴量": feature}
            row.update(summarize(df.loc[mask, feature]))
            stats_rows.append(row)
    stats = pd.DataFrame(stats_rows)
    stats.to_csv(OUT_DIR / "map7_feature_distribution_stats.csv", index=False, encoding="utf-8-sig")

    sep_rows = []
    label = df["正解浸水域"].to_numpy(dtype=bool)
    for feature in feature_cols:
        arr = df[feature].to_numpy(dtype=np.float64)
        auc = auc_score(arr, label)
        auc_use = max(auc, 1.0 - auc) if np.isfinite(auc) else np.nan
        direction = ">=" if auc >= 0.5 else "<="
        scan = threshold_scan(arr, label)
        best_f1 = scan.sort_values("F1", ascending=False).head(1).to_dict("records")[0]
        best_ba = scan.sort_values("balanced_accuracy", ascending=False).head(1).to_dict("records")[0]
        sep_rows.append(
            {
                "特徴量": feature,
                "AUC": auc,
                "分離力_AUC大きい側": auc_use,
                "浸水域が大きい方向": direction,
                "F1最大_方向": best_f1["方向"],
                "F1最大_閾値": best_f1["閾値"],
                "F1最大_precision": best_f1["precision"],
                "F1最大_recall": best_f1["recall"],
                "F1最大_F1": best_f1["F1"],
                "BA最大_方向": best_ba["方向"],
                "BA最大_閾値": best_ba["閾値"],
                "BA最大_precision": best_ba["precision"],
                "BA最大_recall": best_ba["recall"],
                "BA最大_specificity": best_ba["specificity"],
                "BA最大_balanced_accuracy": best_ba["balanced_accuracy"],
            }
        )
    sep = pd.DataFrame(sep_rows).sort_values("分離力_AUC大きい側", ascending=False)
    sep.to_csv(OUT_DIR / "map7_feature_separation_scores.csv", index=False, encoding="utf-8-sig")

    # Simple interpretable two-feature rule candidates within paddy pixels.
    paddy_df = df[df["田んぼ"]].copy()
    rule_rows = []
    for early_th in np.arange(0.0, 4.01, 0.25):
        for mid_th in np.arange(-2.5, 2.51, 0.25):
            for before_th in [None, -17, -16, -15, -14, -13]:
                pred = (paddy_df["差分_早期_minus_後期"] >= early_th) & (paddy_df["差分_6-12h"] <= mid_th)
                cond = f"田んぼ & 差分_早期-後期>={early_th:.2f} & 差分_6-12h<={mid_th:.2f}"
                if before_th is not None:
                    pred &= paddy_df["before_平均"] <= before_th
                    cond += f" & before平均<={before_th:.1f}"
                yy = paddy_df["正解浸水域"].to_numpy(dtype=bool)
                pp = pred.to_numpy(dtype=bool)
                tp = int(np.sum(pp & yy))
                fp = int(np.sum(pp & ~yy))
                fn = int(np.sum(~pp & yy))
                tn = int(np.sum(~pp & ~yy))
                precision = tp / (tp + fp) if tp + fp else 0.0
                recall = tp / (tp + fn) if tp + fn else 0.0
                specificity = tn / (tn + fp) if tn + fp else 0.0
                f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
                rule_rows.append(
                    {
                        "条件": cond,
                        "early_minus_late閾値": early_th,
                        "6_12h上限": mid_th,
                        "before平均上限": before_th,
                        "TP": tp,
                        "FP": fp,
                        "FN": fn,
                        "TN": tn,
                        "precision": precision,
                        "recall": recall,
                        "specificity": specificity,
                        "balanced_accuracy": (recall + specificity) / 2.0,
                        "F1": f1,
                    }
                )
    rules = pd.DataFrame(rule_rows)
    rules.sort_values(["F1", "balanced_accuracy"], ascending=False).to_csv(
        OUT_DIR / "map7_simple_rule_candidates_paddy_sample.csv", index=False, encoding="utf-8-sig"
    )

    plt = setup_matplotlib()

    # Time series plot for difference, target, and before.
    for prefix, title, y_label, filename in [
        ("差分", "正解浸水域と非浸水域の差分時系列", "target - before (dB)", "図1_差分時系列_正解非正解.png"),
        ("target", "Target強度の時系列", "Target backscatter (dB)", "図2_target強度時系列.png"),
        ("before", "Before強度の時系列", "Before backscatter (dB)", "図3_before強度時系列.png"),
    ]:
        fig, ax = plt.subplots(figsize=(8.2, 4.8), dpi=180)
        for group, color in [("正解浸水域", "#d62728"), ("非浸水域", "#4c78a8"), ("田んぼ内_正解浸水域", "#e377c2"), ("田んぼ内_非浸水域", "#59a14f")]:
            sub = stats[(stats["グループ"] == group) & (stats["特徴量"].isin([f"{prefix}_{x}" for x in ELAPSED]))]
            sub = sub.set_index("特徴量").reindex([f"{prefix}_{x}" for x in ELAPSED])
            xlabels = ELAPSED
            ax.plot(xlabels, sub["中央値"], marker="o", label=group, color=color)
            ax.fill_between(xlabels, sub["p25"], sub["p75"], color=color, alpha=0.12)
        if prefix == "差分":
            ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title(title)
        ax.set_xlabel("降雨開始からの経過時間")
        ax.set_ylabel(y_label)
        ax.grid(True, alpha=0.25)
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(OUT_DIR / filename)
        plt.close(fig)

    top_features = sep.head(10)["特徴量"].tolist()
    fig, axes = plt.subplots(2, 5, figsize=(16, 6.2), dpi=180)
    for ax, feature in zip(axes.ravel(), top_features):
        a = df.loc[df["正解浸水域"], feature].dropna().to_numpy()
        b = df.loc[~df["正解浸水域"], feature].dropna().to_numpy()
        lo, hi = np.nanpercentile(np.concatenate([a, b]), [1, 99])
        bins = np.linspace(lo, hi, 45)
        ax.hist(b, bins=bins, density=True, alpha=0.45, label="非浸水域", color="#4c78a8")
        ax.hist(a, bins=bins, density=True, alpha=0.45, label="正解浸水域", color="#d62728")
        ax.set_title(feature, fontsize=9)
        ax.grid(True, alpha=0.2)
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("分離力が高い特徴量の分布", y=1.02)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図4_分離力上位特徴量ヒストグラム.png", bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.2, 5.5), dpi=180)
    colors = np.where(df["正解浸水域"], "#d62728", "#4c78a8")
    ax.scatter(
        df["差分_早期_minus_後期"],
        df["差分_6-12h"],
        c=colors,
        s=3,
        alpha=0.18,
        linewidths=0,
    )
    ax.axvline(1.0, color="black", linestyle="--", linewidth=0.9)
    ax.axhline(0.0, color="black", linestyle="--", linewidth=0.9)
    ax.set_xlabel("差分 早期平均(0-6h) - 後期平均(6-24h)")
    ax.set_ylabel("差分 6-12h")
    ax.set_title("画素別: 早期低下量と6-12h差分")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図5_早期後期差_vs_6_12h_正解非正解.png")
    plt.close(fig)

    summary = {
        "sample_pixels": int(len(df)),
        "truth_pixels_in_sample": int(df["正解浸水域"].sum()),
        "non_truth_pixels_in_sample": int((~df["正解浸水域"]).sum()),
        "output_dir": str(OUT_DIR),
    }
    (OUT_DIR / "summary.json").write_text(pd.Series(summary).to_json(force_ascii=False, indent=2), encoding="utf-8")
    print(pd.DataFrame([summary]).to_string(index=False))
    print(sep.head(12).to_string(index=False))
    print(rules.sort_values(["F1", "balanced_accuracy"], ascending=False).head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
