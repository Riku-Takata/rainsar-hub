#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Denoised 10,000-pixel comparison for truth/background backscatter differences."""

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
SAMPLE_CSV = BASE_DIR / "ペア別_正解浸水域_vs_シーン全体除外_差分サンプル.csv"
OUT_DIR = BASE_DIR / "balanced_10000_pair_stratified_denoised"

ELAPSED = ["0-3h", "3-6h", "6-12h", "12-24h"]
ZONES = ["正解浸水域", "シーン全体_正解浸水域除外"]
N_PER_ZONE_BIN = 10000
RANDOM_SEED = 42
ABS_DIFF_LIMIT_DB = 15.0
MAD_K = 4.5


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


def denoise_group(group: pd.DataFrame) -> pd.DataFrame:
    vals = group["diff_target_minus_before"].to_numpy(dtype=np.float64)
    finite = np.isfinite(vals)
    abs_ok = np.abs(vals) <= ABS_DIFF_LIMIT_DB

    if np.sum(finite & abs_ok) < 20:
        keep = finite & abs_ok
    else:
        base = vals[finite & abs_ok]
        med = np.median(base)
        mad = np.median(np.abs(base - med))
        if mad > 1.0e-6:
            robust_sigma = 1.4826 * mad
            lo = med - MAD_K * robust_sigma
            hi = med + MAD_K * robust_sigma
        else:
            q1, q3 = np.percentile(base, [25, 75])
            iqr = q3 - q1
            lo = q1 - 3.0 * iqr
            hi = q3 + 3.0 * iqr
        keep = finite & abs_ok & (vals >= lo) & (vals <= hi)

    out = group.loc[keep].copy()
    out["ノイズ除去前画素数"] = len(group)
    out["ノイズ除去後画素数"] = len(out)
    out["除去画素数"] = len(group) - len(out)
    return out


def pair_stratified_sample(group: pd.DataFrame, n: int, rng: np.random.Generator) -> pd.DataFrame:
    if len(group) <= n:
        return group.copy()
    keys = list(group.groupby(["rain_day_jst", "pair_no"]).groups.keys())
    base = n // len(keys)
    extra = n % len(keys)
    shuffled = keys.copy()
    rng.shuffle(shuffled)
    quota = {key: base + (1 if i < extra else 0) for i, key in enumerate(shuffled)}
    selected_parts = []
    remaining_parts = []
    for key, sub in group.groupby(["rain_day_jst", "pair_no"], sort=False):
        take = min(quota[key], len(sub))
        picked = sub.sample(n=take, random_state=int(rng.integers(0, 2**32 - 1))) if take else sub.iloc[0:0]
        selected_parts.append(picked)
        if len(sub) > take:
            remaining_parts.append(sub.drop(picked.index))
    selected = pd.concat(selected_parts, ignore_index=False)
    deficit = n - len(selected)
    if deficit > 0 and remaining_parts:
        remaining = pd.concat(remaining_parts, ignore_index=False)
        selected = pd.concat(
            [selected, remaining.sample(n=min(deficit, len(remaining)), random_state=int(rng.integers(0, 2**32 - 1)))],
            ignore_index=False,
        )
    if len(selected) > n:
        selected = selected.sample(n=n, random_state=int(rng.integers(0, 2**32 - 1)))
    return selected.copy()


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(RANDOM_SEED)
    plt = setup_matplotlib()

    df = pd.read_csv(SAMPLE_CSV, encoding="utf-8-sig")
    df = df[np.isfinite(df["diff_target_minus_before"])].copy()

    denoised = (
        df.groupby(["elapsed_bin", "領域", "rain_day_jst", "pair_no"], group_keys=False)
        .apply(denoise_group)
        .reset_index()
    )
    denoised.to_csv(OUT_DIR / "ノイズ除去後_差分サンプル.csv", index=False, encoding="utf-8-sig")

    noise_summary = (
        denoised.groupby(["elapsed_bin", "領域", "rain_day_jst", "pair_no"], dropna=False)
        .agg(
            ノイズ除去前画素数=("ノイズ除去前画素数", "max"),
            ノイズ除去後画素数=("ノイズ除去後画素数", "max"),
            除去画素数=("除去画素数", "max"),
        )
        .reset_index()
    )
    noise_summary["除去率"] = noise_summary["除去画素数"] / noise_summary["ノイズ除去前画素数"]
    noise_summary.to_csv(OUT_DIR / "ノイズ除去_summary.csv", index=False, encoding="utf-8-sig")

    rows = []
    availability_rows = []
    for elapsed in ELAPSED:
        for zone in ZONES:
            sub = denoised[(denoised["elapsed_bin"] == elapsed) & (denoised["領域"] == zone)].copy()
            pair_counts = sub.groupby(["rain_day_jst", "pair_no"], dropna=False).size().reset_index(name="ノイズ除去後候補画素数")
            sampled = pair_stratified_sample(sub, N_PER_ZONE_BIN, rng)
            sampled["比較領域"] = "背景_正解浸水域除外" if zone == "シーン全体_正解浸水域除外" else zone
            rows.append(sampled)
            sampled_counts = sampled.groupby(["rain_day_jst", "pair_no"], dropna=False).size().reset_index(name="採用画素数")
            merged = pair_counts.merge(sampled_counts, on=["rain_day_jst", "pair_no"], how="left").fillna({"採用画素数": 0})
            merged["経過時間帯"] = elapsed
            merged["領域"] = "背景_正解浸水域除外" if zone == "シーン全体_正解浸水域除外" else zone
            merged["目標画素数"] = N_PER_ZONE_BIN
            availability_rows.append(merged)

    balanced = pd.concat(rows, ignore_index=True)
    balanced = balanced.rename(columns={"elapsed_bin": "経過時間帯", "diff_target_minus_before": "差分_target_minus_before"})
    balanced = balanced[["rain_day_jst", "pair_no", "経過時間帯", "比較領域", "差分_target_minus_before"]]
    balanced.to_csv(OUT_DIR / "同数10000_ノイズ除去後_ペア均等抽出_差分値.csv", index=False, encoding="utf-8-sig")
    pd.concat(availability_rows, ignore_index=True).to_csv(
        OUT_DIR / "同数10000_ノイズ除去後_ペア別_候補数と採用数.csv", index=False, encoding="utf-8-sig"
    )

    stats_rows = []
    comp_rows = []
    for elapsed in ELAPSED:
        truth = balanced[(balanced["経過時間帯"] == elapsed) & (balanced["比較領域"] == "正解浸水域")][
            "差分_target_minus_before"
        ].to_numpy(dtype=np.float32)
        bg = balanced[(balanced["経過時間帯"] == elapsed) & (balanced["比較領域"] == "背景_正解浸水域除外")][
            "差分_target_minus_before"
        ].to_numpy(dtype=np.float32)
        for zone, values in [("正解浸水域", truth), ("背景_正解浸水域除外", bg)]:
            row = {"経過時間帯": elapsed, "領域": zone}
            row.update(summarize(values))
            stats_rows.append(row)
        comp_rows.append(
            {
                "経過時間帯": elapsed,
                "比較画素数_each": int(min(truth.size, bg.size)),
                "正解平均": float(np.mean(truth)),
                "背景平均": float(np.mean(bg)),
                "平均差_正解minus背景": float(np.mean(truth) - np.mean(bg)),
                "正解中央値": float(np.median(truth)),
                "背景中央値": float(np.median(bg)),
                "中央値差_正解minus背景": float(np.median(truth) - np.median(bg)),
                "正解_負の画素割合": float(np.mean(truth < 0)),
                "背景_負の画素割合": float(np.mean(bg < 0)),
                "負の画素割合差_正解minus背景": float(np.mean(truth < 0) - np.mean(bg < 0)),
                "Cohen_d": cohens_d(truth, bg),
            }
        )

    stats = pd.DataFrame(stats_rows)
    comp = pd.DataFrame(comp_rows)
    stats.to_csv(OUT_DIR / "同数10000_ノイズ除去後_差分統計.csv", index=False, encoding="utf-8-sig")
    comp.to_csv(OUT_DIR / "同数10000_ノイズ除去後_正解minus背景_比較.csv", index=False, encoding="utf-8-sig")

    colors = {"正解浸水域": "#d62728", "背景_正解浸水域除外": "#4c78a8"}
    fig, ax = plt.subplots(figsize=(8.8, 5.0), dpi=180)
    for zone in ["正解浸水域", "背景_正解浸水域除外"]:
        sub = stats[stats["領域"] == zone].set_index("経過時間帯").reindex(ELAPSED)
        ax.plot(ELAPSED, sub["平均"], marker="o", color=colors[zone], label=f"{zone} 平均")
        ax.plot(ELAPSED, sub["中央値"], marker="s", linestyle="--", color=colors[zone], alpha=0.75, label=f"{zone} 中央値")
        ax.fill_between(ELAPSED, sub["p25"], sub["p75"], color=colors[zone], alpha=0.12)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("ノイズ除去後 各経過時間帯10,000画素: 差分時系列")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before (dB)")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図1_ノイズ除去後_同数10000_差分時系列.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.2, 4.8), dpi=180)
    ax.bar(comp["経過時間帯"], comp["平均差_正解minus背景"], color="#d62728", alpha=0.72, label="平均差")
    ax.plot(comp["経過時間帯"], comp["中央値差_正解minus背景"], color="#333333", marker="o", label="中央値差")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("ノイズ除去後: 正解浸水域 - 背景")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分差 (dB)")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図2_ノイズ除去後_同数10000_正解minus背景.png")
    plt.close(fig)

    fig, axes = plt.subplots(2, 2, figsize=(11.0, 7.4), dpi=180, sharey=True)
    for ax, elapsed in zip(axes.ravel(), ELAPSED):
        sub = balanced[balanced["経過時間帯"] == elapsed]
        truth = sub[sub["比較領域"] == "正解浸水域"]["差分_target_minus_before"].to_numpy()
        bg = sub[sub["比較領域"] == "背景_正解浸水域除外"]["差分_target_minus_before"].to_numpy()
        both = np.concatenate([truth, bg])
        lo, hi = np.percentile(both, [1, 99])
        bins = np.linspace(lo, hi, 55)
        ax.hist(bg, bins=bins, density=True, color="#4c78a8", alpha=0.45, label="背景")
        ax.hist(truth, bins=bins, density=True, color="#d62728", alpha=0.45, label="正解浸水域")
        ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
        ax.set_title(elapsed)
        ax.grid(True, axis="y", alpha=0.25)
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("ノイズ除去後 各経過時間帯10,000画素: 差分分布")
    fig.supxlabel("差分 target - before (dB)")
    fig.supylabel("密度")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図3_ノイズ除去後_同数10000_差分ヒストグラム.png", bbox_inches="tight")
    plt.close(fig)

    print(noise_summary.groupby(["elapsed_bin", "領域"])[["ノイズ除去前画素数", "ノイズ除去後画素数", "除去画素数"]].sum().to_string())
    print(comp.to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
