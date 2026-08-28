#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Analyze target-before backscatter differences over the full satellite pair scenes."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT


ROOT = Path(__file__).resolve().parents[1]
RAIN_DIR = ROOT / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"
PAIR_DIR = RAIN_DIR / "processed_by_date"
DETECTION_DIR = RAIN_DIR / "kurume_inundation_analysis" / "map7_detection_test"
OUT_DIR = DETECTION_DIR / "full_pair_scene_diff"
MAX_YEAR = 2022

ELAPSED = ["0-3h", "3-6h", "6-12h", "12-24h"]
HIST_BINS = np.linspace(-15.0, 15.0, 121)


@dataclass
class RunningStats:
    n: int = 0
    sum: float = 0.0
    sumsq: float = 0.0
    minv: float = np.inf
    maxv: float = -np.inf
    neg: int = 0
    hist: np.ndarray = field(default_factory=lambda: np.zeros(len(HIST_BINS) - 1, dtype=np.int64))
    sample: list[np.ndarray] = field(default_factory=list)
    sample_limit: int = 300000
    rng: np.random.Generator = field(default_factory=lambda: np.random.default_rng(42))

    def update(self, values: np.ndarray) -> None:
        values = values[np.isfinite(values)]
        if values.size == 0:
            return
        self.n += int(values.size)
        self.sum += float(np.sum(values, dtype=np.float64))
        self.sumsq += float(np.sum(values.astype(np.float64) ** 2))
        self.minv = min(self.minv, float(np.min(values)))
        self.maxv = max(self.maxv, float(np.max(values)))
        self.neg += int(np.sum(values < 0))
        self.hist += np.histogram(values, bins=HIST_BINS)[0]

        # Keep a bounded random sample for quantiles and plots.
        remain = max(0, self.sample_limit - sum(len(x) for x in self.sample))
        if remain > 0:
            take = min(remain, values.size)
            idx = self.rng.choice(values.size, size=take, replace=False)
            self.sample.append(values[idx].astype(np.float32))
        else:
            # Occasional replacement keeps later scenes represented without storing all pixels.
            if self.rng.random() < 0.02:
                take = min(5000, values.size)
                idx = self.rng.choice(values.size, size=take, replace=False)
                current = self.get_sample()
                if current.size > take:
                    replace_idx = self.rng.choice(current.size, size=take, replace=False)
                    current[replace_idx] = values[idx].astype(np.float32)
                    self.sample = [current]

    def get_sample(self) -> np.ndarray:
        if not self.sample:
            return np.array([], dtype=np.float32)
        return np.concatenate(self.sample)

    def row(self) -> dict[str, float | int]:
        sample = self.get_sample()
        out: dict[str, float | int] = {
            "画素数": self.n,
            "平均": self.sum / self.n if self.n else np.nan,
            "標準偏差": np.sqrt(max(self.sumsq / self.n - (self.sum / self.n) ** 2, 0.0)) if self.n else np.nan,
            "最小": self.minv if self.n else np.nan,
            "最大": self.maxv if self.n else np.nan,
            "負の画素割合": self.neg / self.n if self.n else np.nan,
            "サンプル画素数": int(sample.size),
        }
        if sample.size:
            qs = np.percentile(sample, [1, 5, 10, 25, 50, 75, 90, 95, 99])
            out.update(
                {
                    "p01_sample": float(qs[0]),
                    "p05_sample": float(qs[1]),
                    "p10_sample": float(qs[2]),
                    "p25_sample": float(qs[3]),
                    "中央値_sample": float(qs[4]),
                    "p75_sample": float(qs[5]),
                    "p90_sample": float(qs[6]),
                    "p95_sample": float(qs[7]),
                    "p99_sample": float(qs[8]),
                }
            )
        return out


def setup_matplotlib():
    import matplotlib.pyplot as plt

    plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return plt


def elapsed_bin(delay_h: float) -> str | None:
    if 0 <= delay_h < 3:
        return "0-3h"
    if 3 <= delay_h < 6:
        return "3-6h"
    if 6 <= delay_h < 12:
        return "6-12h"
    if 12 <= delay_h <= 24:
        return "12-24h"
    return None


def read_manifest(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    grouped: dict[tuple[str, str], dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            grouped.setdefault((row["rain_day_jst"], row["pair_no"]), {})[row["role"]] = row["organized_path"]
            grouped[(row["rain_day_jst"], row["pair_no"])][f"{row['role']}_stac_id"] = row["stac_id"]
    return grouped


def valid_values(arr: np.ndarray, nodata: float | int | None) -> np.ndarray:
    arr = arr.astype(np.float32, copy=False)
    valid = np.isfinite(arr)
    if nodata is not None:
        valid &= arr != nodata
    # The preprocessed rasters contain exact 0 values outside the valid SAR swath/clip.
    # SAR backscatter in dB can theoretically approach 0, but exact zeros here dominate
    # the full-scene distribution and behave as nodata.
    valid &= arr != 0
    out = arr.copy()
    out[~valid] = np.nan
    return out


def process_pair(target_path: Path, before_path: Path) -> tuple[RunningStats, dict[str, str | int | float]]:
    stats = RunningStats(sample_limit=200000)
    with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
        same_grid = (
            target_src.crs == before_src.crs
            and target_src.transform == before_src.transform
            and target_src.width == before_src.width
            and target_src.height == before_src.height
        )
        meta = {
            "width": int(target_src.width),
            "height": int(target_src.height),
            "target_crs": str(target_src.crs),
            "target_bounds": str(target_src.bounds),
            "same_grid": str(same_grid),
        }
        if same_grid:
            before_reader = before_src
            for _, window in target_src.block_windows(1):
                target = valid_values(target_src.read(1, window=window), target_src.nodata)
                before = valid_values(before_reader.read(1, window=window), before_reader.nodata)
                diff = target - before
                stats.update(diff[np.isfinite(target) & np.isfinite(before)])
        else:
            with WarpedVRT(
                before_src,
                crs=target_src.crs,
                transform=target_src.transform,
                width=target_src.width,
                height=target_src.height,
                resampling=Resampling.bilinear,
                nodata=before_src.nodata,
            ) as vrt:
                for _, window in target_src.block_windows(1):
                    target = valid_values(target_src.read(1, window=window), target_src.nodata)
                    before = valid_values(vrt.read(1, window=window), vrt.nodata)
                    diff = target - before
                    stats.update(diff[np.isfinite(target) & np.isfinite(before)])
    return stats, meta


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plt = setup_matplotlib()

    manifest = read_manifest(PAIR_DIR / "manifest.csv")
    delay_df = pd.read_csv(RAIN_DIR / "map7_db_rain_days_s1_delay_all_jst.csv", encoding="utf-8-sig")
    delay_by_stac = delay_df.set_index("stac_id")["delay_from_rain_start_h"].to_dict()

    pair_rows = []
    hist_rows = []
    sample_rows = []
    elapsed_stats = {label: RunningStats(sample_limit=300000) for label in ELAPSED}
    all_stats = RunningStats(sample_limit=500000)

    for (rain_day, pair_no), roles in sorted(manifest.items()):
        if int(rain_day[:4]) > MAX_YEAR:
            continue
        if "target" not in roles or "pair" not in roles:
            continue
        target_path = Path(roles["target"])
        before_path = Path(roles["pair"])
        if not target_path.exists() or not before_path.exists():
            continue
        delay = float(delay_by_stac.get(roles.get("target_stac_id", ""), np.nan))
        label = elapsed_bin(delay)
        if label is None:
            continue

        stats, meta = process_pair(target_path, before_path)
        row = {
            "rain_day_jst": rain_day,
            "pair_no": pair_no,
            "elapsed_h": delay,
            "elapsed_bin": label,
            "target_stac_id": roles.get("target_stac_id", ""),
            "before_stac_id": roles.get("pair_stac_id", ""),
            "target_path": str(target_path),
            "before_path": str(before_path),
        }
        row.update(meta)
        row.update(stats.row())
        pair_rows.append(row)

        elapsed_stats[label].n += stats.n
        elapsed_stats[label].sum += stats.sum
        elapsed_stats[label].sumsq += stats.sumsq
        elapsed_stats[label].minv = min(elapsed_stats[label].minv, stats.minv)
        elapsed_stats[label].maxv = max(elapsed_stats[label].maxv, stats.maxv)
        elapsed_stats[label].neg += stats.neg
        elapsed_stats[label].hist += stats.hist
        elapsed_stats[label].sample.append(stats.get_sample())

        all_stats.n += stats.n
        all_stats.sum += stats.sum
        all_stats.sumsq += stats.sumsq
        all_stats.minv = min(all_stats.minv, stats.minv)
        all_stats.maxv = max(all_stats.maxv, stats.maxv)
        all_stats.neg += stats.neg
        all_stats.hist += stats.hist
        all_stats.sample.append(stats.get_sample())

        centers = (HIST_BINS[:-1] + HIST_BINS[1:]) / 2.0
        for center, count in zip(centers, stats.hist):
            hist_rows.append(
                {
                    "rain_day_jst": rain_day,
                    "pair_no": pair_no,
                    "elapsed_bin": label,
                    "bin_center": float(center),
                    "count": int(count),
                }
            )
        sample = stats.get_sample()
        if sample.size:
            take = min(50000, sample.size)
            rng = np.random.default_rng(abs(hash((rain_day, pair_no))) % (2**32))
            idx = rng.choice(sample.size, size=take, replace=False)
            for value in sample[idx]:
                sample_rows.append(
                    {
                        "rain_day_jst": rain_day,
                        "pair_no": pair_no,
                        "elapsed_bin": label,
                        "diff_target_minus_before": float(value),
                    }
                )

    pair_df = pd.DataFrame(pair_rows)
    pair_df.to_csv(OUT_DIR / "全衛星ペア_シーン全体_差分統計.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(hist_rows).to_csv(OUT_DIR / "全衛星ペア_シーン全体_差分ヒストグラム.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(sample_rows).to_csv(OUT_DIR / "全衛星ペア_シーン全体_差分サンプル.csv", index=False, encoding="utf-8-sig")

    elapsed_rows = []
    for label in ELAPSED:
        row = {"集計単位": "経過時間帯", "経過時間帯": label}
        row.update(elapsed_stats[label].row())
        elapsed_rows.append(row)
    row = {"集計単位": "全ペア", "経過時間帯": "all"}
    row.update(all_stats.row())
    elapsed_rows.append(row)
    elapsed_df = pd.DataFrame(elapsed_rows)
    elapsed_df.to_csv(OUT_DIR / "全衛星ペア_経過時間帯別_シーン全体差分統計.csv", index=False, encoding="utf-8-sig")

    # Compare against the former limited evaluation window summary if present.
    limited_path = DETECTION_DIR / "balanced_inundated_noninundated_diff" / "同数画素_経過時間別_差分統計.csv"
    if limited_path.exists():
        limited = pd.read_csv(limited_path, encoding="utf-8-sig")
        limited = limited[limited["領域"].eq("非浸水域_同数抽出")][["経過時間帯", "平均", "中央値"]].rename(
            columns={"平均": "旧_非浸水域同数抽出_平均", "中央値": "旧_非浸水域同数抽出_中央値"}
        )
        comp = elapsed_df[elapsed_df["集計単位"].eq("経過時間帯")][
            ["経過時間帯", "平均", "中央値_sample", "画素数"]
        ].rename(columns={"平均": "全シーン_平均", "中央値_sample": "全シーン_中央値_sample", "画素数": "全シーン_画素数"})
        comp = comp.merge(limited, on="経過時間帯", how="left")
        comp.to_csv(OUT_DIR / "旧非浸水域定義_vs_全シーン差分比較.csv", index=False, encoding="utf-8-sig")

    # Plots.
    fig, ax = plt.subplots(figsize=(8.2, 5.0), dpi=180)
    plot_df = elapsed_df[elapsed_df["集計単位"].eq("経過時間帯")].set_index("経過時間帯").reindex(ELAPSED)
    ax.plot(ELAPSED, plot_df["平均"], marker="o", label="平均", color="#4c78a8")
    ax.plot(ELAPSED, plot_df["中央値_sample"], marker="s", linestyle="--", label="中央値(sample)", color="#f58518")
    ax.fill_between(ELAPSED, plot_df["p25_sample"], plot_df["p75_sample"], color="#4c78a8", alpha=0.14, label="p25-p75(sample)")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("衛星データペア全体の後方散乱強度差分")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before (dB)")
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図1_全衛星ペア_経過時間帯別_差分時系列.png")
    plt.close(fig)

    fig, axes = plt.subplots(2, 2, figsize=(11.0, 7.2), dpi=180, sharey=True)
    for ax, label in zip(axes.ravel(), ELAPSED):
        s = elapsed_stats[label].get_sample()
        s = s[np.isfinite(s)]
        if s.size:
            lo, hi = np.percentile(s, [1, 99])
            bins = np.linspace(lo, hi, 70)
            ax.hist(s, bins=bins, density=True, color="#4c78a8", alpha=0.78)
            ax.axvline(np.median(s), color="black", linewidth=1.1, label="中央値")
            ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
        ax.set_title(label)
        ax.grid(True, axis="y", alpha=0.25)
    fig.suptitle("衛星データペア全体: 経過時間帯別の差分分布(sample)")
    fig.supxlabel("差分 target - before (dB)")
    fig.supylabel("密度")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図2_全衛星ペア_経過時間帯別_差分ヒストグラム.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(9.8, 5.8), dpi=180)
    pair_plot = pair_df.sort_values(["elapsed_bin", "elapsed_h", "rain_day_jst"])
    ax.scatter(pair_plot["elapsed_h"], pair_plot["平均"], s=48, color="#4c78a8", alpha=0.82)
    for _, r in pair_plot.iterrows():
        ax.annotate(f"{r['rain_day_jst']}\n{r['pair_no']}", (r["elapsed_h"], r["平均"]), fontsize=6, alpha=0.75)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("衛星ペアごとのシーン全体平均差分")
    ax.set_xlabel("降雨開始からの経過時間 (h)")
    ax.set_ylabel("平均差分 target - before (dB)")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図3_全衛星ペア_ペア別_平均差分.png")
    plt.close(fig)

    print(elapsed_df.to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
