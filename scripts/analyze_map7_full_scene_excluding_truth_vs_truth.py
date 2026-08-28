#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Compare full satellite pair scenes excluding truth inundation pixels against truth pixels."""

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
KURUME_DIR = Path(r"D:\sotsuron\kurume")
OUT_DIR = DETECTION_DIR / "full_scene_without_truth_vs_truth"

ELAPSED = ["0-3h", "3-6h", "6-12h", "12-24h"]
HIST_BINS = np.linspace(-15.0, 15.0, 121)
MAX_YEAR = 2022
MASK_MIN = 0.5
MASK_MAX = 1.7


@dataclass
class RunningStats:
    n: int = 0
    sum: float = 0.0
    sumsq: float = 0.0
    neg: int = 0
    minv: float = np.inf
    maxv: float = -np.inf
    hist: np.ndarray = field(default_factory=lambda: np.zeros(len(HIST_BINS) - 1, dtype=np.int64))
    sample: list[np.ndarray] = field(default_factory=list)
    sample_limit: int = 300000
    rng: np.random.Generator = field(default_factory=lambda: np.random.default_rng(42))

    def update(self, values: np.ndarray) -> None:
        values = values[np.isfinite(values)]
        if values.size == 0:
            return
        self.n += int(values.size)
        v64 = values.astype(np.float64, copy=False)
        self.sum += float(np.sum(v64))
        self.sumsq += float(np.sum(v64 * v64))
        self.neg += int(np.sum(values < 0))
        self.minv = min(self.minv, float(np.min(values)))
        self.maxv = max(self.maxv, float(np.max(values)))
        self.hist += np.histogram(values, bins=HIST_BINS)[0]

        current_n = sum(len(x) for x in self.sample)
        remain = max(0, self.sample_limit - current_n)
        if remain > 0:
            take = min(remain, values.size)
            idx = self.rng.choice(values.size, size=take, replace=False)
            self.sample.append(values[idx].astype(np.float32))
        elif self.rng.random() < 0.02:
            take = min(5000, values.size)
            idx = self.rng.choice(values.size, size=take, replace=False)
            current = self.get_sample()
            if current.size > take:
                ridx = self.rng.choice(current.size, size=take, replace=False)
                current[ridx] = values[idx].astype(np.float32)
                self.sample = [current]

    def merge(self, other: "RunningStats") -> None:
        self.n += other.n
        self.sum += other.sum
        self.sumsq += other.sumsq
        self.neg += other.neg
        self.minv = min(self.minv, other.minv)
        self.maxv = max(self.maxv, other.maxv)
        self.hist += other.hist
        s = other.get_sample()
        if s.size:
            current_n = sum(len(x) for x in self.sample)
            remain = max(0, self.sample_limit - current_n)
            if remain > 0:
                take = min(remain, s.size)
                idx = self.rng.choice(s.size, size=take, replace=False)
                self.sample.append(s[idx].astype(np.float32))

    def get_sample(self) -> np.ndarray:
        if not self.sample:
            return np.array([], dtype=np.float32)
        return np.concatenate(self.sample)

    def row(self) -> dict[str, float | int]:
        sample = self.get_sample()
        row: dict[str, float | int] = {
            "画素数": self.n,
            "平均": self.sum / self.n if self.n else np.nan,
            "標準偏差": np.sqrt(max(self.sumsq / self.n - (self.sum / self.n) ** 2, 0.0)) if self.n else np.nan,
            "最小": self.minv if self.n else np.nan,
            "最大": self.maxv if self.n else np.nan,
            "負の画素割合": self.neg / self.n if self.n else np.nan,
            "サンプル画素数": int(sample.size),
        }
        if sample.size:
            q = np.percentile(sample, [1, 5, 10, 25, 50, 75, 90, 95, 99])
            row.update(
                {
                    "p01_sample": float(q[0]),
                    "p05_sample": float(q[1]),
                    "p10_sample": float(q[2]),
                    "p25_sample": float(q[3]),
                    "中央値_sample": float(q[4]),
                    "p75_sample": float(q[5]),
                    "p90_sample": float(q[6]),
                    "p95_sample": float(q[7]),
                    "p99_sample": float(q[8]),
                }
            )
        return row


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
            key = (row["rain_day_jst"], row["pair_no"])
            grouped.setdefault(key, {})[row["role"]] = row["organized_path"]
            grouped[key][f"{row['role']}_stac_id"] = row["stac_id"]
    return grouped


def valid_values(arr: np.ndarray, nodata) -> np.ndarray:
    arr = arr.astype(np.float32, copy=False)
    valid = np.isfinite(arr)
    if nodata is not None:
        valid &= arr != nodata
    valid &= arr != 0
    out = arr.copy()
    out[~valid] = np.nan
    return out


def truth_union_on_grid(mask_paths: list[Path], template) -> np.ndarray:
    union = np.zeros((template.height, template.width), dtype=bool)
    for path in mask_paths:
        with rasterio.open(path) as src:
            with WarpedVRT(
                src,
                crs=template.crs,
                transform=template.transform,
                width=template.width,
                height=template.height,
                resampling=Resampling.nearest,
                nodata=src.nodata,
            ) as vrt:
                arr = vrt.read(1).astype(np.float32)
                valid = np.isfinite(arr)
                if vrt.nodata is not None:
                    valid &= arr != vrt.nodata
                union |= valid & (arr >= MASK_MIN) & (arr <= MASK_MAX)
    return union


def process_pair(target_path: Path, before_path: Path, mask_paths: list[Path]) -> tuple[dict[str, RunningStats], dict[str, int | str]]:
    zone_stats = {
        "正解浸水域": RunningStats(sample_limit=200000),
        "シーン全体_正解浸水域除外": RunningStats(sample_limit=300000),
    }
    with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
        truth = truth_union_on_grid(mask_paths, target_src)
        same_grid = (
            target_src.crs == before_src.crs
            and target_src.transform == before_src.transform
            and target_src.width == before_src.width
            and target_src.height == before_src.height
        )
        meta = {
            "width": int(target_src.width),
            "height": int(target_src.height),
            "same_grid": str(same_grid),
            "truth_union_pixels_on_scene": int(np.sum(truth)),
            "target_bounds": str(target_src.bounds),
        }

        if same_grid:
            for _, window in target_src.block_windows(1):
                target = valid_values(target_src.read(1, window=window), target_src.nodata)
                before = valid_values(before_src.read(1, window=window), before_src.nodata)
                truth_block = truth[
                    int(window.row_off) : int(window.row_off + window.height),
                    int(window.col_off) : int(window.col_off + window.width),
                ]
                valid = np.isfinite(target) & np.isfinite(before)
                diff = target - before
                zone_stats["正解浸水域"].update(diff[valid & truth_block])
                zone_stats["シーン全体_正解浸水域除外"].update(diff[valid & ~truth_block])
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
                    truth_block = truth[
                        int(window.row_off) : int(window.row_off + window.height),
                        int(window.col_off) : int(window.col_off + window.width),
                    ]
                    valid = np.isfinite(target) & np.isfinite(before)
                    diff = target - before
                    zone_stats["正解浸水域"].update(diff[valid & truth_block])
                    zone_stats["シーン全体_正解浸水域除外"].update(diff[valid & ~truth_block])
    return zone_stats, meta


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plt = setup_matplotlib()

    mask_paths = sorted(KURUME_DIR.glob("*.tif"))
    if not mask_paths:
        raise RuntimeError(f"正解浸水域TIFが見つかりません: {KURUME_DIR}")

    manifest = read_manifest(PAIR_DIR / "manifest.csv")
    delay_df = pd.read_csv(RAIN_DIR / "map7_db_rain_days_s1_delay_all_jst.csv", encoding="utf-8-sig")
    delay_by_stac = delay_df.set_index("stac_id")["delay_from_rain_start_h"].to_dict()

    aggregate = {
        zone: {label: RunningStats(sample_limit=400000) for label in ELAPSED}
        for zone in ["正解浸水域", "シーン全体_正解浸水域除外"]
    }
    pair_rows = []
    sample_rows = []
    hist_rows = []

    for (rain_day, pair_no), roles in sorted(manifest.items()):
        if int(rain_day[:4]) > MAX_YEAR:
            continue
        if "target" not in roles or "pair" not in roles:
            continue
        delay = float(delay_by_stac.get(roles.get("target_stac_id", ""), np.nan))
        label = elapsed_bin(delay)
        if label is None:
            continue
        target_path = Path(roles["target"])
        before_path = Path(roles["pair"])
        if not target_path.exists() or not before_path.exists():
            continue

        stats_by_zone, meta = process_pair(target_path, before_path, mask_paths)
        for zone, stats in stats_by_zone.items():
            row = {
                "rain_day_jst": rain_day,
                "pair_no": pair_no,
                "elapsed_h": delay,
                "elapsed_bin": label,
                "領域": zone,
            }
            row.update(meta)
            row.update(stats.row())
            pair_rows.append(row)
            aggregate[zone][label].merge(stats)

            centers = (HIST_BINS[:-1] + HIST_BINS[1:]) / 2.0
            for center, count in zip(centers, stats.hist):
                hist_rows.append(
                    {
                        "rain_day_jst": rain_day,
                        "pair_no": pair_no,
                        "elapsed_bin": label,
                        "領域": zone,
                        "bin_center": float(center),
                        "count": int(count),
                    }
                )
            sample = stats.get_sample()
            if sample.size:
                rng = np.random.default_rng(abs(hash((rain_day, pair_no, zone))) % (2**32))
                if zone == "正解浸水域":
                    selected = sample
                else:
                    take = min(30000, sample.size)
                    idx = rng.choice(sample.size, size=take, replace=False)
                    selected = sample[idx]
                for value in selected:
                    sample_rows.append(
                        {
                            "rain_day_jst": rain_day,
                            "pair_no": pair_no,
                            "elapsed_bin": label,
                            "領域": zone,
                            "diff_target_minus_before": float(value),
                        }
                    )

    pair_df = pd.DataFrame(pair_rows)
    pair_df.to_csv(OUT_DIR / "ペア別_正解浸水域_vs_シーン全体除外_差分統計.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(hist_rows).to_csv(OUT_DIR / "ペア別_正解浸水域_vs_シーン全体除外_ヒストグラム.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(sample_rows).to_csv(OUT_DIR / "ペア別_正解浸水域_vs_シーン全体除外_差分サンプル.csv", index=False, encoding="utf-8-sig")

    rows = []
    for zone in ["正解浸水域", "シーン全体_正解浸水域除外"]:
        for label in ELAPSED:
            row = {"領域": zone, "経過時間帯": label}
            row.update(aggregate[zone][label].row())
            rows.append(row)
    summary = pd.DataFrame(rows)
    summary.to_csv(OUT_DIR / "経過時間帯別_正解浸水域_vs_シーン全体除外_差分統計.csv", index=False, encoding="utf-8-sig")

    comp_rows = []
    for label in ELAPSED:
        truth = aggregate["正解浸水域"][label]
        scene = aggregate["シーン全体_正解浸水域除外"][label]
        trow = truth.row()
        srow = scene.row()
        comp_rows.append(
            {
                "経過時間帯": label,
                "正解浸水域_画素数": trow["画素数"],
                "背景_画素数": srow["画素数"],
                "平均差_正解minus背景": trow["平均"] - srow["平均"],
                "中央値差_正解minus背景_sample": trow.get("中央値_sample", np.nan) - srow.get("中央値_sample", np.nan),
                "負の画素割合差_正解minus背景": trow["負の画素割合"] - srow["負の画素割合"],
                "正解_平均": trow["平均"],
                "背景_平均": srow["平均"],
                "正解_中央値_sample": trow.get("中央値_sample", np.nan),
                "背景_中央値_sample": srow.get("中央値_sample", np.nan),
            }
        )
    comp = pd.DataFrame(comp_rows)
    comp.to_csv(OUT_DIR / "経過時間帯別_正解浸水域_minus_シーン全体除外_比較.csv", index=False, encoding="utf-8-sig")

    colors = {"正解浸水域": "#d62728", "シーン全体_正解浸水域除外": "#4c78a8"}
    fig, ax = plt.subplots(figsize=(8.6, 5.0), dpi=180)
    for zone in ["正解浸水域", "シーン全体_正解浸水域除外"]:
        sub = summary[summary["領域"].eq(zone)].set_index("経過時間帯").reindex(ELAPSED)
        ax.plot(ELAPSED, sub["平均"], marker="o", color=colors[zone], label=f"{zone} 平均")
        ax.plot(ELAPSED, sub["中央値_sample"], marker="s", linestyle="--", color=colors[zone], alpha=0.75, label=f"{zone} 中央値")
        ax.fill_between(ELAPSED, sub["p25_sample"], sub["p75_sample"], color=colors[zone], alpha=0.12)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("正解浸水域とシーン全体背景の差分時系列")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("差分 target - before (dB)")
    ax.grid(True, alpha=0.25)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図1_正解浸水域_vs_シーン全体除外_差分時系列.png")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8.2, 4.8), dpi=180)
    ax.bar(comp["経過時間帯"], comp["平均差_正解minus背景"], color="#d62728", alpha=0.75, label="平均差")
    ax.plot(comp["経過時間帯"], comp["中央値差_正解minus背景_sample"], color="#333333", marker="o", label="中央値差")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("正解浸水域と背景の差分差")
    ax.set_xlabel("降雨開始からの経過時間")
    ax.set_ylabel("正解浸水域 - 背景 (dB)")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図2_正解浸水域_minus_背景_差分差.png")
    plt.close(fig)

    fig, axes = plt.subplots(2, 2, figsize=(11.0, 7.4), dpi=180, sharey=True)
    for ax, label in zip(axes.ravel(), ELAPSED):
        for zone in ["シーン全体_正解浸水域除外", "正解浸水域"]:
            s = aggregate[zone][label].get_sample()
            s = s[np.isfinite(s)]
            if s.size == 0:
                continue
            lo, hi = np.percentile(s, [1, 99])
            bins = np.linspace(lo, hi, 55)
            ax.hist(s, bins=bins, density=True, color=colors[zone], alpha=0.42, label=zone)
        ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
        ax.set_title(label)
        ax.grid(True, axis="y", alpha=0.25)
    handles, labels = axes.ravel()[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("正解浸水域と背景の経過時間帯別差分分布")
    fig.supxlabel("差分 target - before (dB)")
    fig.supylabel("密度")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "図3_正解浸水域_vs_背景_差分ヒストグラム.png", bbox_inches="tight")
    plt.close(fig)

    print(summary.to_string(index=False))
    print(comp.to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
