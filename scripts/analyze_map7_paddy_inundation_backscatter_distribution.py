#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Analyze backscatter distributions for inundated paddy pixels."""

from __future__ import annotations

from dataclasses import dataclass, field
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
LABELED_DIR = DETECTION_DIR / "pixel_backscatter_labeled_rasters"
MANIFEST_CSV = LABELED_DIR / "pixel_backscatter_diff_label_manifest.csv"
PADDY_MASK = DETECTION_DIR / "landmask_filter" / "map7_paddy_mask.tif"
OUT_DIR = DETECTION_DIR / "paddy_inundation_backscatter_distribution"

FEATURES = {
    "before_backscatter_db": 1,
    "target_backscatter_db": 2,
    "diff_target_minus_before_db": 3,
}
ZONES = ["田んぼ内_正解浸水域", "田んぼ内_非浸水域"]
ELAPSED = ["0-3h", "3-6h", "6-12h", "12-24h"]


@dataclass
class RunningStats:
    n: int = 0
    sum: float = 0.0
    sumsq: float = 0.0
    minv: float = np.inf
    maxv: float = -np.inf
    neg: int = 0
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
        self.minv = min(self.minv, float(np.min(values)))
        self.maxv = max(self.maxv, float(np.max(values)))
        self.neg += int(np.sum(values < 0))

        current = sum(len(x) for x in self.sample)
        remain = max(0, self.sample_limit - current)
        if remain > 0:
            take = min(remain, values.size)
            idx = self.rng.choice(values.size, size=take, replace=False)
            self.sample.append(values[idx].astype(np.float32))

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
            "負の値割合": self.neg / self.n if self.n else np.nan,
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


def update_zone_stats(stats, elapsed: str, zone: str, feature: str, values: np.ndarray) -> None:
    stats.setdefault(elapsed, {}).setdefault(zone, {}).setdefault(feature, RunningStats()).update(values)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plt = setup_matplotlib()

    manifest = pd.read_csv(MANIFEST_CSV, encoding="utf-8-sig")
    manifest = manifest[manifest["status"].eq("exported") & (manifest["valid_pixel_count"] > 0)].copy()

    stats: dict[str, dict[str, dict[str, RunningStats]]] = {}
    pair_rows = []

    with rasterio.open(PADDY_MASK) as paddy_src:
        for _, row in manifest.iterrows():
            tif_path = Path(row["output_tif"])
            if not tif_path.exists():
                continue
            elapsed = str(row["elapsed_bin"])
            with rasterio.open(tif_path) as src:
                with WarpedVRT(
                    paddy_src,
                    crs=src.crs,
                    transform=src.transform,
                    width=src.width,
                    height=src.height,
                    resampling=Resampling.nearest,
                    nodata=0,
                ) as paddy_vrt:
                    pair_counts = {zone: 0 for zone in ZONES}
                    pair_feature_sum = {
                        zone: {feature: 0.0 for feature in FEATURES}
                        for zone in ZONES
                    }
                    for _, window in src.block_windows(1):
                        before = src.read(1, window=window).astype(np.float32)
                        target = src.read(2, window=window).astype(np.float32)
                        diff = src.read(3, window=window).astype(np.float32)
                        label = src.read(4, window=window).astype(np.float32)
                        paddy = paddy_vrt.read(1, window=window) > 0

                        valid = np.isfinite(before) & np.isfinite(target) & np.isfinite(diff) & np.isfinite(label) & paddy
                        if not np.any(valid):
                            continue
                        inundated = valid & (label == 1)
                        non_inundated = valid & (label == 0)
                        zone_masks = {
                            "田んぼ内_正解浸水域": inundated,
                            "田んぼ内_非浸水域": non_inundated,
                        }
                        feature_arrays = {
                            "before_backscatter_db": before,
                            "target_backscatter_db": target,
                            "diff_target_minus_before_db": diff,
                        }
                        for zone, mask in zone_masks.items():
                            if not np.any(mask):
                                continue
                            count = int(np.sum(mask))
                            pair_counts[zone] += count
                            for feature, arr in feature_arrays.items():
                                vals = arr[mask]
                                update_zone_stats(stats, elapsed, zone, feature, vals)
                                pair_feature_sum[zone][feature] += float(np.nansum(vals.astype(np.float64)))

                    for zone in ZONES:
                        out = {
                            "rain_day_jst": row["rain_day_jst"],
                            "pair_no": row["pair_no"],
                            "elapsed_h": row["elapsed_h"],
                            "elapsed_bin": elapsed,
                            "領域": zone,
                            "画素数": pair_counts[zone],
                            "output_tif": str(tif_path),
                        }
                        for feature in FEATURES:
                            out[f"{feature}_平均"] = (
                                pair_feature_sum[zone][feature] / pair_counts[zone]
                                if pair_counts[zone]
                                else np.nan
                            )
                        pair_rows.append(out)

    summary_rows = []
    sample_rows = []
    for elapsed in ELAPSED:
        for zone in ZONES:
            for feature in FEATURES:
                stat = stats.get(elapsed, {}).get(zone, {}).get(feature, RunningStats())
                out = {"経過時間帯": elapsed, "領域": zone, "特徴量": feature}
                out.update(stat.row())
                summary_rows.append(out)
                sample = stat.get_sample()
                if sample.size:
                    rng = np.random.default_rng(abs(hash((elapsed, zone, feature))) % (2**32))
                    take = min(100000, sample.size)
                    idx = rng.choice(sample.size, size=take, replace=False)
                    for value in sample[idx]:
                        sample_rows.append(
                            {
                                "経過時間帯": elapsed,
                                "領域": zone,
                                "特徴量": feature,
                                "値": float(value),
                            }
                        )

    summary_df = pd.DataFrame(summary_rows)
    pair_df = pd.DataFrame(pair_rows)
    sample_df = pd.DataFrame(sample_rows)
    summary_df.to_csv(OUT_DIR / "田んぼ_浸水域非浸水域_後方散乱強度差分_統計.csv", index=False, encoding="utf-8-sig")
    pair_df.to_csv(OUT_DIR / "田んぼ_ペア別_後方散乱強度差分_統計.csv", index=False, encoding="utf-8-sig")
    sample_df.to_csv(OUT_DIR / "田んぼ_後方散乱強度差分_分布サンプル.csv", index=False, encoding="utf-8-sig")

    colors = {"田んぼ内_正解浸水域": "#d62728", "田んぼ内_非浸水域": "#4c78a8"}
    feature_labels = {
        "before_backscatter_db": "Before後方散乱強度 (dB)",
        "target_backscatter_db": "Target後方散乱強度 (dB)",
        "diff_target_minus_before_db": "差分 Target - Before (dB)",
    }

    for feature in FEATURES:
        fig, ax = plt.subplots(figsize=(8.8, 5.0), dpi=180)
        for zone in ZONES:
            sub = summary_df[(summary_df["特徴量"] == feature) & (summary_df["領域"] == zone)]
            sub = sub.set_index("経過時間帯").reindex(ELAPSED)
            ax.plot(ELAPSED, sub["平均"], marker="o", color=colors[zone], label=f"{zone} 平均")
            ax.plot(ELAPSED, sub["中央値_sample"], marker="s", linestyle="--", color=colors[zone], alpha=0.75, label=f"{zone} 中央値")
            ax.fill_between(ELAPSED, sub["p25_sample"], sub["p75_sample"], color=colors[zone], alpha=0.12)
        if "diff" in feature:
            ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title(f"田んぼ領域: {feature_labels[feature]} の経過時間別推移")
        ax.set_xlabel("降雨開始からの経過時間")
        ax.set_ylabel(feature_labels[feature])
        ax.grid(True, alpha=0.25)
        ax.legend(fontsize=8)
        fig.tight_layout()
        safe = feature.replace("_db", "")
        fig.savefig(OUT_DIR / f"図_{safe}_経過時間別推移.png")
        plt.close(fig)

    for feature in FEATURES:
        fig, axes = plt.subplots(2, 2, figsize=(11.0, 7.4), dpi=180, sharey=True)
        for ax, elapsed in zip(axes.ravel(), ELAPSED):
            sub = sample_df[(sample_df["特徴量"] == feature) & (sample_df["経過時間帯"] == elapsed)]
            vals_all = sub["値"].to_numpy()
            if vals_all.size == 0:
                ax.set_title(elapsed)
                continue
            lo, hi = np.nanpercentile(vals_all, [1, 99])
            if not np.isfinite(lo) or not np.isfinite(hi) or lo == hi:
                lo, hi = float(np.nanmin(vals_all)), float(np.nanmax(vals_all))
            bins = np.linspace(lo, hi, 55)
            for zone in ZONES[::-1]:
                vals = sub[sub["領域"] == zone]["値"].to_numpy()
                if vals.size:
                    ax.hist(vals, bins=bins, density=True, alpha=0.45, color=colors[zone], label=zone)
            if "diff" in feature:
                ax.axvline(0, color="black", linestyle="--", linewidth=0.8)
            ax.set_title(elapsed)
            ax.grid(True, axis="y", alpha=0.25)
        handles, labels = axes.ravel()[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", ncol=2)
        fig.suptitle(f"田んぼ領域: {feature_labels[feature]} の分布")
        fig.supxlabel(feature_labels[feature])
        fig.supylabel("密度")
        fig.tight_layout()
        safe = feature.replace("_db", "")
        fig.savefig(OUT_DIR / f"図_{safe}_分布ヒストグラム.png", bbox_inches="tight")
        plt.close(fig)

    print(summary_df.to_string(index=False))
    print(f"saved: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
