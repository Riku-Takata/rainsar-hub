#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Sample representative signed-difference pixels and extract statistical features."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PAIR_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed_by_date"
DEFAULT_RAIN_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"
DEFAULT_KURUME_DIR = Path(r"D:\sotsuron\kurume")
DEFAULT_OUTPUT_DIR = DEFAULT_RAIN_DIR / "kurume_inundation_analysis" / "feature_pixel_sampling"
ELAPSED_STEP_HOURS = 3
ELAPSED_MAX_HOURS = 48
ELAPSED_BINS = list(range(0, ELAPSED_MAX_HOURS + ELAPSED_STEP_HOURS, ELAPSED_STEP_HOURS))
ELAPSED_LABELS = [f"{left}-{right}h" for left, right in zip(ELAPSED_BINS[:-1], ELAPSED_BINS[1:])]


@dataclass
class Reservoir:
    capacity: int
    rng: np.random.Generator
    values: np.ndarray
    seen: int = 0
    size: int = 0

    @classmethod
    def create(cls, capacity: int, seed: int) -> "Reservoir":
        return cls(
            capacity=capacity,
            rng=np.random.default_rng(seed),
            values=np.empty(capacity, dtype=np.float32),
        )

    def update(self, incoming: np.ndarray) -> None:
        incoming = incoming[np.isfinite(incoming)].astype(np.float32, copy=False)
        if incoming.size == 0:
            return
        for value in incoming:
            self.seen += 1
            if self.size < self.capacity:
                self.values[self.size] = value
                self.size += 1
            else:
                j = int(self.rng.integers(0, self.seen))
                if j < self.capacity:
                    self.values[j] = value

    def sample(self) -> np.ndarray:
        return self.values[: self.size].copy()


def read_manifest(path: Path) -> dict[tuple[str, str], dict[str, dict[str, str]]]:
    grouped: dict[tuple[str, str], dict[str, dict[str, str]]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            key = (row["rain_day_jst"], row["pair_no"])
            grouped.setdefault(key, {})[row["role"]] = row
    return grouped


def elapsed_bin(delay_h: float) -> str | None:
    for left, right, label in zip(ELAPSED_BINS[:-1], ELAPSED_BINS[1:], ELAPSED_LABELS):
        if left <= delay_h < right:
            return label
    return None


def get_reservoir(reservoirs: dict[tuple[str, str], Reservoir], key: tuple[str, str], capacity: int) -> Reservoir:
    if key not in reservoirs:
        seed = abs(hash(key)) % (2**32)
        reservoirs[key] = Reservoir.create(capacity=capacity, seed=seed)
    return reservoirs[key]


def stratified_sample(values: np.ndarray, n: int, rng: np.random.Generator, bins: int = 20) -> np.ndarray:
    values = values[np.isfinite(values)]
    if values.size <= n:
        return values.copy()
    order = np.argsort(values)
    sorted_values = values[order]
    chunks = np.array_split(sorted_values, bins)
    per_bin = max(1, n // bins)
    samples: list[np.ndarray] = []
    for chunk in chunks:
        if chunk.size == 0:
            continue
        take = min(per_bin, chunk.size)
        idx = rng.choice(chunk.size, size=take, replace=False)
        samples.append(chunk[idx])
    sampled = np.concatenate(samples) if samples else np.empty(0, dtype=np.float32)
    if sampled.size < n:
        remaining = np.setdiff1d(values, sampled, assume_unique=False)
        if remaining.size:
            take = min(n - sampled.size, remaining.size)
            sampled = np.concatenate([sampled, rng.choice(remaining, size=take, replace=False)])
    elif sampled.size > n:
        sampled = rng.choice(sampled, size=n, replace=False)
    return sampled.astype(np.float32, copy=False)


def stats(values: np.ndarray) -> dict[str, Any]:
    values = values[np.isfinite(values)]
    if values.size == 0:
        return {
            "sample_count": 0,
            "mean": None,
            "std": None,
            "min": None,
            "p05": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p95": None,
            "max": None,
            "iqr": None,
            "mad": None,
            "skewness": None,
            "kurtosis": None,
            "positive_ratio": None,
            "negative_ratio": None,
            "near_zero_ratio_abs_lt_0_5": None,
            "strong_negative_ratio_lt_minus_2": None,
            "strong_positive_ratio_gt_2": None,
        }
    q = np.percentile(values, [5, 25, 50, 75, 95])
    mean = float(np.mean(values))
    std = float(np.std(values))
    centered = values.astype(np.float64) - mean
    if std > 0:
        z = centered / std
        skewness = float(np.mean(z**3))
        kurtosis = float(np.mean(z**4) - 3.0)
    else:
        skewness = 0.0
        kurtosis = 0.0
    median = float(q[2])
    mad = float(np.median(np.abs(values - median)))
    return {
        "sample_count": int(values.size),
        "mean": mean,
        "std": std,
        "min": float(np.min(values)),
        "p05": float(q[0]),
        "p25": float(q[1]),
        "p50": median,
        "p75": float(q[3]),
        "p95": float(q[4]),
        "max": float(np.max(values)),
        "iqr": float(q[3] - q[1]),
        "mad": mad,
        "skewness": skewness,
        "kurtosis": kurtosis,
        "positive_ratio": float(np.mean(values > 0)),
        "negative_ratio": float(np.mean(values < 0)),
        "near_zero_ratio_abs_lt_0_5": float(np.mean(np.abs(values) < 0.5)),
        "strong_negative_ratio_lt_minus_2": float(np.mean(values < -2.0)),
        "strong_positive_ratio_gt_2": float(np.mean(values > 2.0)),
    }


def process_pair(
    target_path: Path,
    before_path: Path,
    mask_paths: list[Path],
    bin_label: str,
    reservoirs: dict[tuple[str, str], Reservoir],
    reservoir_size: int,
    mask_min: float,
    mask_max: float,
) -> None:
    with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
        mask_sources = [rasterio.open(path) for path in mask_paths]
        try:
            mask_vrts = [
                WarpedVRT(
                    src,
                    crs=target_src.crs,
                    transform=target_src.transform,
                    width=target_src.width,
                    height=target_src.height,
                    resampling=Resampling.nearest,
                    nodata=np.nan,
                )
                for src in mask_sources
            ]
            before_vrt = WarpedVRT(
                before_src,
                crs=target_src.crs,
                transform=target_src.transform,
                width=target_src.width,
                height=target_src.height,
                resampling=Resampling.bilinear,
                nodata=np.nan,
            )
            try:
                for _, window in target_src.block_windows(1):
                    target_arr = target_src.read(1, window=window, masked=False).astype(np.float32, copy=False)
                    before_arr = before_vrt.read(1, window=window, masked=False).astype(np.float32, copy=False)
                    valid = np.isfinite(target_arr) & np.isfinite(before_arr)
                    if target_src.nodata is not None:
                        valid &= target_arr != target_src.nodata
                    if before_src.nodata is not None:
                        valid &= before_arr != before_src.nodata
                    if not valid.any():
                        continue
                    diff = target_arr - before_arr
                    get_reservoir(reservoirs, ("シーン全体", bin_label), reservoir_size).update(diff[valid])

                    union_footprint = np.zeros((window.height, window.width), dtype=bool)
                    union_inundated = np.zeros((window.height, window.width), dtype=bool)
                    area_masks: list[tuple[str, np.ndarray, np.ndarray]] = []
                    for path, vrt in zip(mask_paths, mask_vrts):
                        mask_arr = vrt.read(1, window=window, masked=False)
                        footprint = np.isfinite(mask_arr)
                        if not footprint.any():
                            continue
                        inundated = footprint & (mask_arr >= mask_min) & (mask_arr <= mask_max)
                        non_inundated = footprint & ~inundated
                        union_footprint |= footprint
                        union_inundated |= inundated
                        base = path.stem
                        area_masks.append((base, inundated, non_inundated))

                    if union_footprint.any():
                        union_non = union_footprint & ~union_inundated
                        get_reservoir(reservoirs, ("浸水域", bin_label), reservoir_size).update(diff[union_inundated & valid])
                        get_reservoir(reservoirs, ("非浸水域", bin_label), reservoir_size).update(diff[union_non & valid])

                    for base, inundated, non_inundated in area_masks:
                        get_reservoir(reservoirs, (f"{base}_浸水域", bin_label), reservoir_size).update(
                            diff[inundated & valid]
                        )
                        get_reservoir(reservoirs, (f"{base}_非浸水域", bin_label), reservoir_size).update(
                            diff[non_inundated & valid]
                        )
            finally:
                before_vrt.close()
                for vrt in mask_vrts:
                    vrt.close()
        finally:
            for src in mask_sources:
                src.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-dir", type=Path, default=DEFAULT_PAIR_DIR)
    parser.add_argument("--rain-dir", type=Path, default=DEFAULT_RAIN_DIR)
    parser.add_argument("--kurume-dir", type=Path, default=DEFAULT_KURUME_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-year", type=int, default=2022)
    parser.add_argument("--mask-min", type=float, default=0.5)
    parser.add_argument("--mask-max", type=float, default=1.7)
    parser.add_argument("--sample-size", type=int, default=20000)
    parser.add_argument("--reservoir-size", type=int, default=200000)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    mask_paths = sorted(args.kurume_dir.glob("*.tif"))
    manifest = read_manifest(args.pair_dir / "manifest.csv")
    delay_df = pd.read_csv(args.rain_dir / "map7_db_rain_days_s1_delay_all_jst.csv")
    delay_by_stac = delay_df.set_index("stac_id")["delay_from_rain_start_h"].to_dict()

    selected = [
        (key, roles)
        for key, roles in sorted(manifest.items())
        if "target" in roles and "pair" in roles and int(roles["target"]["rain_day_jst"][:4]) <= args.max_year
    ]

    reservoirs: dict[tuple[str, str], Reservoir] = {}
    pair_rows = []
    for index, ((rain_day, pair_no), roles) in enumerate(selected, start=1):
        stac_id = roles["target"]["stac_id"]
        delay_h = float(delay_by_stac.get(stac_id, np.nan))
        bin_label = elapsed_bin(delay_h)
        if bin_label is None:
            continue
        print(f"[{index}/{len(selected)}] {rain_day} {pair_no} {bin_label}")
        process_pair(
            Path(roles["target"]["organized_path"]),
            Path(roles["pair"]["organized_path"]),
            mask_paths,
            bin_label,
            reservoirs,
            args.reservoir_size,
            args.mask_min,
            args.mask_max,
        )
        pair_rows.append(
            {
                "rain_day_jst": rain_day,
                "pair_no": pair_no,
                "target_stac_id": stac_id,
                "elapsed_h": delay_h,
                "elapsed_bin": bin_label,
            }
        )

    rng = np.random.default_rng(20260513)
    feature_rows = []
    sample_rows = []
    for (area, bin_label), reservoir in sorted(reservoirs.items()):
        reservoir_values = reservoir.sample()
        sample = stratified_sample(reservoir_values, args.sample_size, rng)
        feature_rows.append(
            {
                "area": area,
                "elapsed_bin": bin_label,
                "source_pixel_seen": reservoir.seen,
                "reservoir_count": reservoir.size,
                **stats(sample),
            }
        )
        for value in sample:
            sample_rows.append({"area": area, "elapsed_bin": bin_label, "diff": float(value)})

    features = pd.DataFrame(feature_rows)
    samples = pd.DataFrame(sample_rows)
    pairs = pd.DataFrame(pair_rows)

    feature_csv = args.output_dir / "特徴点サンプル_統計特徴量.csv"
    sample_csv = args.output_dir / "特徴点サンプル_符号付き差分.csv"
    pair_csv = args.output_dir / "特徴点サンプル_対象ペア.csv"
    features.to_csv(feature_csv, index=False, encoding="utf-8-sig")
    samples.to_csv(sample_csv, index=False, encoding="utf-8-sig")
    pairs.to_csv(pair_csv, index=False, encoding="utf-8-sig")

    try:
        import matplotlib.pyplot as plt

        plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
        plt.rcParams["axes.unicode_minus"] = False

        plot_areas = ["浸水域", "非浸水域", "シーン全体"]
        main = features[features["area"].isin(plot_areas)].copy()
        common_observed_labels = set.intersection(
            *(set(main.loc[main["area"] == area, "elapsed_bin"]) for area in plot_areas)
        )
        last_observed_index = max(
            (index for index, label in enumerate(ELAPSED_LABELS) if label in common_observed_labels),
            default=-1,
        )
        plot_labels = ELAPSED_LABELS[: last_observed_index + 1]
        x = np.arange(len(plot_labels))
        fig, ax = plt.subplots(figsize=(9, 4), dpi=160)
        for area in plot_areas:
            g = main[main["area"] == area].set_index("elapsed_bin").reindex(plot_labels)
            observed = g["mean"].notna().to_numpy()
            ax.plot(x[observed], g.loc[observed, "mean"], marker="o", label=area)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(plot_labels)
        ax.set_xlabel("経過時間帯")
        ax.set_ylabel("特徴点サンプル平均差分 target - before")
        ax.set_title("20,000画素サンプルによる符号付き差分")
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(args.output_dir / "特徴点サンプル_平均差分_経過時間.png")
        plt.close(fig)

        heat = features[features["area"].str.endswith("_浸水域") | (features["area"] == "浸水域")]
        heat = heat.pivot(index="area", columns="elapsed_bin", values="mean").reindex(columns=plot_labels)
        fig, ax = plt.subplots(figsize=(8, max(4, 0.35 * len(heat))), dpi=160)
        matrix = heat.to_numpy(dtype=float)
        vmax = np.nanmax(np.abs(matrix)) if np.isfinite(matrix).any() else 1
        image = ax.imshow(matrix, aspect="auto", cmap="coolwarm", vmin=-vmax, vmax=vmax)
        ax.set_xticks(np.arange(len(heat.columns)))
        ax.set_xticklabels(heat.columns, rotation=45, ha="right")
        ax.set_yticks(np.arange(len(heat.index)))
        ax.set_yticklabels(heat.index)
        ax.set_title("各浸水域の特徴点サンプル平均差分")
        fig.colorbar(image, ax=ax, label="平均差分 target - before")
        fig.tight_layout()
        fig.savefig(args.output_dir / "特徴点サンプル_各浸水域ヒートマップ.png")
        plt.close(fig)
    except Exception as exc:
        print(f"plot skipped: {exc}")

    summary = {
        "sample_size_per_area_elapsed_bin": args.sample_size,
        "elapsed_bin_width_hours": ELAPSED_STEP_HOURS,
        "elapsed_range_hours": f"0 <= elapsed < {ELAPSED_MAX_HOURS}",
        "reservoir_size_per_area_elapsed_bin": args.reservoir_size,
        "max_year": args.max_year,
        "inundated_condition": f"{args.mask_min} <= inundation_depth <= {args.mask_max}",
        "sampling": "reservoir sampling followed by quantile-stratified sampling",
        "feature_csv": str(feature_csv),
        "sample_csv": str(sample_csv),
        "pair_csv": str(pair_csv),
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(features[features["area"].isin(["浸水域", "非浸水域", "シーン全体"])].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
