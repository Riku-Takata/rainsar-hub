#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Fast point sampling of signed backscatter differences for Kurume zones."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform as transform_coords


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PAIR_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "processed_by_date"
DEFAULT_RAIN_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1"
DEFAULT_KURUME_DIR = Path(r"D:\sotsuron\kurume")
DEFAULT_OUTPUT_DIR = DEFAULT_RAIN_DIR / "kurume_inundation_analysis" / "feature_point_sampling"
ELAPSED_STEP_HOURS = 3
ELAPSED_MAX_HOURS = 48
ELAPSED_BINS = list(range(0, ELAPSED_MAX_HOURS + ELAPSED_STEP_HOURS, ELAPSED_STEP_HOURS))
ELAPSED_LABELS = [f"{left}-{right}h" for left, right in zip(ELAPSED_BINS[:-1], ELAPSED_BINS[1:])]


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


def stratified_sample(values: np.ndarray, n: int, rng: np.random.Generator, bins: int = 20) -> np.ndarray:
    values = values[np.isfinite(values)].astype(np.float32, copy=False)
    if values.size <= n:
        return values.copy()
    values = np.sort(values)
    chunks = np.array_split(values, bins)
    per_bin = max(1, n // bins)
    out = []
    for chunk in chunks:
        if chunk.size == 0:
            continue
        take = min(per_bin, chunk.size)
        out.append(rng.choice(chunk, size=take, replace=False))
    sampled = np.concatenate(out) if out else np.empty(0, dtype=np.float32)
    if sampled.size > n:
        sampled = rng.choice(sampled, size=n, replace=False)
    return sampled.astype(np.float32, copy=False)


def stats(values: np.ndarray) -> dict[str, Any]:
    values = values[np.isfinite(values)].astype(np.float64, copy=False)
    if values.size == 0:
        return {"sample_count": 0}
    q = np.percentile(values, [5, 25, 50, 75, 95])
    mean = float(values.mean())
    std = float(values.std())
    if std > 0:
        z = (values - mean) / std
        skewness = float(np.mean(z**3))
        kurtosis = float(np.mean(z**4) - 3.0)
    else:
        skewness = 0.0
        kurtosis = 0.0
    return {
        "sample_count": int(values.size),
        "mean": mean,
        "std": std,
        "min": float(values.min()),
        "p05": float(q[0]),
        "p25": float(q[1]),
        "p50": float(q[2]),
        "p75": float(q[3]),
        "p95": float(q[4]),
        "max": float(values.max()),
        "iqr": float(q[3] - q[1]),
        "mad": float(np.median(np.abs(values - q[2]))),
        "skewness": skewness,
        "kurtosis": kurtosis,
        "positive_ratio": float(np.mean(values > 0)),
        "negative_ratio": float(np.mean(values < 0)),
        "near_zero_ratio_abs_lt_0_5": float(np.mean(np.abs(values) < 0.5)),
        "strong_negative_ratio_lt_minus_2": float(np.mean(values < -2.0)),
        "strong_positive_ratio_gt_2": float(np.mean(values > 2.0)),
    }


def sample_scene_points(src: rasterio.DatasetReader, n: int, rng: np.random.Generator, multiplier: int = 3) -> list[tuple[float, float]]:
    rows = rng.integers(0, src.height, size=n * multiplier)
    cols = rng.integers(0, src.width, size=n * multiplier)
    xs, ys = rasterio.transform.xy(src.transform, rows, cols, offset="center")
    return list(zip(xs, ys))


def inundated_flags_for_points(
    mask_paths: list[Path],
    points: list[tuple[float, float]],
    point_crs,
    mask_min: float,
    mask_max: float,
) -> np.ndarray:
    if not points:
        return np.zeros(0, dtype=bool)
    flags = np.zeros(len(points), dtype=bool)
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    for mask_path in mask_paths:
        with rasterio.open(mask_path) as src:
            sample_points = points
            if src.crs and point_crs and src.crs != point_crs:
                mx, my = transform_coords(point_crs, src.crs, xs, ys)
                sample_points = list(zip(mx, my))
            values = np.array([v[0] for v in src.sample(sample_points)], dtype=np.float32)
            valid = np.isfinite(values)
            if src.nodata is not None:
                valid &= values != src.nodata
            flags |= valid & (values >= mask_min) & (values <= mask_max)
    return flags


def sample_mask_points(
    mask_path: Path,
    zone: str,
    n: int,
    rng: np.random.Generator,
    target_crs,
    mask_min: float,
    mask_max: float,
) -> list[tuple[float, float]]:
    with rasterio.open(mask_path) as src:
        arr = src.read(1, masked=True)
        valid = ~np.ma.getmaskarray(arr)
        data = np.asarray(arr.filled(np.nan))
        if zone == "inundated":
            mask = valid & (data >= mask_min) & (data <= mask_max)
        else:
            mask = valid & ~((data >= mask_min) & (data <= mask_max))
        rows, cols = np.where(mask)
        if rows.size == 0:
            return []
        take = min(n, rows.size)
        idx = rng.choice(rows.size, size=take, replace=False)
        xs, ys = rasterio.transform.xy(src.transform, rows[idx], cols[idx], offset="center")
        xs = list(xs)
        ys = list(ys)
        if src.crs and target_crs and src.crs != target_crs:
            xs, ys = transform_coords(src.crs, target_crs, xs, ys)
        return list(zip(xs, ys))


def sample_diff_at_points(target_src, before_vrt, points: list[tuple[float, float]]) -> np.ndarray:
    if not points:
        return np.empty(0, dtype=np.float32)
    target = np.array([v[0] for v in target_src.sample(points)], dtype=np.float32)
    before = np.array([v[0] for v in before_vrt.sample(points)], dtype=np.float32)
    valid = np.isfinite(target) & np.isfinite(before)
    if target_src.nodata is not None:
        valid &= target != target_src.nodata
    return (target[valid] - before[valid]).astype(np.float32, copy=False)


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
    parser.add_argument("--per-pair-zone-sample", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=20260513)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.output_dir / "progress.log"

    def log(message: str) -> None:
        print(message, flush=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(message + "\n")

    if log_path.exists():
        log_path.unlink()

    rng = np.random.default_rng(args.seed)
    mask_paths = sorted(args.kurume_dir.glob("*.tif"))
    manifest = read_manifest(args.pair_dir / "manifest.csv")
    delay_df = pd.read_csv(args.rain_dir / "map7_db_rain_days_s1_delay_all_jst.csv")
    delay_by_stac = delay_df.set_index("stac_id")["delay_from_rain_start_h"].to_dict()

    selected = [
        (key, roles)
        for key, roles in sorted(manifest.items())
        if "target" in roles and "pair" in roles and int(roles["target"]["rain_day_jst"][:4]) <= args.max_year
    ]
    values_by_group: dict[tuple[str, str], list[np.ndarray]] = {}
    pair_rows = []

    for index, ((rain_day, pair_no), roles) in enumerate(selected, start=1):
        stac_id = roles["target"]["stac_id"]
        delay_h = float(delay_by_stac.get(stac_id, np.nan))
        bin_label = elapsed_bin(delay_h)
        if bin_label is None:
            continue
        log(f"[{index}/{len(selected)}] {rain_day} {pair_no}: {bin_label}, elapsed={delay_h:.3f}h")
        target_path = Path(roles["target"]["organized_path"])
        before_path = Path(roles["pair"]["organized_path"])
        with rasterio.open(target_path) as target_src, rasterio.open(before_path) as before_src:
            with WarpedVRT(
                before_src,
                crs=target_src.crs,
                transform=target_src.transform,
                width=target_src.width,
                height=target_src.height,
                resampling=Resampling.bilinear,
                nodata=np.nan,
            ) as before_vrt:
                scene_points = sample_scene_points(target_src, args.per_pair_zone_sample, rng)
                scene_inundated = inundated_flags_for_points(
                    mask_paths,
                    scene_points,
                    target_src.crs,
                    args.mask_min,
                    args.mask_max,
                )
                scene_non_inundated_points = [
                    point for point, is_inundated in zip(scene_points, scene_inundated) if not is_inundated
                ][: args.per_pair_zone_sample]
                scene_non_inundated_values = sample_diff_at_points(
                    target_src,
                    before_vrt,
                    scene_non_inundated_points,
                )
                values_by_group.setdefault(("シーン全体", bin_label), []).append(scene_non_inundated_values)
                values_by_group.setdefault(("非浸水域", bin_label), []).append(scene_non_inundated_values)

                union_inun = []
                for mask_path in mask_paths:
                    inun_points = sample_mask_points(
                        mask_path,
                        "inundated",
                        args.per_pair_zone_sample,
                        rng,
                        target_src.crs,
                        args.mask_min,
                        args.mask_max,
                    )
                    inun_values = sample_diff_at_points(target_src, before_vrt, inun_points)
                    if inun_values.size:
                        values_by_group.setdefault((f"{mask_path.stem}_浸水域", bin_label), []).append(inun_values)
                        union_inun.append(inun_values)
                if union_inun:
                    values_by_group.setdefault(("浸水域", bin_label), []).append(np.concatenate(union_inun))

        pair_rows.append(
            {
                "rain_day_jst": rain_day,
                "pair_no": pair_no,
                "target_stac_id": stac_id,
                "elapsed_h": delay_h,
                "elapsed_bin": bin_label,
            }
        )

    log("sampling finished; extracting statistical features")
    feature_rows = []
    sample_rows = []
    for (area, bin_label), chunks in sorted(values_by_group.items()):
        raw = np.concatenate(chunks) if chunks else np.empty(0, dtype=np.float32)
        sampled = stratified_sample(raw, args.sample_size, rng)
        feature_rows.append(
            {
                "area": area,
                "elapsed_bin": bin_label,
                "raw_sample_count": int(raw.size),
                **stats(sampled),
            }
        )
        sample_rows.extend({"area": area, "elapsed_bin": bin_label, "diff": float(v)} for v in sampled)

    features = pd.DataFrame(feature_rows)
    samples = pd.DataFrame(sample_rows)
    pairs = pd.DataFrame(pair_rows)
    feature_csv = args.output_dir / "feature_sample_statistics.csv"
    sample_csv = args.output_dir / "feature_sample_signed_diff.csv"
    pair_csv = args.output_dir / "feature_sample_pairs.csv"
    features.to_csv(feature_csv, index=False, encoding="utf-8-sig")
    samples.to_csv(sample_csv, index=False, encoding="utf-8-sig")
    pairs.to_csv(pair_csv, index=False, encoding="utf-8-sig")

    try:
        import matplotlib.pyplot as plt

        plt.rcParams["font.family"] = ["Yu Gothic", "Meiryo", "MS Gothic", "DejaVu Sans"]
        plt.rcParams["axes.unicode_minus"] = False
        plot_areas = ["浸水域", "シーン全体"]
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
            label = "シーン全体（浸水域除外）" if area == "シーン全体" else area
            observed = g["mean"].notna().to_numpy()
            ax.plot(x[observed], g.loc[observed, "mean"], marker="o", label=label)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(plot_labels)
        ax.set_xlabel("経過時間帯")
        ax.set_ylabel("特徴点サンプル平均差分 target - before")
        ax.set_title("20,000画素サンプルによる符号付き差分")
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(args.output_dir / "feature_sample_mean_diff_elapsed.png")
        fig.savefig(args.output_dir / "特徴点サンプル_平均差分_経過時間.png")
        plt.close(fig)
    except Exception as exc:
        log(f"plot skipped: {exc}")

    summary = {
        "sample_size_per_area_elapsed_bin": args.sample_size,
        "elapsed_bin_width_hours": ELAPSED_STEP_HOURS,
        "elapsed_range_hours": f"0 <= elapsed < {ELAPSED_MAX_HOURS}",
        "per_pair_zone_sample": args.per_pair_zone_sample,
        "max_year": args.max_year,
        "inundated_condition": f"{args.mask_min} <= inundation_depth <= {args.mask_max}",
        "feature_csv": str(feature_csv),
        "sample_csv": str(sample_csv),
        "pair_csv": str(pair_csv),
        "progress_log": str(log_path),
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log(json.dumps(summary, ensure_ascii=False))
    print(features[features["area"].isin(["浸水域", "シーン全体"])].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
