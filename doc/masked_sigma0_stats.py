#!/usr/bin/env python3
# masked_sigma0_stats.py
#
# Input:
#   --sigma_db  : GeoTIFF (single band) of sigma0 in dB
#   --mask      : GeoTIFF mask (same grid as sigma_db). Non-zero => keep by default
#
# Output:
#   --out_csv   : CSV with summary stats (count, mean_linear, mean_db_from_linear, mean_db, etc.)
#
# Notes:
# - dB<->linear conversion:
#     sigma0_db = 10 * log10(sigma0_linear)
#     sigma0_linear = 10 ** (sigma0_db / 10)
#   :contentReference[oaicite:2]{index=2}
#
# - Rasterio nodata masks: dataset_mask() etc. :contentReference[oaicite:3]{index=3}

import argparse
import csv
import math
import numpy as np
import rasterio

def db_to_linear(db: np.ndarray) -> np.ndarray:
    # sigma0_linear = 10 ** (db/10)
    return np.power(10.0, db / 10.0)

def safe_stats_from_stream(
    sigma_path: str,
    mask_path: str,
    band: int,
    mask_keep_values: set[int] | None,
    mask_nonzero: bool,
    ignore_nodata: bool,
    sample_size: int,
    seed: int,
):
    """
    Streaming stats on masked pixels:
    - count
    - mean in linear
    - mean in dB (arith mean of dB, for reference only)
    - mean_db_from_linear_mean: 10*log10(mean_linear)
    - min/max (dB)
    - optional reservoir sample for median / percentiles (approx)
    """
    rng = np.random.default_rng(seed)

    count = 0
    sum_linear = 0.0
    sum_db = 0.0
    min_db = math.inf
    max_db = -math.inf

    # Reservoir sampling for approx percentiles without storing all pixels
    sample = np.empty((sample_size,), dtype=np.float32) if sample_size > 0 else None
    sample_filled = 0

    with rasterio.open(sigma_path) as sig, rasterio.open(mask_path) as msk:
        # Grid check (strict). If mismatched, align mask beforehand.
        if (sig.crs != msk.crs) or (sig.transform != msk.transform) or (sig.width != msk.width) or (sig.height != msk.height):
            raise ValueError(
                "Mask GeoTIFF grid does not match sigma GeoTIFF grid. "
                "Please reproject/resample mask to match (same CRS, transform, width/height)."
            )

        sig_nodata = sig.nodata
        msk_nodata = msk.nodata

        # Iterate by internal blocks to be memory-safe
        for _, window in sig.block_windows(band):
            db = sig.read(band, window=window).astype(np.float32)
            mk = msk.read(1, window=window)

            # Base validity mask: finite sigma + optional nodata exclusion
            valid = np.isfinite(db)
            if ignore_nodata and (sig_nodata is not None):
                valid &= (db != sig_nodata)
            # Rasterio dataset masks can be used too, but nodata+finite check is usually sufficient. :contentReference[oaicite:4]{index=4}

            # Mask selection
            if mask_nonzero:
                keep = (mk != 0)
            else:
                keep = np.zeros(mk.shape, dtype=bool)
                for v in (mask_keep_values or set()):
                    keep |= (mk == v)

            if ignore_nodata and (msk_nodata is not None):
                keep &= (mk != msk_nodata)

            sel = valid & keep
            if not np.any(sel):
                continue

            db_sel = db[sel]
            lin_sel = db_to_linear(db_sel)

            n = db_sel.size
            count += n
            sum_db += float(np.sum(db_sel))
            sum_linear += float(np.sum(lin_sel))

            local_min = float(np.min(db_sel))
            local_max = float(np.max(db_sel))
            if local_min < min_db:
                min_db = local_min
            if local_max > max_db:
                max_db = local_max

            # Reservoir sampling update (optional)
            if sample is not None and sample_size > 0:
                for val in db_sel:
                    if sample_filled < sample_size:
                        sample[sample_filled] = val
                        sample_filled += 1
                    else:
                        j = rng.integers(0, count)  # uniform over seen items
                        if j < sample_size:
                            sample[j] = val

    if count == 0:
        raise ValueError("No pixels selected by mask (or all were nodata).")

    mean_linear = sum_linear / count
    mean_db = sum_db / count
    mean_db_from_linear = 10.0 * math.log10(mean_linear)

    result = {
        "count": count,
        "mean_linear": mean_linear,
        "mean_db_arith": mean_db,  # reference only
        "mean_db_from_linear_mean": mean_db_from_linear,  # recommended representative
        "min_db": min_db,
        "max_db": max_db,
    }

    # Approx percentiles from reservoir sample
    if sample is not None and sample_filled > 0:
        s = np.sort(sample[:sample_filled])
        def pct(p):
            return float(np.percentile(s, p))
        result.update({
            "approx_p05_db": pct(5),
            "approx_median_db": pct(50),
            "approx_p95_db": pct(95),
        })

    return result

def write_csv(out_csv: str, stats: dict, sigma_path: str, mask_path: str):
    fieldnames = ["sigma_db_path", "mask_path"] + list(stats.keys())
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        row = {"sigma_db_path": sigma_path, "mask_path": mask_path}
        row.update(stats)
        w.writerow(row)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sigma_db", required=True, help="GeoTIFF of sigma0 in dB (single-band recommended)")
    ap.add_argument("--mask", required=True, help="Mask GeoTIFF aligned to sigma (same grid)")
    ap.add_argument("--band", type=int, default=1, help="Band index of sigma_db (1-based)")
    ap.add_argument("--mask_nonzero", action="store_true", help="Keep pixels where mask != 0 (default)")
    ap.add_argument("--mask_values", default=None, help="Comma-separated mask values to keep (if not using --mask_nonzero)")
    ap.add_argument("--no_ignore_nodata", action="store_true", help="Do not filter nodata")
    ap.add_argument("--sample_size", type=int, default=200000, help="Reservoir sample size for approx percentiles (0 disables)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out_csv", required=True, help="Output CSV for summary stats")
    args = ap.parse_args()

    if args.mask_values:
        mask_keep_values = {int(x) for x in args.mask_values.split(",") if x.strip() != ""}
    else:
        mask_keep_values = None

    stats = safe_stats_from_stream(
        sigma_path=args.sigma_db,
        mask_path=args.mask,
        band=args.band,
        mask_keep_values=mask_keep_values,
        mask_nonzero=(True if not args.mask_values else False) or args.mask_nonzero,
        ignore_nodata=(not args.no_ignore_nodata),
        sample_size=max(0, args.sample_size),
        seed=args.seed,
    )

    write_csv(args.out_csv, stats, args.sigma_db, args.mask)
    print("Wrote:", args.out_csv)
    print("Summary:", stats)
    print("\nInterpretation tip:")
    print("- mean_db_from_linear_mean is the recommended representative backscatter (average in linear, then convert to dB).")
    print("- mean_db_arith is the arithmetic mean of dB values (different meaning).")

if __name__ == "__main__":
    main()