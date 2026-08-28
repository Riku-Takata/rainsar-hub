#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Plot per-day pair backscatter distributions from histogram CSV."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_STATS_DIR = ROOT_DIR / "output" / "gsi_h30_geojson_s1" / "map7_rain_s1" / "backscatter_stats"


def safe_name(value: str) -> str:
    return value.replace(":", "").replace("/", "-").replace("\\", "-")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stats-dir", type=Path, default=DEFAULT_STATS_DIR)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()

    stats_dir = args.stats_dir
    output_dir = args.output_dir or (stats_dir / "pair_distribution_plots")
    output_dir.mkdir(parents=True, exist_ok=True)

    hist_path = stats_dir / "map7_backscatter_histograms_by_mask.csv"
    hist = pd.read_csv(hist_path)

    import matplotlib.pyplot as plt

    plot_rows = []
    keys = ["rain_day_jst", "pair_no"]
    for (rain_day, pair_no), pair_df in hist.groupby(keys, sort=True):
        masks = sorted(pair_df["mask"].unique())
        fig, axes = plt.subplots(len(masks), 2, figsize=(12, 4 * len(masks)), dpi=160, squeeze=False)

        for row_idx, mask in enumerate(masks):
            mask_df = pair_df[pair_df["mask"] == mask]

            ax = axes[row_idx][0]
            for value_type in ["target", "before"]:
                g = mask_df[mask_df["value_type"] == value_type]
                if g.empty or g["count"].sum() == 0:
                    continue
                centers = (g["bin_left"] + g["bin_right"]) / 2.0
                ax.plot(centers, g["frequency"], label=value_type, linewidth=1.8)
            ax.set_title(f"{rain_day} {pair_no} {mask}: target / before")
            ax.set_xlabel("Backscatter")
            ax.set_ylabel("Frequency")
            ax.grid(True, alpha=0.3)
            handles, labels = ax.get_legend_handles_labels()
            if handles:
                ax.legend(handles, labels)

            ax = axes[row_idx][1]
            g = mask_df[mask_df["value_type"] == "diff"]
            if not g.empty and g["count"].sum() > 0:
                centers = (g["bin_left"] + g["bin_right"]) / 2.0
                width = float((g["bin_right"] - g["bin_left"]).median())
                ax.bar(centers, g["frequency"], width=width, align="center")
            ax.axvline(0, color="black", linewidth=0.8)
            ax.set_title(f"{rain_day} {pair_no} {mask}: diff = target - before")
            ax.set_xlabel("Difference")
            ax.set_ylabel("Frequency")
            ax.grid(True, alpha=0.3)

        fig.tight_layout()
        out_path = output_dir / f"{safe_name(rain_day)}_{safe_name(pair_no)}_distribution.png"
        fig.savefig(out_path)
        plt.close(fig)
        plot_rows.append({"rain_day_jst": rain_day, "pair_no": pair_no, "plot_path": str(out_path)})
        print(out_path)

    index_path = output_dir / "pair_distribution_plots_index.csv"
    pd.DataFrame(plot_rows).to_csv(index_path, index=False, encoding="utf-8-sig")
    print(index_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
