from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import analyze_map7_bbox_paddy_pixel_polygon_20000 as base
import export_map7_paddy_dem5m_probabilities as prob_base
import run_map7_paddy_dem5m_classification as dem_base


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_paddy_dem5m_only_classification"


def md_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    shown = shown.fillna("")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def plot_metrics(best: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.8))
    x = range(len(best))
    ax.bar([i - 0.2 for i in x], best["balanced_accuracy"], width=0.2, label="Balanced Accuracy")
    ax.bar(x, best["precision"], width=0.2, label="Precision")
    ax.bar([i + 0.2 for i in x], best["recall"], width=0.2, label="Recall")
    ax.set_xticks(list(x))
    ax.set_xticklabels(best["scenario"])
    ax.set_ylim(0.45, 1.0)
    ax.set_title("DEM5m特徴量のみの浸水判別精度")
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT / "図_DEM5mのみ_精度比較.png", dpi=220, bbox_inches="tight")
    plt.close()


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame, best: pd.DataFrame) -> None:
    lines = [
        "# DEM5m特徴量のみの浸水判別",
        "",
        "## 方法",
        "",
        "- 後方散乱強度差分は使わず、GSI DEM 5m由来の特徴量のみで学習しました。",
        "- 田んぼ画素単位: `elevation_m`, `slope_deg`, `relative_elevation_7x7_m`。",
        "- 田んぼ筆ポリゴン単位: 標高・傾斜・相対標高の筆内統計量。",
        "- 浸水/非浸水は同数抽出し、7:3で学習用・検証用に分割しました。",
        "- RandomForestとXGBoostを比較し、検証データのBalanced Accuracyが高いモデルを採用しました。",
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## 最良モデル",
        "",
        md_table(best),
        "",
        "## 全モデル評価",
        "",
        md_table(metrics),
        "",
    ]
    (OUT / "DEM5mのみ_機械学習精度レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pixel_df, polygon_df, _, _, _ = prob_base.load_frames()
    scenarios = [
        ("田んぼ画素", pixel_df, dem_base.PIXEL_DEM_FEATURES, prob_base.PIXEL_PER_CLASS),
        ("田んぼ筆ポリゴン", polygon_df, dem_base.POLYGON_DEM_FEATURES, prob_base.POLYGON_PER_CLASS),
    ]
    counts = []
    metrics = []
    for scenario, df, features, n_per_class in scenarios:
        valid = df.dropna(subset=features + [base.LABEL_COL]).copy()
        sampled = prob_base.balanced_sample(valid, features, n_per_class)
        _, _, model_metrics = prob_base.select_model(sampled, features, scenario)
        metrics.append(model_metrics)
        counts.append(
            {
                "scenario": scenario,
                "available_positive": int((valid[base.LABEL_COL] == 1).sum()),
                "available_negative": int((valid[base.LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[base.LABEL_COL] == 1).sum()),
                "sampled_negative": int((sampled[base.LABEL_COL] == 0).sum()),
                "test_positive": int((sampled[base.LABEL_COL] == 1).sum() * prob_base.TEST_SIZE),
                "test_negative": int((sampled[base.LABEL_COL] == 0).sum() * prob_base.TEST_SIZE),
                "feature_count": len(features),
                "features": ", ".join(features),
            }
        )

    counts_df = pd.DataFrame(counts)
    metrics_df = pd.concat(metrics, ignore_index=True)
    best = metrics_df.sort_values(["scenario", "balanced_accuracy"], ascending=[True, False]).groupby("scenario").head(1)
    counts_df.to_csv(OUT / "DEM5mのみ_母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics_df.to_csv(OUT / "DEM5mのみ_全モデル評価.csv", index=False, encoding="utf-8-sig")
    best.to_csv(OUT / "DEM5mのみ_最良モデル.csv", index=False, encoding="utf-8-sig")
    plot_metrics(best)
    write_report(counts_df, metrics_df, best)
    print(best.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
