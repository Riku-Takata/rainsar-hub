#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Run Kurume/map7 paddy classification using random 5,000 pixels per class."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import model_map7_paddy_characteristic_5000_report as base


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = base.DETECTION_DIR
OUT_DIR = DETECTION_DIR / "paddy_random_5000_model_report"
SEED = 42
N_PER_CLASS = 5000
LABEL_COL = base.LABEL_COL


def random_sample(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(SEED)
    rows = []
    meta = []
    for label in [1, 0]:
        sub = df[df[LABEL_COL] == label]
        n = min(N_PER_CLASS, len(sub))
        selected_idx = rng.choice(sub.index.to_numpy(), size=n, replace=False)
        rows.append(sub.loc[selected_idx])
        meta.append({"label": int(label), "available_pixels": int(len(sub)), "selected_pixels": int(n)})
    sampled = pd.concat(rows, ignore_index=True).sample(frac=1.0, random_state=SEED).reset_index(drop=True)
    return sampled, pd.DataFrame(meta)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    base.OUT_DIR = OUT_DIR

    full_df = base.build_feature_frame()
    sampled, sample_meta = random_sample(full_df)
    sampled.to_csv(OUT_DIR / "kurume_paddy_random_5000_pixels.csv", index=False, encoding="utf-8-sig")
    sample_meta.to_csv(OUT_DIR / "kurume_paddy_random_5000_sampling_summary.csv", index=False, encoding="utf-8-sig")

    stats = base.summarize_features(full_df, sampled)
    stats.to_csv(OUT_DIR / "kurume_paddy_random_5000_feature_stats.csv", index=False, encoding="utf-8-sig")
    base.plot_profile(stats)

    grid_metrics, _, grid_imp, grid_params = base.run_searches(sampled, "grid")
    random_metrics, _, random_imp, random_params = base.run_searches(sampled, "randomized")
    all_metrics = pd.concat([grid_metrics.assign(search="grid"), random_metrics.assign(search="randomized")], ignore_index=True)
    all_importance = pd.concat([grid_imp, random_imp], ignore_index=True)
    all_metrics.to_csv(OUT_DIR / "kurume_paddy_random_5000_test_metrics.csv", index=False, encoding="utf-8-sig")
    all_importance.to_csv(OUT_DIR / "kurume_paddy_random_5000_feature_importance.csv", index=False, encoding="utf-8-sig")

    previous_random_10000 = DETECTION_DIR / "paddy_model_gridsearch_report" / "gridsearch_test_metrics.csv"
    previous_characteristic_5000 = (
        DETECTION_DIR
        / "paddy_characteristic_5000_model_report"
        / "kurume_paddy_characteristic_5000_test_metrics.csv"
    )
    compare = [all_metrics.assign(dataset="random_5000_each")]
    if previous_random_10000.exists():
        compare.append(pd.read_csv(previous_random_10000, encoding="utf-8-sig").assign(search="previous_grid", dataset="random_10000_each"))
    if previous_characteristic_5000.exists():
        compare.append(pd.read_csv(previous_characteristic_5000, encoding="utf-8-sig").assign(dataset="characteristic_5000_each"))
    pd.concat(compare, ignore_index=True).to_csv(OUT_DIR / "kurume_paddy_random_5000_comparison_metrics.csv", index=False, encoding="utf-8-sig")

    metric_cols = [
        "search",
        "model",
        "selected_threshold",
        "best_cv_balanced_accuracy",
        "precision",
        "recall",
        "specificity",
        "balanced_accuracy",
        "F1",
        "ROC_AUC",
        "TP",
        "FP",
        "FN",
        "TN",
    ]
    full_counts = full_df[LABEL_COL].value_counts().rename(index={0: "non_inundated", 1: "inundated_truth"}).reset_index()
    full_counts.columns = ["label", "pixels"]

    report = []
    report.append("# Kurume/map7 田んぼ内 ランダム5000画素モデルレポート\n")
    report.append("## 抽出方法\n")
    report.append("- 対象: 田んぼマスク内の全有効画素")
    report.append("- 正解浸水域・非浸水域から、それぞれランダムに5,000画素を抽出")
    report.append("- 特徴的画素の選別は行っていない")
    report.append("- train/test = 70/30、StratifiedKFold(3)、評価主指標は balanced_accuracy")
    report.append("")
    report.append("## 元の画素数\n")
    report.append(base.md_table(full_counts))
    report.append("")
    report.append("## 抽出サマリー\n")
    report.append(base.md_table(sample_meta))
    report.append("")
    report.append("## GridSearchCV / RandomizedSearchCV 結果\n")
    report.append(base.md_table(all_metrics[metric_cols].sort_values("balanced_accuracy", ascending=False), 3))
    report.append("")
    report.append("## GridSearchCV 最良パラメータ\n")
    report.append(base.md_table(pd.DataFrame([{"model": k, "best_params": json.dumps(v, ensure_ascii=False)} for k, v in grid_params.items()])))
    report.append("")
    report.append("## RandomizedSearchCV 最良パラメータ\n")
    report.append(base.md_table(pd.DataFrame([{"model": k, "best_params": json.dumps(v, ensure_ascii=False, default=str)} for k, v in random_params.items()])))
    report.append("")
    report.append("## 解釈\n")
    report.append("- ランダム抽出版は、特徴的5000画素版と異なり、田んぼ内のばらつきや曖昧な画素を含む。")
    report.append("- そのため、こちらの精度の方が全田んぼ画素へ適用した場合の実態に近い。")
    (OUT_DIR / "kurume_paddy_random_5000_model_report.md").write_text("\n".join(report), encoding="utf-8")

    print(all_metrics[metric_cols].sort_values("balanced_accuracy", ascending=False).to_string(index=False))
    print(f"saved: {OUT_DIR / 'kurume_paddy_random_5000_model_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
