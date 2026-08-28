#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Apply the Kurume/map7 characteristic-5000 paddy model to all paddy pixels."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = (
    ROOT
    / "output"
    / "gsi_h30_geojson_s1"
    / "map7_rain_s1"
    / "kurume_inundation_analysis"
    / "map7_detection_test"
)
MODEL_DIR = DETECTION_DIR / "paddy_characteristic_5000_model_report"
OUT_DIR = MODEL_DIR / "applied_to_all_paddy_pixels"
SAMPLED_CSV = MODEL_DIR / "kurume_paddy_characteristic_5000_pixels.csv"
BEST_PARAMS_JSON = MODEL_DIR / "randomized_best_params.json"

SEED = 42
LABEL_COL = "正解浸水域"
THRESHOLD_FROM_5000 = 0.37

FEATURE_COLS = [
    "diff_0_3h",
    "diff_3_6h",
    "diff_6_12h",
    "diff_12_24h",
    "early_mean_0_6h",
    "late_mean_6_24h",
    "early_minus_late",
    "drop_0_3_to_3_6",
    "drop_3_6_to_6_12",
    "recovery_6_12_to_12_24",
    "drop_0_3_to_6_12",
    "change_0_3_to_6_12",
    "profile_mean",
    "profile_std",
    "profile_range",
    "negative_bin_count",
    "monotonic_drop_score",
]


def read_float(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def read_bool(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1) > 0


def build_feature_frame() -> tuple[pd.DataFrame, tuple[int, int]]:
    d0 = read_float(DETECTION_DIR / "map7_mean_diff_0_3h.tif")
    d3 = read_float(DETECTION_DIR / "map7_mean_diff_3_6h.tif")
    d6 = read_float(DETECTION_DIR / "map7_mean_diff_6_12h.tif")
    d12 = read_float(DETECTION_DIR / "map7_mean_diff_12_24h.tif")
    truth = read_bool(DETECTION_DIR / "map7_inundation_truth_mask.tif")
    paddy = read_bool(DETECTION_DIR / "landmask_filter" / "map7_paddy_mask.tif")
    valid = np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12) & paddy

    row, col = np.where(valid)
    df = pd.DataFrame(
        {
            "row": row,
            "col": col,
            LABEL_COL: truth[valid].astype(np.uint8),
            "diff_0_3h": d0[valid],
            "diff_3_6h": d3[valid],
            "diff_6_12h": d6[valid],
            "diff_12_24h": d12[valid],
        }
    )
    df["early_mean_0_6h"] = (df["diff_0_3h"] + df["diff_3_6h"]) / 2.0
    df["late_mean_6_24h"] = (df["diff_6_12h"] + df["diff_12_24h"]) / 2.0
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df["diff_0_3h"] - df["diff_3_6h"]
    df["drop_3_6_to_6_12"] = df["diff_3_6h"] - df["diff_6_12h"]
    df["recovery_6_12_to_12_24"] = df["diff_12_24h"] - df["diff_6_12h"]
    df["drop_0_3_to_6_12"] = df["diff_0_3h"] - df["diff_6_12h"]
    df["change_0_3_to_6_12"] = df["diff_6_12h"] - df["diff_0_3h"]
    profile = df[["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]].to_numpy(np.float32)
    df["profile_mean"] = profile.mean(axis=1)
    df["profile_std"] = profile.std(axis=1)
    df["profile_range"] = profile.max(axis=1) - profile.min(axis=1)
    df["negative_bin_count"] = (profile < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h"] >= df["diff_3_6h"]).astype(int)
        + (df["diff_3_6h"] >= df["diff_6_12h"]).astype(int)
        + (df["diff_6_12h"] <= df["diff_12_24h"]).astype(int)
    )
    return df, d0.shape


def metric_row(name: str, y_true: np.ndarray, pred: np.ndarray, score: np.ndarray) -> dict:
    tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
    return {
        "model": name,
        "TP": int(tp),
        "FP": int(fp),
        "FN": int(fn),
        "TN": int(tn),
        "accuracy": accuracy_score(y_true, pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, pred),
        "precision": precision_score(y_true, pred, zero_division=0),
        "recall": recall_score(y_true, pred, zero_division=0),
        "specificity": tn / (tn + fp) if tn + fp else 0.0,
        "F1": f1_score(y_true, pred, zero_division=0),
        "ROC_AUC": roc_auc_score(y_true, score),
        "detected_pixels": int(np.sum(pred == 1)),
        "truth_pixels": int(np.sum(y_true == 1)),
        "non_truth_pixels": int(np.sum(y_true == 0)),
    }


def write_raster(path: Path, data: np.ndarray, dtype: str, nodata) -> None:
    with rasterio.open(DETECTION_DIR / "map7_mean_diff_0_3h.tif") as src:
        profile = src.profile.copy()
    profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def md_table(df: pd.DataFrame, digits: int = 3) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.{digits}f}")
    shown = shown.fillna("")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sampled = pd.read_csv(SAMPLED_CSV, encoding="utf-8-sig")
    params = json.loads(BEST_PARAMS_JSON.read_text(encoding="utf-8"))["xgboost"]

    model = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=SEED,
        n_jobs=1,
        **params,
    )
    model.fit(sampled[FEATURE_COLS].to_numpy(np.float32), sampled[LABEL_COL].to_numpy(np.int32))

    all_df, shape = build_feature_frame()
    prob = model.predict_proba(all_df[FEATURE_COLS].to_numpy(np.float32))[:, 1]
    y = all_df[LABEL_COL].to_numpy(np.int32)

    rows = []
    for threshold in [THRESHOLD_FROM_5000, 0.5]:
        pred = (prob >= threshold).astype(np.int32)
        row = metric_row(f"xgboost_characteristic5000_threshold_{threshold:.2f}", y, pred, prob)
        row["threshold"] = threshold
        rows.append(row)
        all_df[f"pred_{threshold:.2f}"] = pred

    scan_rows = []
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.int32)
        row = metric_row("xgboost_characteristic5000", y, pred, prob)
        row["threshold"] = float(threshold)
        scan_rows.append(row)
    scan = pd.DataFrame(scan_rows)
    best = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
    rows.append(metric_row(f"xgboost_characteristic5000_best_all_paddy_threshold_{best['threshold']:.3f}", y, (prob >= best["threshold"]).astype(np.int32), prob) | {"threshold": best["threshold"]})

    metrics = pd.DataFrame(rows).sort_values("balanced_accuracy", ascending=False)
    metrics.to_csv(OUT_DIR / "all_paddy_application_metrics.csv", index=False, encoding="utf-8-sig")
    scan.to_csv(OUT_DIR / "all_paddy_threshold_scan.csv", index=False, encoding="utf-8-sig")

    rng = np.random.default_rng(SEED)
    pos_idx = np.where(y == 1)[0]
    neg_idx = np.where(y == 0)[0]
    n_balanced = min(len(pos_idx), len(neg_idx))
    balanced_idx = np.concatenate(
        [
            rng.choice(pos_idx, n_balanced, replace=False),
            rng.choice(neg_idx, n_balanced, replace=False),
        ]
    )
    rng.shuffle(balanced_idx)
    y_bal = y[balanced_idx]
    prob_bal = prob[balanced_idx]
    balanced_rows = []
    for threshold in [THRESHOLD_FROM_5000, 0.5]:
        pred = (prob_bal >= threshold).astype(np.int32)
        row = metric_row(f"balanced_eval_threshold_{threshold:.2f}", y_bal, pred, prob_bal)
        row["threshold"] = threshold
        balanced_rows.append(row)
    balanced_scan_rows = []
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob_bal >= threshold).astype(np.int32)
        row = metric_row("balanced_eval_threshold_scan", y_bal, pred, prob_bal)
        row["threshold"] = float(threshold)
        balanced_scan_rows.append(row)
    balanced_scan = pd.DataFrame(balanced_scan_rows)
    balanced_best = balanced_scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
    balanced_rows.append(
        metric_row(
            f"balanced_eval_best_threshold_{balanced_best['threshold']:.3f}",
            y_bal,
            (prob_bal >= balanced_best["threshold"]).astype(np.int32),
            prob_bal,
        )
        | {"threshold": balanced_best["threshold"]}
    )
    balanced_metrics = pd.DataFrame(balanced_rows).sort_values("balanced_accuracy", ascending=False)
    balanced_metrics.to_csv(OUT_DIR / "balanced_paddy_application_metrics.csv", index=False, encoding="utf-8-sig")
    balanced_scan.to_csv(OUT_DIR / "balanced_paddy_threshold_scan.csv", index=False, encoding="utf-8-sig")

    all_df["xgboost_probability"] = prob
    all_df.to_csv(OUT_DIR / "all_paddy_pixel_predictions.csv", index=False, encoding="utf-8-sig")

    prob_raster = np.full(shape, np.nan, dtype=np.float32)
    mask037 = np.zeros(shape, dtype=np.uint8)
    mask050 = np.zeros(shape, dtype=np.uint8)
    truth_raster = np.zeros(shape, dtype=np.uint8)
    prob_raster[all_df["row"].to_numpy(), all_df["col"].to_numpy()] = prob.astype(np.float32)
    mask037[all_df["row"].to_numpy(), all_df["col"].to_numpy()] = (prob >= THRESHOLD_FROM_5000).astype(np.uint8)
    mask050[all_df["row"].to_numpy(), all_df["col"].to_numpy()] = (prob >= 0.5).astype(np.uint8)
    truth_raster[all_df["row"].to_numpy(), all_df["col"].to_numpy()] = y.astype(np.uint8)

    write_raster(OUT_DIR / "all_paddy_xgboost_probability.tif", prob_raster, "float32", np.nan)
    write_raster(OUT_DIR / "all_paddy_detection_threshold_0p37.tif", mask037, "uint8", 0)
    write_raster(OUT_DIR / "all_paddy_detection_threshold_0p50.tif", mask050, "uint8", 0)
    write_raster(OUT_DIR / "all_paddy_truth_mask.tif", truth_raster, "uint8", 0)

    report = []
    report.append("# 特徴的5000画素モデルの全田んぼ画素適用結果\n")
    report.append("## 方法\n")
    report.append("- 学習: 田んぼ内の特徴的5000画素/クラスで学習した RandomizedSearchCV の XGBoost 最良パラメータ")
    report.append(f"- 主判定閾値: 5000画素検証で最良だった `{THRESHOLD_FROM_5000}`")
    report.append("- 適用対象: map7評価範囲の田んぼ内全有効画素")
    report.append("- 注意: 学習画素も適用対象に含まれるため、純粋な未知データ評価ではありません。")
    report.append("")
    report.append("## 評価結果\n")
    report.append("### 全田んぼ画素のまま評価\n")
    report.append(md_table(metrics))
    report.append("")
    report.append("### 浸水域と非浸水域の画素数を同数にした評価\n")
    report.append(f"- 浸水域: {n_balanced:,} 画素")
    report.append(f"- 非浸水域: {n_balanced:,} 画素")
    report.append(md_table(balanced_metrics))
    report.append("")
    report.append("## 出力\n")
    report.append("- `all_paddy_xgboost_probability.tif`: 浸水確率")
    report.append("- `all_paddy_detection_threshold_0p37.tif`: threshold=0.37 の判定")
    report.append("- `all_paddy_detection_threshold_0p50.tif`: threshold=0.50 の判定")
    report.append("- `all_paddy_pixel_predictions.csv`: 画素ごとの特徴量・確率・判定")
    report.append("- `all_paddy_threshold_scan.csv`: 全田んぼ画素での閾値別評価")
    report.append("- `balanced_paddy_application_metrics.csv`: 浸水域・非浸水域を同数にした評価")
    report.append("- `balanced_paddy_threshold_scan.csv`: 同数評価での閾値別評価")
    (OUT_DIR / "all_paddy_application_report.md").write_text("\n".join(report), encoding="utf-8")

    print(metrics.to_string(index=False))
    print(f"saved: {OUT_DIR / 'all_paddy_application_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
