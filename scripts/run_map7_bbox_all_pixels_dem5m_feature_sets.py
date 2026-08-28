from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

import analyze_map7_bbox_paddy_pixel_polygon_20000 as paddy_base
import run_map7_paddy_dem5m_classification as dem_base


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_all_pixels_dem5m_feature_sets"
SEED = 42
TEST_SIZE = 0.30
PIXEL_PER_CLASS = 20000
LABEL_COL = "label"

SATELLITE_FEATURES = paddy_base.PIXEL_FEATURES
DEM_FEATURES = dem_base.PIXEL_DEM_FEATURES


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


def template_profile() -> dict:
    with rasterio.open(paddy_base.DIFF_RASTERS["0_3h"]) as src:
        return src.profile.copy()


def write_raster(path: Path, data: np.ndarray, dtype: str, nodata) -> None:
    profile = template_profile()
    profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def build_all_pixel_frame() -> tuple[pd.DataFrame, tuple[int, int], np.ndarray]:
    d0 = read_float(paddy_base.DIFF_RASTERS["0_3h"])
    d3 = read_float(paddy_base.DIFF_RASTERS["3_6h"])
    d6 = read_float(paddy_base.DIFF_RASTERS["6_12h"])
    d12 = read_float(paddy_base.DIFF_RASTERS["12_24h"])
    bbox = read_bool(BASE / "bbox_balanced_classification/GIS_bbox_union_mask.tif")
    truth = read_bool(BASE / "map7_inundation_truth_mask.tif")
    valid = bbox & np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12)
    rows, cols = np.where(valid)
    df = pd.DataFrame(
        {
            "row": rows,
            "col": cols,
            LABEL_COL: truth[valid].astype(np.uint8),
            "diff_0_3h": d0[valid],
            "diff_3_6h": d3[valid],
            "diff_6_12h": d6[valid],
            "diff_12_24h": d12[valid],
        }
    )
    return paddy_base.add_pixel_features(df), d0.shape, bbox


def add_dem_features(df: pd.DataFrame, bbox: np.ndarray) -> pd.DataFrame:
    old_out = dem_base.OUT
    dem_base.OUT = OUT
    try:
        elevation, slope, relative, source_stats = dem_base.build_gsi_dem5m_arrays(bbox)
    finally:
        dem_base.OUT = old_out
    source_stats.to_csv(OUT / "GSI_DEM5m_取得状況.csv", index=False, encoding="utf-8-sig")
    out = df.copy()
    r = out["row"].to_numpy(np.int32)
    c = out["col"].to_numpy(np.int32)
    out["elevation_m"] = elevation[r, c]
    out["slope_deg"] = slope[r, c]
    out["relative_elevation_7x7_m"] = relative[r, c]
    return out


def balanced_sample(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    valid = df.dropna(subset=features + [LABEL_COL]).copy()
    pos = valid[valid[LABEL_COL] == 1]
    neg = valid[valid[LABEL_COL] == 0]
    n = min(len(pos), len(neg), PIXEL_PER_CLASS)
    sampled = pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    )
    return sampled.sample(frac=1, random_state=SEED)


def models() -> dict:
    return {
        "RandomForest": RandomForestClassifier(
            n_estimators=180,
            max_features="sqrt",
            min_samples_leaf=10,
            random_state=SEED,
            n_jobs=1,
        ),
        "XGBoost": XGBClassifier(
            objective="binary:logistic",
            eval_metric="logloss",
            n_estimators=140,
            max_depth=3,
            learning_rate=0.05,
            min_child_weight=3,
            subsample=0.85,
            colsample_bytree=0.85,
            random_state=SEED,
            n_jobs=1,
        ),
    }


def threshold_scan(y_true: np.ndarray, prob: np.ndarray) -> dict:
    auc = roc_auc_score(y_true, prob)
    rows = []
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.uint8)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        rows.append(
            {
                "threshold": float(threshold),
                "balanced_accuracy": balanced_accuracy_score(y_true, pred),
                "precision": precision_score(y_true, pred, zero_division=0),
                "recall": recall_score(y_true, pred, zero_division=0),
                "specificity": tn / (tn + fp) if tn + fp else 0.0,
                "F1": f1_score(y_true, pred, zero_division=0),
                "ROC_AUC": auc,
                "TP": int(tp),
                "FP": int(fp),
                "FN": int(fn),
                "TN": int(tn),
            }
        )
    return pd.DataFrame(rows).sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()


def evaluate_and_predict(df: pd.DataFrame, features: list[str], feature_set: str, shape: tuple[int, int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    sampled = balanced_sample(df, features)
    x = sampled[features].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    metric_rows = []
    best_model = None
    best_info = None
    for model_name, model in models().items():
        model.fit(x_train, y_train)
        prob = model.predict_proba(x_test)[:, 1]
        info = threshold_scan(y_test, prob)
        info.update({"feature_set": feature_set, "model": model_name})
        metric_rows.append(info)
        if best_info is None or info["balanced_accuracy"] > best_info["balanced_accuracy"]:
            best_info = info
            best_model = model

    assert best_model is not None and best_info is not None
    final_model = models()[best_info["model"]]
    final_model.fit(x, y)
    valid_df = df.dropna(subset=features + [LABEL_COL]).copy()
    probability = final_model.predict_proba(valid_df[features].to_numpy(np.float32))[:, 1]
    valid_df["inundation_probability"] = probability
    valid_df["predicted_inundated"] = (probability >= float(best_info["threshold"])).astype(np.uint8)

    prob_arr = np.full(shape, np.nan, dtype=np.float32)
    pred_arr = np.zeros(shape, dtype=np.uint8)
    rows = valid_df["row"].to_numpy(np.int32)
    cols = valid_df["col"].to_numpy(np.int32)
    prob_arr[rows, cols] = valid_df["inundation_probability"].to_numpy(np.float32)
    pred_arr[rows, cols] = valid_df["predicted_inundated"].to_numpy(np.uint8)
    safe = feature_set.replace("+", "_plus_")
    write_raster(OUT / f"GIS_bbox全画素_{safe}_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / f"GIS_bbox全画素_{safe}_浸水判定.tif", pred_arr, "uint8", 0)
    valid_df[["row", "col", LABEL_COL, "inundation_probability", "predicted_inundated"]].to_csv(
        OUT / f"bbox全画素_{safe}_浸水確率一覧.csv",
        index=False,
        encoding="utf-8-sig",
    )

    available = df.dropna(subset=features + [LABEL_COL])
    counts = {
        "feature_set": feature_set,
        "available_positive": int((available[LABEL_COL] == 1).sum()),
        "available_negative": int((available[LABEL_COL] == 0).sum()),
        "sampled_positive": int((sampled[LABEL_COL] == 1).sum()),
        "sampled_negative": int((sampled[LABEL_COL] == 0).sum()),
        "test_positive": int((sampled[LABEL_COL] == 1).sum() * TEST_SIZE),
        "test_negative": int((sampled[LABEL_COL] == 0).sum() * TEST_SIZE),
        "feature_count": len(features),
        "best_model": best_info["model"],
        "best_threshold": best_info["threshold"],
        "all_units": len(valid_df),
        "truth_inundated": int(valid_df[LABEL_COL].sum()),
        "predicted_inundated": int(valid_df["predicted_inundated"].sum()),
    }
    metrics = pd.DataFrame(metric_rows)
    counts_df = pd.DataFrame([counts])
    return metrics, counts_df


def plot_comparison(best: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(best))
    width = 0.18
    for i, col in enumerate(["balanced_accuracy", "precision", "recall", "ROC_AUC"]):
        ax.bar(x + (i - 1.5) * width, best[col], width, label=col)
    ax.set_xticks(x)
    ax.set_xticklabels(best["feature_set"])
    ax.set_ylim(0.45, 1.0)
    ax.set_title("bbox内全画素: 特徴量セット別の浸水判別精度")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.16))
    plt.tight_layout()
    plt.savefig(OUT / "図_bbox全画素_特徴量セット別精度比較.png", dpi=220, bbox_inches="tight")
    plt.close()


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


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame) -> None:
    best = metrics.sort_values(["feature_set", "balanced_accuracy"], ascending=[True, False]).groupby("feature_set").head(1)
    lines = [
        "# bbox内全画素における特徴量セット別の浸水判別",
        "",
        "## 条件",
        "",
        "- 田んぼ・道路などの地物マスクは使用せず、bbox内の全有効画素を対象にしました。",
        "- 正解浸水域と非浸水域を同数抽出して学習・検証しました。",
        "- 比較した特徴量セットは、`satellite_only`, `dem5m_only`, `satellite_plus_dem5m` です。",
        "- モデルはRandomForestとXGBoostを比較し、検証データ上でしきい値を最適化しました。",
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
        "## GIS出力",
        "",
        "- `GIS_bbox全画素_satellite_only_浸水確率.tif`",
        "- `GIS_bbox全画素_dem5m_only_浸水確率.tif`",
        "- `GIS_bbox全画素_satellite_plus_dem5m_浸水確率.tif`",
        "- 各特徴量セットについて `*_浸水判定.tif` も出力しています。",
        "",
    ]
    (OUT / "bbox全画素_特徴量セット別_浸水判別レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    df, shape, bbox = build_all_pixel_frame()
    df = add_dem_features(df, bbox)
    feature_sets = {
        "satellite_only": SATELLITE_FEATURES,
        "dem5m_only": DEM_FEATURES,
        "satellite_plus_dem5m": SATELLITE_FEATURES + DEM_FEATURES,
    }
    metrics = []
    counts = []
    for name, features in feature_sets.items():
        m, c = evaluate_and_predict(df, features, name, shape)
        metrics.append(m)
        counts.append(c)
    metrics_df = pd.concat(metrics, ignore_index=True)
    counts_df = pd.concat(counts, ignore_index=True)
    metrics_df.to_csv(OUT / "bbox全画素_特徴量セット別_全モデル評価.csv", index=False, encoding="utf-8-sig")
    counts_df.to_csv(OUT / "bbox全画素_特徴量セット別_母数と抽出数.csv", index=False, encoding="utf-8-sig")
    best = metrics_df.sort_values(["feature_set", "balanced_accuracy"], ascending=[True, False]).groupby("feature_set").head(1)
    best.to_csv(OUT / "bbox全画素_特徴量セット別_最良モデル.csv", index=False, encoding="utf-8-sig")
    plot_comparison(best)
    write_report(counts_df, metrics_df)
    print(best.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
