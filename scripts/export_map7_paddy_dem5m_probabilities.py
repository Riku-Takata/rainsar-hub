from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize
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

import analyze_map7_bbox_paddy_pixel_polygon_20000 as base
import run_map7_paddy_dem5m_classification as dem_base


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
DEM_DIR = BASE / "bbox_paddy_dem5m_classification"
OUT = BASE / "bbox_paddy_dem5m_probabilities"
SEED = 42
TEST_SIZE = 0.30
PIXEL_PER_CLASS = 20000
POLYGON_PER_CLASS = 20000
LABEL_COL = "label"


def read_float(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
    if nodata is not None:
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def template_profile() -> dict:
    with rasterio.open(base.DIFF_RASTERS["0_3h"]) as src:
        return src.profile.copy()


def write_raster(path: Path, data: np.ndarray, dtype: str, nodata) -> None:
    profile = template_profile()
    profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def balanced_sample(df: pd.DataFrame, features: list[str], n_per_class: int) -> pd.DataFrame:
    valid = df.dropna(subset=features + [LABEL_COL]).copy()
    pos = valid[valid[LABEL_COL] == 1]
    neg = valid[valid[LABEL_COL] == 0]
    n = min(len(pos), len(neg), n_per_class)
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
    rows = []
    auc = roc_auc_score(y_true, prob)
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


def select_model(sampled: pd.DataFrame, features: list[str], scenario: str) -> tuple[object, dict, pd.DataFrame]:
    x = sampled[features].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    rows = []
    best_model = None
    best_score = -np.inf
    best_info: dict | None = None
    for name, model in models().items():
        model.fit(x_train, y_train)
        prob = model.predict_proba(x_test)[:, 1]
        info = threshold_scan(y_test, prob)
        info.update({"scenario": scenario, "model": name})
        rows.append(info)
        if info["balanced_accuracy"] > best_score:
            best_score = float(info["balanced_accuracy"])
            best_model = model
            best_info = info
    assert best_model is not None and best_info is not None

    final_model = models()[best_info["model"]]
    final_model.fit(x, y)
    return final_model, best_info, pd.DataFrame(rows)


def load_frames() -> tuple[pd.DataFrame, pd.DataFrame, np.ndarray, tuple[int, int], gpd.GeoDataFrame]:
    base.OUT = BASE / "bbox_paddy_pixel_polygon_20000"
    pixel_df, shape = base.build_paddy_pixel_frame()
    polygon_df, polygon_ids, paddy_gdf = base.build_paddy_polygon_frame(shape)

    elevation = read_float(DEM_DIR / "GIS_GSI_DEM5m_elevation_m.tif")
    slope = read_float(DEM_DIR / "GIS_GSI_DEM5m_slope_deg.tif")
    relative = read_float(DEM_DIR / "GIS_GSI_DEM5m_relative_elevation_7x7_m.tif")
    pixel_df = dem_base.add_dem_to_pixel_frame(pixel_df, elevation, slope, relative)
    polygon_df = dem_base.add_dem_to_polygon_frame(polygon_df, polygon_ids, elevation, slope, relative)
    return pixel_df, polygon_df, polygon_ids, shape, paddy_gdf


def export_pixel_probability(df: pd.DataFrame, model, features: list[str], threshold: float, shape: tuple[int, int]) -> pd.DataFrame:
    pred_df = df.dropna(subset=features + [LABEL_COL]).copy()
    prob = model.predict_proba(pred_df[features].to_numpy(np.float32))[:, 1]
    pred_df["dem5m_inundation_probability"] = prob
    pred_df["dem5m_predicted_inundated"] = (prob >= threshold).astype(np.uint8)

    prob_arr = np.full(shape, np.nan, dtype=np.float32)
    pred_arr = np.zeros(shape, dtype=np.uint8)
    rows = pred_df["row"].to_numpy(np.int32)
    cols = pred_df["col"].to_numpy(np.int32)
    prob_arr[rows, cols] = pred_df["dem5m_inundation_probability"].to_numpy(np.float32)
    pred_arr[rows, cols] = pred_df["dem5m_predicted_inundated"].to_numpy(np.uint8)
    write_raster(OUT / "GIS_田んぼ画素_DEM5m浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_田んぼ画素_DEM5m浸水判定.tif", pred_arr, "uint8", 0)
    pred_df[["row", "col", LABEL_COL, "dem5m_inundation_probability", "dem5m_predicted_inundated"]].to_csv(
        OUT / "GIS_田んぼ画素_DEM5m浸水確率一覧.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return pred_df


def export_polygon_probability(df: pd.DataFrame, model, features: list[str], threshold: float, gdf: gpd.GeoDataFrame, shape: tuple[int, int]) -> pd.DataFrame:
    pred_df = df.dropna(subset=features + [LABEL_COL]).copy()
    prob = model.predict_proba(pred_df[features].to_numpy(np.float32))[:, 1]
    pred_df["dem5m_inundation_probability"] = prob
    pred_df["dem5m_predicted_inundated"] = (prob >= threshold).astype(np.uint8)

    attrs = pred_df[
        [
            "feature_seq_id",
            LABEL_COL,
            "truth_pixel_count",
            "sentinel_pixel_count",
            "truth_pixel_ratio",
            "dem5m_inundation_probability",
            "dem5m_predicted_inundated",
        ]
    ].copy()
    merged = gdf.merge(attrs, on="feature_seq_id", how="inner")
    selected = merged[merged["dem5m_predicted_inundated"] == 1].copy()
    merged.to_file(OUT / "GIS_田んぼ筆ポリゴン_DEM5m全確率.geojson", driver="GeoJSON")
    selected.to_file(OUT / "GIS_田んぼ筆ポリゴン_DEM5m浸水判定.geojson", driver="GeoJSON")
    pred_df.to_csv(OUT / "GIS_田んぼ筆ポリゴン_DEM5m浸水確率一覧.csv", index=False, encoding="utf-8-sig")

    prob_shapes = [(geom, float(p)) for geom, p in zip(merged.geometry, merged["dem5m_inundation_probability"]) if geom is not None and not geom.is_empty]
    pred_shapes = [(geom, int(p)) for geom, p in zip(merged.geometry, merged["dem5m_predicted_inundated"]) if geom is not None and not geom.is_empty]
    profile = template_profile()
    prob_arr = rasterize(prob_shapes, out_shape=shape, transform=profile["transform"], fill=np.nan, dtype="float32", all_touched=False)
    pred_arr = rasterize(pred_shapes, out_shape=shape, transform=profile["transform"], fill=0, dtype="uint8", all_touched=False)
    write_raster(OUT / "GIS_田んぼ筆ポリゴン_DEM5m浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_田んぼ筆ポリゴン_DEM5m浸水判定.tif", pred_arr, "uint8", 0)
    return pred_df


def summarize_predictions(pixel_pred: pd.DataFrame, polygon_pred: pd.DataFrame, pixel_info: dict, polygon_info: dict) -> pd.DataFrame:
    rows = []
    for name, df, info in [
        ("田んぼ画素", pixel_pred, pixel_info),
        ("田んぼ筆ポリゴン", polygon_pred, polygon_info),
    ]:
        rows.append(
            {
                "scenario": name,
                "selected_model": info["model"],
                "threshold": info["threshold"],
                "test_balanced_accuracy": info["balanced_accuracy"],
                "test_precision": info["precision"],
                "test_recall": info["recall"],
                "test_specificity": info["specificity"],
                "test_ROC_AUC": info["ROC_AUC"],
                "all_units": len(df),
                "truth_inundated": int(df[LABEL_COL].sum()),
                "predicted_inundated": int(df["dem5m_predicted_inundated"].sum()),
                "probability_mean": float(df["dem5m_inundation_probability"].mean()),
                "probability_median": float(df["dem5m_inundation_probability"].median()),
            }
        )
    summary = pd.DataFrame(rows)
    summary.to_csv(OUT / "DEM5m浸水確率_要約.csv", index=False, encoding="utf-8-sig")
    return summary


def plot_probability_hist(pixel_pred: pd.DataFrame, polygon_pred: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), sharey=True)
    for ax, title, df in [
        (axes[0], "田んぼ画素", pixel_pred),
        (axes[1], "田んぼ筆ポリゴン", polygon_pred),
    ]:
        for label, color, name in [(1, "#e45756", "正解浸水"), (0, "#4c78a8", "非浸水")]:
            vals = df.loc[df[LABEL_COL] == label, "dem5m_inundation_probability"].to_numpy()
            ax.hist(vals, bins=np.linspace(0, 1, 41), alpha=0.55, color=color, label=name)
        ax.set_title(title)
        ax.set_xlabel("DEM5mを加味した浸水確率")
        ax.grid(alpha=0.25)
        ax.legend()
    axes[0].set_ylabel("件数")
    plt.tight_layout()
    plt.savefig(OUT / "図_DEM5m浸水確率分布.png", dpi=220, bbox_inches="tight")
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


def write_report(summary: pd.DataFrame, model_metrics: pd.DataFrame) -> None:
    lines = [
        "# DEM5mを加味した浸水確率の再算出",
        "",
        "## 方法",
        "",
        "- 既存の後方散乱強度差分特徴量に、GSI DEM 5m由来の標高・傾斜・相対標高を追加しました。",
        "- 田んぼ画素単位、田んぼ筆ポリゴン単位の2種類でモデルを作成しました。",
        "- RandomForestとXGBoostを比較し、検証データのBalanced Accuracyが高いモデルを採用しました。",
        "- 採用モデルを同じバランス抽出データ全体で再学習し、bbox内の全対象へ浸水確率を出力しました。",
        "",
        "## 要約",
        "",
        md_table(summary),
        "",
        "## 検証データ上のモデル評価",
        "",
        md_table(model_metrics),
        "",
        "## 主な出力",
        "",
        "- `GIS_田んぼ画素_DEM5m浸水確率.tif`",
        "- `GIS_田んぼ画素_DEM5m浸水判定.tif`",
        "- `GIS_田んぼ筆ポリゴン_DEM5m浸水確率.tif`",
        "- `GIS_田んぼ筆ポリゴン_DEM5m浸水判定.tif`",
        "- `GIS_田んぼ筆ポリゴン_DEM5m全確率.geojson`",
        "- `GIS_田んぼ筆ポリゴン_DEM5m浸水判定.geojson`",
        "",
    ]
    (OUT / "DEM5m浸水確率_再算出レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pixel_df, polygon_df, _, shape, paddy_gdf = load_frames()
    pixel_features = base.PIXEL_FEATURES + dem_base.PIXEL_DEM_FEATURES
    polygon_features = base.POLYGON_FEATURES + dem_base.POLYGON_DEM_FEATURES

    pixel_sample = balanced_sample(pixel_df, pixel_features, PIXEL_PER_CLASS)
    polygon_sample = balanced_sample(polygon_df, polygon_features, POLYGON_PER_CLASS)
    pixel_model, pixel_info, pixel_metrics = select_model(pixel_sample, pixel_features, "田んぼ画素")
    polygon_model, polygon_info, polygon_metrics = select_model(polygon_sample, polygon_features, "田んぼ筆ポリゴン")

    pixel_pred = export_pixel_probability(pixel_df, pixel_model, pixel_features, float(pixel_info["threshold"]), shape)
    polygon_pred = export_polygon_probability(polygon_df, polygon_model, polygon_features, float(polygon_info["threshold"]), paddy_gdf, shape)

    model_metrics = pd.concat([pixel_metrics, polygon_metrics], ignore_index=True)
    model_metrics.to_csv(OUT / "DEM5m浸水確率_モデル評価.csv", index=False, encoding="utf-8-sig")
    summary = summarize_predictions(pixel_pred, polygon_pred, pixel_info, polygon_info)
    plot_probability_hist(pixel_pred, polygon_pred)
    write_report(summary, model_metrics)
    print(summary.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
