from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
LAND = BASE / "landmask_filter"
OUT = BASE / "bbox_paddy_pixel_polygon_20000"
BBOX_MASK = BASE / "bbox_balanced_classification/GIS_bbox_union_mask.tif"
PADDY_GEOJSON = ROOT / "output/gsi_h30_geojson_s1/map7_land_polygons/map7_fude_paddy_polygons_from_db.geojson"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 5
PIXEL_PER_CLASS = 20000
POLYGON_PER_CLASS = 20000
LABEL_COL = "label"

DIFF_RASTERS = {
    "0_3h": BASE / "map7_mean_diff_0_3h.tif",
    "3_6h": BASE / "map7_mean_diff_3_6h.tif",
    "6_12h": BASE / "map7_mean_diff_6_12h.tif",
    "12_24h": BASE / "map7_mean_diff_12_24h.tif",
}

PIXEL_FEATURES = [
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

POLYGON_FEATURES = [
    "sentinel_pixel_count",
    "diff_0_3h_mean",
    "diff_3_6h_mean",
    "diff_6_12h_mean",
    "diff_12_24h_mean",
    "diff_0_3h_std",
    "diff_3_6h_std",
    "diff_6_12h_std",
    "diff_12_24h_std",
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


def setup_font() -> font_manager.FontProperties:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    else:
        prop = font_manager.FontProperties()
    plt.rcParams["axes.unicode_minus"] = False
    return prop


FONT = setup_font()


def template_profile() -> dict:
    with rasterio.open(DIFF_RASTERS["0_3h"]) as src:
        return src.profile.copy()


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


def write_raster(path: Path, data: np.ndarray, dtype: str, nodata) -> None:
    profile = template_profile()
    profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def load_features(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8")).get("features", [])


def add_pixel_features(df: pd.DataFrame) -> pd.DataFrame:
    df["early_mean_0_6h"] = (df["diff_0_3h"] + df["diff_3_6h"]) / 2
    df["late_mean_6_24h"] = (df["diff_6_12h"] + df["diff_12_24h"]) / 2
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
    return df


def build_paddy_pixel_frame() -> tuple[pd.DataFrame, tuple[int, int]]:
    d0 = read_float(DIFF_RASTERS["0_3h"])
    d3 = read_float(DIFF_RASTERS["3_6h"])
    d6 = read_float(DIFF_RASTERS["6_12h"])
    d12 = read_float(DIFF_RASTERS["12_24h"])
    bbox = read_bool(BBOX_MASK)
    truth = read_bool(BASE / "map7_inundation_truth_mask.tif")
    paddy = read_bool(LAND / "map7_paddy_mask.tif")
    valid = bbox & paddy & np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12)
    rows, cols = np.where(valid)
    df = pd.DataFrame(
        {
            "scenario": "田んぼ画素",
            "row": rows,
            "col": cols,
            LABEL_COL: truth[valid].astype(np.uint8),
            "diff_0_3h": d0[valid],
            "diff_3_6h": d3[valid],
            "diff_6_12h": d6[valid],
            "diff_12_24h": d12[valid],
        }
    )
    return add_pixel_features(df), d0.shape


def rasterize_polygon_ids(features: list[dict], shape: tuple[int, int]) -> np.ndarray:
    profile = template_profile()
    return rasterize(
        ((f["geometry"], idx) for idx, f in enumerate(features, start=1) if f.get("geometry")),
        out_shape=shape,
        transform=profile["transform"],
        fill=0,
        dtype="int32",
        all_touched=False,
    )


def grouped_stats(ids: np.ndarray, values: np.ndarray, valid: np.ndarray, name: str) -> pd.DataFrame:
    mask = valid & (ids > 0) & np.isfinite(values)
    flat_ids = ids[mask].astype(np.int32)
    flat_values = values[mask].astype(np.float32)
    order = np.argsort(flat_ids)
    flat_ids = flat_ids[order]
    flat_values = flat_values[order]
    unique_ids, starts, counts = np.unique(flat_ids, return_index=True, return_counts=True)
    rows = []
    for uid, start, count in zip(unique_ids, starts, counts):
        vals = flat_values[start : start + count]
        rows.append(
            {
                "feature_seq_id": int(uid),
                f"diff_{name}_valid_pixel_count": int(count),
                f"diff_{name}_mean": float(np.mean(vals)),
                f"diff_{name}_median": float(np.median(vals)),
                f"diff_{name}_std": float(np.std(vals)),
                f"diff_{name}_p25": float(np.percentile(vals, 25)),
                f"diff_{name}_p75": float(np.percentile(vals, 75)),
            }
        )
    return pd.DataFrame(rows)


def build_paddy_polygon_frame(shape: tuple[int, int]) -> tuple[pd.DataFrame, np.ndarray, gpd.GeoDataFrame]:
    features = load_features(PADDY_GEOJSON)
    ids = rasterize_polygon_ids(features, shape)
    bbox = read_bool(BBOX_MASK)
    truth = read_bool(BASE / "map7_inundation_truth_mask.tif")
    diffs = {name: read_float(path) for name, path in DIFF_RASTERS.items()}
    valid = bbox & (ids > 0)
    for arr in diffs.values():
        valid &= np.isfinite(arr)

    base_ids, pixel_counts = np.unique(ids[valid], return_counts=True)
    df = pd.DataFrame({"feature_seq_id": base_ids.astype(np.int32), "sentinel_pixel_count": pixel_counts.astype(np.int32)})
    truth_ids, truth_counts = np.unique(ids[valid & truth], return_counts=True)
    truth_df = pd.DataFrame({"feature_seq_id": truth_ids.astype(np.int32), "truth_pixel_count": truth_counts.astype(np.int32)})
    df = df.merge(truth_df, on="feature_seq_id", how="left")
    df["truth_pixel_count"] = df["truth_pixel_count"].fillna(0).astype(np.int32)
    df["truth_pixel_ratio"] = df["truth_pixel_count"] / df["sentinel_pixel_count"]
    df[LABEL_COL] = (df["truth_pixel_count"] > 0).astype(np.uint8)

    props = []
    for uid in df["feature_seq_id"].to_numpy():
        prop = features[int(uid) - 1].get("properties", {})
        props.append(
            {
                "feature_seq_id": int(uid),
                "polygon_uuid": prop.get("polygon_uuid"),
                "local_government_cd": prop.get("local_government_cd"),
                "pref_id": prop.get("pref_id"),
                "land_type": prop.get("land_type"),
            }
        )
    df = pd.DataFrame(props).merge(df, on="feature_seq_id", how="right")

    for name, arr in diffs.items():
        df = df.merge(grouped_stats(ids, arr, valid, name), on="feature_seq_id", how="left")

    df["early_mean_0_6h"] = (df["diff_0_3h_mean"] + df["diff_3_6h_mean"]) / 2
    df["late_mean_6_24h"] = (df["diff_6_12h_mean"] + df["diff_12_24h_mean"]) / 2
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df["diff_0_3h_mean"] - df["diff_3_6h_mean"]
    df["drop_3_6_to_6_12"] = df["diff_3_6h_mean"] - df["diff_6_12h_mean"]
    df["recovery_6_12_to_12_24"] = df["diff_12_24h_mean"] - df["diff_6_12h_mean"]
    df["drop_0_3_to_6_12"] = df["diff_0_3h_mean"] - df["diff_6_12h_mean"]
    df["change_0_3_to_6_12"] = df["diff_6_12h_mean"] - df["diff_0_3h_mean"]
    profile = df[["diff_0_3h_mean", "diff_3_6h_mean", "diff_6_12h_mean", "diff_12_24h_mean"]].to_numpy(np.float32)
    df["profile_mean"] = np.nanmean(profile, axis=1)
    df["profile_std"] = np.nanstd(profile, axis=1)
    df["profile_range"] = np.nanmax(profile, axis=1) - np.nanmin(profile, axis=1)
    df["negative_bin_count"] = (profile < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h_mean"] >= df["diff_3_6h_mean"]).astype(int)
        + (df["diff_3_6h_mean"] >= df["diff_6_12h_mean"]).astype(int)
        + (df["diff_6_12h_mean"] <= df["diff_12_24h_mean"]).astype(int)
    )

    gdf = gpd.read_file(PADDY_GEOJSON)
    gdf = gdf.reset_index().rename(columns={"index": "feature_index"})
    gdf["feature_seq_id"] = gdf["feature_index"] + 1
    return df, ids, gdf


def balanced_sample(df: pd.DataFrame, features: list[str], n_per_class: int) -> pd.DataFrame:
    df = df.dropna(subset=features + [LABEL_COL]).copy()
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(len(pos), len(neg), n_per_class)
    return pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)


def model_specs_default() -> dict:
    return {
        "ロジスティック回帰": Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
        "決定木": DecisionTreeClassifier(random_state=SEED),
        "ランダムフォレスト": RandomForestClassifier(n_estimators=240, random_state=SEED, n_jobs=1),
        "XGBoost": XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
    }


def model_specs_grid() -> dict:
    return {
        "ロジスティック回帰": (
            Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
            {"model__C": [0.05, 0.1, 0.5, 1.0], "model__class_weight": [None, "balanced"]},
        ),
        "決定木": (
            DecisionTreeClassifier(random_state=SEED),
            {"max_depth": [5, 8, None], "min_samples_leaf": [10, 30, 80], "class_weight": [None, "balanced"]},
        ),
        "ランダムフォレスト": (
            RandomForestClassifier(random_state=SEED, n_jobs=1),
            {"n_estimators": [160, 240], "max_depth": [10, None], "min_samples_leaf": [10, 30, 80], "max_features": ["sqrt"]},
        ),
        "XGBoost": (
            XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            {
                "n_estimators": [120, 180],
                "max_depth": [2, 3],
                "learning_rate": [0.03, 0.05],
                "min_child_weight": [1, 3],
                "subsample": [0.85],
                "colsample_bytree": [0.85],
            },
        ),
    }


def threshold_scan(y_true: np.ndarray, prob: np.ndarray, scenario: str, mode: str, model_name: str) -> tuple[dict, pd.DataFrame]:
    rows = []
    auc = roc_auc_score(y_true, prob)
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.int32)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        rows.append(
            {
                "scenario": scenario,
                "mode": mode,
                "model": model_name,
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
    scan = pd.DataFrame(rows)
    return scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict(), scan


def evaluate(sampled: pd.DataFrame, features: list[str], scenario: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    x = sampled[features].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    rows = []
    scans = []
    estimators = []
    for model_name, model in model_specs_default().items():
        model.fit(x_train, y_train)
        best, scan = threshold_scan(y_test, model.predict_proba(x_test)[:, 1], scenario, "通常設定", model_name)
        rows.append(best)
        scans.append(scan)

    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)
    for model_name, (estimator, grid) in model_specs_grid().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        prob = search.predict_proba(x_test)[:, 1]
        best, scan = threshold_scan(y_test, prob, scenario, "GridSearch", model_name)
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        rows.append(best)
        scans.append(scan)
        estimators.append(
            {
                "scenario": scenario,
                "model": model_name,
                "estimator": search.best_estimator_,
                "threshold": float(best["threshold"]),
                "balanced_accuracy": float(best["balanced_accuracy"]),
                "features": features,
            }
        )
    return pd.DataFrame(rows), pd.concat(scans, ignore_index=True), pd.DataFrame(estimators)


def predict_frame(df: pd.DataFrame, estimator_row: pd.Series) -> pd.DataFrame:
    out = df.dropna(subset=estimator_row["features"] + [LABEL_COL]).copy()
    prob = estimator_row["estimator"].predict_proba(out[estimator_row["features"]].to_numpy(np.float32))[:, 1]
    pred = prob >= float(estimator_row["threshold"])
    out["predicted_probability"] = prob
    out["predicted_inundated"] = pred.astype(np.uint8)
    return out


def export_pixel_gis(pred_df: pd.DataFrame, shape: tuple[int, int]) -> None:
    prob = np.full(shape, np.nan, dtype=np.float32)
    pred = np.zeros(shape, dtype=np.uint8)
    rows = pred_df["row"].to_numpy(np.int32)
    cols = pred_df["col"].to_numpy(np.int32)
    prob[rows, cols] = pred_df["predicted_probability"].to_numpy(np.float32)
    pred[rows, cols] = pred_df["predicted_inundated"].to_numpy(np.uint8)
    write_raster(OUT / "GIS_田んぼ画素_浸水確率.tif", prob, "float32", np.nan)
    write_raster(OUT / "GIS_田んぼ画素_浸水判定.tif", pred, "uint8", 0)
    pred_df[["row", "col", LABEL_COL, "predicted_probability", "predicted_inundated"]].to_csv(
        OUT / "GIS_田んぼ画素_浸水判定一覧.csv", index=False, encoding="utf-8-sig"
    )


def export_polygon_gis(pred_df: pd.DataFrame, gdf: gpd.GeoDataFrame) -> None:
    attrs = pred_df[
        [
            "feature_seq_id",
            LABEL_COL,
            "truth_pixel_count",
            "sentinel_pixel_count",
            "truth_pixel_ratio",
            "predicted_probability",
            "predicted_inundated",
        ]
    ].copy()
    merged = gdf.merge(attrs, on="feature_seq_id", how="inner")
    selected = merged[merged["predicted_inundated"] == 1].copy()
    merged.to_file(OUT / "GIS_田んぼ筆ポリゴン_全判定.geojson", driver="GeoJSON")
    selected.to_file(OUT / "GIS_田んぼ筆ポリゴン_浸水判定.geojson", driver="GeoJSON")
    selected.drop(columns="geometry").to_csv(OUT / "GIS_田んぼ筆ポリゴン_浸水判定一覧.csv", index=False, encoding="utf-8-sig")

    fig, ax = plt.subplots(figsize=(8, 8))
    merged.boundary.plot(ax=ax, color="#cccccc", linewidth=0.15)
    selected.plot(ax=ax, color="#e45756", edgecolor="#8b1a1a", linewidth=0.25, alpha=0.85)
    ax.set_title("浸水と判定された田んぼ筆ポリゴン", fontproperties=FONT)
    ax.set_axis_off()
    plt.tight_layout()
    plt.savefig(OUT / "図_田んぼ筆ポリゴン_浸水判定_ポリゴン直接描画.png", dpi=250, bbox_inches="tight")
    plt.close()


def plot_confusion(row: pd.Series, path: Path, title: str) -> None:
    cm = np.array([[int(row["TP"]), int(row["FN"])], [int(row["FP"]), int(row["TN"])]])
    fig, ax = plt.subplots(figsize=(5.6, 4.8))
    im = ax.imshow(cm, cmap="Blues")
    labels = [["TP\n浸水を検出", "FN\n浸水を未検出"], ["FP\n非浸水を誤検出", "TN\n非浸水を非検出"]]
    max_v = cm.max() if cm.size else 1
    for i in range(2):
        for j in range(2):
            color = "white" if cm[i, j] > max_v * 0.55 else "black"
            ax.text(j, i, f"{labels[i][j]}\n{cm[i, j]:,}", ha="center", va="center", color=color, fontproperties=FONT)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["浸水と予測", "非浸水と予測"], fontproperties=FONT)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["実際に浸水", "実際に非浸水"], fontproperties=FONT)
    ax.set_title(title, fontproperties=FONT)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    plt.tight_layout()
    plt.savefig(path, dpi=200, bbox_inches="tight")
    plt.close()


def plot_summary(metrics: pd.DataFrame, counts: pd.DataFrame) -> None:
    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1).copy()
    fig, ax = plt.subplots(figsize=(9.5, 5))
    x = np.arange(len(best))
    width = 0.18
    for i, (col, label, color) in enumerate(
        [
            ("balanced_accuracy", "Balanced Accuracy", "#4c78a8"),
            ("precision", "Precision", "#f58518"),
            ("recall", "Recall", "#54a24b"),
            ("ROC_AUC", "AUC", "#e45756"),
        ]
    ):
        ax.bar(x + (i - 1.5) * width, best[col], width, label=label, color=color)
    ax.set_xticks(x)
    ax.set_xticklabels((best["scenario"] + "\n" + best["mode"]).tolist(), fontproperties=FONT)
    ax.set_ylim(0.4, 0.85)
    ax.set_title("田んぼ限定: 画素単位と筆ポリゴン単位の比較", fontproperties=FONT)
    ax.set_ylabel("スコア", fontproperties=FONT)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.20))
    plt.tight_layout()
    plt.savefig(OUT / "図_田んぼ限定_性能比較.png", dpi=200, bbox_inches="tight")
    plt.close()

    for _, row in best.iterrows():
        plot_confusion(row, OUT / f"混同行列_{row['scenario']}_{row['mode']}_{row['model']}.png", f"{row['scenario']} {row['mode']} {row['model']}")


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


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame, pixel_pred: pd.DataFrame, poly_pred: pd.DataFrame) -> None:
    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    pred_counts = pd.DataFrame(
        [
            {
                "scenario": "田んぼ画素",
                "all_units": len(pixel_pred),
                "predicted_inundated": int(pixel_pred["predicted_inundated"].sum()),
                "truth_inundated": int(pixel_pred[LABEL_COL].sum()),
            },
            {
                "scenario": "田んぼ筆ポリゴン",
                "all_units": len(poly_pred),
                "predicted_inundated": int(poly_pred["predicted_inundated"].sum()),
                "truth_inundated": int(poly_pred[LABEL_COL].sum()),
            },
        ]
    )
    lines = [
        "# bbox内・田んぼ限定 画素単位/筆ポリゴン単位 比較レポート",
        "",
        "## 条件",
        "",
        "- bbox内に含まれる田んぼのみを対象にしました。",
        "- 画素単位は正解浸水域20,000画素、非浸水域20,000画素を抽出しました。",
        "- 筆ポリゴン単位は利用可能な正解浸水筆1,233、非浸水筆1,233を抽出しました。",
        "- どちらも訓練:検証 = 7:3、GridSearchCVは訓練データ内で5-foldです。",
        "- 筆ポリゴンの可視化は、浸水判定された筆ポリゴンGeoJSONを直接描画しました。ラスタ画素の抜けではなくポリゴン形状そのものを表示しています。",
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## 最良モデル",
        "",
        md_table(best[["scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_params"]]),
        "",
        "## 全体へ適用したときの判定数",
        "",
        md_table(pred_counts),
        "",
        "## 主な出力",
        "",
        "- `GIS_田んぼ画素_浸水判定.tif`",
        "- `GIS_田んぼ画素_浸水確率.tif`",
        "- `GIS_田んぼ筆ポリゴン_浸水判定.geojson`",
        "- `GIS_田んぼ筆ポリゴン_全判定.geojson`",
        "- `図_田んぼ筆ポリゴン_浸水判定_ポリゴン直接描画.png`",
        "- `図_田んぼ限定_性能比較.png`",
        "- `混同行列_*.png`",
        "",
    ]
    (OUT / "田んぼ限定_画素単位_筆ポリゴン単位_比較レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pixel_df, shape = build_paddy_pixel_frame()
    polygon_df, polygon_ids, paddy_gdf = build_paddy_polygon_frame(shape)

    scenarios = [
        ("田んぼ画素", pixel_df, PIXEL_FEATURES, PIXEL_PER_CLASS),
        ("田んぼ筆ポリゴン", polygon_df, POLYGON_FEATURES, POLYGON_PER_CLASS),
    ]
    counts = []
    metric_frames = []
    scan_frames = []
    estimator_frames = []
    sampled_frames = []
    for scenario, df, features, n_per_class in scenarios:
        valid_df = df.dropna(subset=features + [LABEL_COL]).copy()
        sampled = balanced_sample(valid_df, features, n_per_class)
        sampled["scenario"] = scenario
        metrics, scans, estimators = evaluate(sampled, features, scenario)
        counts.append(
            {
                "scenario": scenario,
                "available_positive": int((valid_df[LABEL_COL] == 1).sum()),
                "available_negative": int((valid_df[LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[LABEL_COL] == 1).sum()),
                "sampled_negative": int((sampled[LABEL_COL] == 0).sum()),
                "train_positive": int((sampled[LABEL_COL] == 1).sum() * (1 - TEST_SIZE)),
                "train_negative": int((sampled[LABEL_COL] == 0).sum() * (1 - TEST_SIZE)),
                "test_positive": int((sampled[LABEL_COL] == 1).sum() * TEST_SIZE),
                "test_negative": int((sampled[LABEL_COL] == 0).sum() * TEST_SIZE),
                "feature_count": len(features),
            }
        )
        metric_frames.append(metrics)
        scan_frames.append(scans)
        estimator_frames.append(estimators)
        sampled_frames.append(sampled)

    counts_df = pd.DataFrame(counts)
    metrics_df = pd.concat(metric_frames, ignore_index=True)
    scans_df = pd.concat(scan_frames, ignore_index=True)
    estimators_df = pd.concat(estimator_frames, ignore_index=True)
    sampled_df = pd.concat(sampled_frames, ignore_index=True)

    pixel_est = estimators_df[estimators_df["scenario"] == "田んぼ画素"].sort_values("balanced_accuracy", ascending=False).iloc[0]
    poly_est = estimators_df[estimators_df["scenario"] == "田んぼ筆ポリゴン"].sort_values("balanced_accuracy", ascending=False).iloc[0]
    pixel_pred = predict_frame(pixel_df, pixel_est)
    poly_pred = predict_frame(polygon_df, poly_est)

    counts_df.to_csv(OUT / "母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics_df.to_csv(OUT / "全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans_df.to_csv(OUT / "閾値スキャン.csv", index=False, encoding="utf-8-sig")
    sampled_df.to_csv(OUT / "抽出データ.csv", index=False, encoding="utf-8-sig")
    polygon_df.to_csv(OUT / "田んぼ筆ポリゴン特徴量.csv", index=False, encoding="utf-8-sig")
    pixel_pred[["row", "col", LABEL_COL, "predicted_probability", "predicted_inundated"]].to_csv(
        OUT / "田んぼ画素_全判定一覧.csv", index=False, encoding="utf-8-sig"
    )
    poly_pred.to_csv(OUT / "田んぼ筆ポリゴン_全判定一覧.csv", index=False, encoding="utf-8-sig")

    export_pixel_gis(pixel_pred, shape)
    export_polygon_gis(poly_pred, paddy_gdf)
    plot_summary(metrics_df, counts_df)
    write_report(counts_df, metrics_df, pixel_pred, poly_pred)

    best = metrics_df.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    print(best[["scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
