from __future__ import annotations

import json
from pathlib import Path

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
OUT = BASE / "bbox_balanced_classification"
BBOX_GEOJSON = ROOT / "output/kurume_tif_bboxes/kurume_tif_valid_bboxes.geojson"
PADDY_GEOJSON = ROOT / "output/gsi_h30_geojson_s1/map7_land_polygons/map7_fude_paddy_polygons_from_db.geojson"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 3
LABEL_COL = "label"
MAX_PIXEL_PER_CLASS = 10000
MAX_POLYGON_PER_CLASS = 1000

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
    with rasterio.open(DIFF_RASTERS["0_3h"]) as src:
        return src.profile.copy()


def write_raster(path: Path, data: np.ndarray, dtype: str, nodata) -> None:
    profile = template_profile()
    profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def load_geojson_features(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8")).get("features", [])


def build_bbox_mask() -> np.ndarray:
    features = load_geojson_features(BBOX_GEOJSON)
    profile = template_profile()
    mask = rasterize(
        ((f["geometry"], 1) for f in features if f.get("geometry")),
        out_shape=(profile["height"], profile["width"]),
        transform=profile["transform"],
        fill=0,
        dtype="uint8",
        all_touched=True,
    ).astype(bool)
    write_raster(OUT / "GIS_bbox_union_mask.tif", mask.astype(np.uint8), "uint8", 0)
    return mask


def add_time_features(df: pd.DataFrame, prefix: str = "") -> pd.DataFrame:
    c0 = f"{prefix}diff_0_3h"
    c3 = f"{prefix}diff_3_6h"
    c6 = f"{prefix}diff_6_12h"
    c12 = f"{prefix}diff_12_24h"
    df["early_mean_0_6h"] = (df[c0] + df[c3]) / 2
    df["late_mean_6_24h"] = (df[c6] + df[c12]) / 2
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df[c0] - df[c3]
    df["drop_3_6_to_6_12"] = df[c3] - df[c6]
    df["recovery_6_12_to_12_24"] = df[c12] - df[c6]
    df["drop_0_3_to_6_12"] = df[c0] - df[c6]
    df["change_0_3_to_6_12"] = df[c6] - df[c0]
    profile = df[[c0, c3, c6, c12]].to_numpy(np.float32)
    df["profile_mean"] = profile.mean(axis=1)
    df["profile_std"] = profile.std(axis=1)
    df["profile_range"] = profile.max(axis=1) - profile.min(axis=1)
    df["negative_bin_count"] = (profile < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df[c0] >= df[c3]).astype(int)
        + (df[c3] >= df[c6]).astype(int)
        + (df[c6] <= df[c12]).astype(int)
    )
    return df


def build_pixel_frame(mask_path: Path, scenario: str, bbox_mask: np.ndarray) -> tuple[pd.DataFrame, tuple[int, int]]:
    d0 = read_float(DIFF_RASTERS["0_3h"])
    d3 = read_float(DIFF_RASTERS["3_6h"])
    d6 = read_float(DIFF_RASTERS["6_12h"])
    d12 = read_float(DIFF_RASTERS["12_24h"])
    truth = read_bool(BASE / "map7_inundation_truth_mask.tif")
    land_mask = read_bool(mask_path)
    valid = np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12) & land_mask & bbox_mask
    row, col = np.where(valid)
    df = pd.DataFrame(
        {
            "scenario": scenario,
            "row": row,
            "col": col,
            LABEL_COL: truth[valid].astype(np.uint8),
            "diff_0_3h": d0[valid],
            "diff_3_6h": d3[valid],
            "diff_6_12h": d6[valid],
            "diff_12_24h": d12[valid],
        }
    )
    return add_time_features(df), d0.shape


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
    if len(flat_ids) == 0:
        return pd.DataFrame(columns=["feature_seq_id"])
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


def build_polygon_frame(bbox_mask: np.ndarray, shape: tuple[int, int]) -> tuple[pd.DataFrame, np.ndarray, list[dict]]:
    features = load_geojson_features(PADDY_GEOJSON)
    ids = rasterize_polygon_ids(features, shape)
    truth = read_bool(BASE / "map7_inundation_truth_mask.tif")
    diffs = {name: read_float(path) for name, path in DIFF_RASTERS.items()}
    valid = bbox_mask & (ids > 0)
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
    return df, ids, features


def balanced_sample(df: pd.DataFrame, features: list[str], max_per_class: int) -> pd.DataFrame:
    df = df.dropna(subset=features + [LABEL_COL]).copy()
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(len(pos), len(neg), max_per_class)
    if n == 0:
        raise ValueError("positive または negative が0件のため評価できません。")
    sampled = pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)
    return sampled


def model_specs_default() -> dict:
    return {
        "ロジスティック回帰": Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
        "決定木": DecisionTreeClassifier(random_state=SEED),
        "ランダムフォレスト": RandomForestClassifier(n_estimators=200, random_state=SEED, n_jobs=1),
        "XGBoost": XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
    }


def model_specs_grid() -> dict:
    return {
        "ロジスティック回帰": (
            Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
            {"model__C": [0.05, 0.1, 0.5, 1.0, 2.0], "model__class_weight": [None, "balanced"]},
        ),
        "決定木": (
            DecisionTreeClassifier(random_state=SEED),
            {"max_depth": [3, 5, 8, None], "min_samples_leaf": [10, 30, 80], "class_weight": [None, "balanced"]},
        ),
        "ランダムフォレスト": (
            RandomForestClassifier(random_state=SEED, n_jobs=1),
            {"n_estimators": [160, 240], "max_depth": [8, 12, None], "min_samples_leaf": [10, 30, 80], "max_features": ["sqrt", "log2"]},
        ),
        "XGBoost": (
            XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            {
                "n_estimators": [120, 180],
                "max_depth": [2, 3, 4],
                "learning_rate": [0.03, 0.05, 0.08],
                "min_child_weight": [1, 3],
                "subsample": [0.8, 0.95],
                "colsample_bytree": [0.8, 0.95],
            },
        ),
    }


def threshold_metrics(y_true: np.ndarray, prob: np.ndarray, model_name: str, mode: str, scenario: str) -> tuple[dict, pd.DataFrame]:
    rows = []
    auc = roc_auc_score(y_true, prob) if len(np.unique(y_true)) == 2 else np.nan
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
    best = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
    return best, scan


def evaluate_scenario(sampled: pd.DataFrame, features: list[str], scenario: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    x = sampled[features].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    metric_rows = []
    scan_frames = []
    estimator_rows = []

    for model_name, model in model_specs_default().items():
        model.fit(x_train, y_train)
        prob = model.predict_proba(x_test)[:, 1]
        best, scan = threshold_metrics(y_test, prob, model_name, "全特徴量_通常設定", scenario)
        metric_rows.append(best)
        scan_frames.append(scan)

    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)
    for model_name, (estimator, grid) in model_specs_grid().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        prob = search.predict_proba(x_test)[:, 1]
        best, scan = threshold_metrics(y_test, prob, model_name, "全特徴量_GridSearch", scenario)
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        metric_rows.append(best)
        scan_frames.append(scan)
        estimator_rows.append(
            {
                "scenario": scenario,
                "model": model_name,
                "estimator": search.best_estimator_,
                "threshold": float(best["threshold"]),
                "balanced_accuracy": float(best["balanced_accuracy"]),
                "features": features,
                "mode": "全特徴量_GridSearch",
            }
        )

    return pd.DataFrame(metric_rows), pd.concat(scan_frames, ignore_index=True), pd.DataFrame(estimator_rows)


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


def plot_results(metrics: pd.DataFrame, counts: pd.DataFrame) -> None:
    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1).copy()
    fig, ax = plt.subplots(figsize=(12, 5.8))
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
    ax.set_xticklabels((best["scenario"] + "\n" + best["mode"].str.replace("全特徴量_", "", regex=False)).tolist(), fontproperties=FONT)
    ax.set_ylim(0.4, 0.9)
    ax.set_ylabel("スコア", fontproperties=FONT)
    ax.set_title("bbox内に限定した条件別モデル性能", fontproperties=FONT)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.20))
    plt.tight_layout()
    plt.savefig(OUT / "図_条件別モデル性能比較.png", dpi=200, bbox_inches="tight")
    plt.close()

    fig, ax = plt.subplots(figsize=(9, 4.8))
    c = counts.set_index("scenario")[["available_positive", "available_negative"]]
    c.plot(kind="bar", ax=ax, color=["#e45756", "#4c78a8"])
    ax.set_yscale("log")
    ax.set_ylabel("画素数・筆数（対数）", fontproperties=FONT)
    ax.set_title("bbox内の利用可能なpositive / negative母数", fontproperties=FONT)
    ax.set_xticklabels(c.index.tolist(), rotation=0, fontproperties=FONT)
    ax.legend(["浸水", "非浸水"], prop=FONT)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT / "図_bbox内母数.png", dpi=200, bbox_inches="tight")
    plt.close()

    for _, row in best.iterrows():
        safe = f"{row['scenario']}_{row['mode']}_{row['model']}".replace("/", "_").replace("\\", "_")
        plot_confusion(row, OUT / f"混同行列_{safe}.png", f"{row['scenario']} {row['mode']} {row['model']}")


def export_paddy_pixel_gis(paddy_df: pd.DataFrame, estimator_row: pd.Series, shape: tuple[int, int]) -> None:
    features = estimator_row["features"]
    model = estimator_row["estimator"]
    threshold = float(estimator_row["threshold"])
    prob = model.predict_proba(paddy_df[features].to_numpy(np.float32))[:, 1]
    pred = prob >= threshold
    prob_arr = np.full(shape, np.nan, dtype=np.float32)
    pred_arr = np.zeros(shape, dtype=np.uint8)
    rows = paddy_df["row"].to_numpy(np.int32)
    cols = paddy_df["col"].to_numpy(np.int32)
    prob_arr[rows, cols] = prob.astype(np.float32)
    pred_arr[rows, cols] = pred.astype(np.uint8)
    write_raster(OUT / "GIS_bbox_田んぼ画素_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_bbox_田んぼ画素_浸水判定.tif", pred_arr, "uint8", 0)
    out_df = paddy_df[["row", "col", LABEL_COL]].copy()
    out_df["predicted_probability"] = prob
    out_df["predicted_inundated"] = pred.astype(np.uint8)
    out_df.to_csv(OUT / "GIS_bbox_田んぼ画素_浸水判定一覧.csv", index=False, encoding="utf-8-sig")


def export_paddy_polygon_gis(poly_df: pd.DataFrame, estimator_row: pd.Series, ids: np.ndarray, source_features: list[dict]) -> None:
    feature_cols = estimator_row["features"]
    model = estimator_row["estimator"]
    threshold = float(estimator_row["threshold"])
    prob = model.predict_proba(poly_df[feature_cols].to_numpy(np.float32))[:, 1]
    pred = prob >= threshold
    out_df = poly_df[
        ["feature_seq_id", "polygon_uuid", "local_government_cd", "pref_id", "land_type", "truth_pixel_count", "sentinel_pixel_count", "truth_pixel_ratio", LABEL_COL]
    ].copy()
    out_df["predicted_probability"] = prob
    out_df["predicted_inundated"] = pred.astype(np.uint8)
    out_df.to_csv(OUT / "GIS_bbox_田んぼ筆ポリゴン_浸水判定一覧.csv", index=False, encoding="utf-8-sig")

    prob_by_id = dict(zip(out_df["feature_seq_id"].astype(int), prob.astype(float)))
    selected_ids = set(int(v) for v in out_df.loc[out_df["predicted_inundated"] == 1, "feature_seq_id"])
    selected_features = []
    for seq_id in sorted(selected_ids):
        f = source_features[seq_id - 1]
        props = dict(f.get("properties", {}))
        row = out_df[out_df["feature_seq_id"] == seq_id].iloc[0]
        props.update(
            {
                "predicted_probability": float(prob_by_id[seq_id]),
                "predicted_inundated": 1,
                "truth_pixel_count": int(row["truth_pixel_count"]),
                "sentinel_pixel_count_in_bbox": int(row["sentinel_pixel_count"]),
                "truth_pixel_ratio_in_bbox": float(row["truth_pixel_ratio"]),
            }
        )
        selected_features.append({"type": "Feature", "geometry": f["geometry"], "properties": props})
    (OUT / "GIS_bbox_田んぼ筆ポリゴン_浸水判定.geojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": selected_features}, ensure_ascii=False),
        encoding="utf-8",
    )

    pred_arr = np.isin(ids, list(selected_ids)).astype(np.uint8)
    prob_arr = np.full(ids.shape, np.nan, dtype=np.float32)
    for seq_id, p in prob_by_id.items():
        prob_arr[ids == seq_id] = np.float32(p)
    write_raster(OUT / "GIS_bbox_田んぼ筆ポリゴン_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_bbox_田んぼ筆ポリゴン_浸水判定.tif", pred_arr, "uint8", 0)


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
    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    report = [
        "# bbox内に限定した浸水域判別 総合比較レポート",
        "",
        "## 条件",
        "",
        "- `D:/sotsuron/kurume` の各TIF有効画素を囲むbboxの和集合内だけを対象にしました。",
        "- 正解浸水域は既存の `map7_inundation_truth_mask.tif` を使用しました。",
        "- 画素単位は positive / negative の画素数を同数にそろえています。",
        "- 筆ポリゴン単位は bbox内に含まれるSentinel画素だけで筆ごとの特徴量を再計算し、positive / negative の筆数を同数にそろえています。",
        f"- 画素単位は最大 {MAX_PIXEL_PER_CLASS:,} / {MAX_PIXEL_PER_CLASS:,}、筆ポリゴン単位は最大 {MAX_POLYGON_PER_CLASS:,} / {MAX_POLYGON_PER_CLASS:,} で抽出しました。",
        "- `全特徴量_通常設定` は全特徴量を使い、パラメータチューニングなしで評価しました。",
        "- `全特徴量_GridSearch` は全特徴量を使い、GridSearchCVでパラメータチューニングしました。",
        "",
        "## bbox内の母数と抽出数",
        "",
        md_table(counts),
        "",
        "## 条件別の最良モデル",
        "",
        md_table(best[["scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_params"]]),
        "",
        "## 全モデル結果",
        "",
        md_table(metrics[["scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "best_cv_balanced_accuracy", "best_params"]]),
        "",
        "## 出力図",
        "",
        "- `図_条件別モデル性能比較.png`",
        "- `図_bbox内母数.png`",
        "- `混同行列_*.png`",
        "",
        "## GIS出力",
        "",
        "- `GIS_bbox_union_mask.tif`: bbox和集合マスク",
        "- `GIS_bbox_田んぼ画素_浸水判定.tif`: 田んぼ画素単位の浸水判定",
        "- `GIS_bbox_田んぼ画素_浸水確率.tif`: 田んぼ画素単位の浸水確率",
        "- `GIS_bbox_田んぼ画素_浸水判定一覧.csv`: 田んぼ画素単位の判定一覧",
        "- `GIS_bbox_田んぼ筆ポリゴン_浸水判定.geojson`: 浸水判定された田んぼ筆ポリゴン",
        "- `GIS_bbox_田んぼ筆ポリゴン_浸水判定.tif`: 筆ポリゴン単位の浸水判定ラスタ",
        "- `GIS_bbox_田んぼ筆ポリゴン_浸水確率.tif`: 筆ポリゴン単位の浸水確率ラスタ",
        "- `GIS_bbox_田んぼ筆ポリゴン_浸水判定一覧.csv`: 筆ポリゴン単位の判定一覧",
        "",
    ]
    (OUT / "bbox内総合比較レポート.md").write_text("\n".join(report), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    bbox_mask = build_bbox_mask()
    paddy_or_road_df, shape = build_pixel_frame(LAND / "map7_paddy_or_road_mask.tif", "道路+田んぼ画素_bbox内", bbox_mask)
    paddy_df, _ = build_pixel_frame(LAND / "map7_paddy_mask.tif", "田んぼ画素_bbox内", bbox_mask)
    polygon_df, polygon_ids, source_features = build_polygon_frame(bbox_mask, shape)
    polygon_df["scenario"] = "田んぼ筆ポリゴン_bbox内"

    scenario_items = [
        ("道路+田んぼ画素_bbox内", paddy_or_road_df, PIXEL_FEATURES, MAX_PIXEL_PER_CLASS),
        ("田んぼ画素_bbox内", paddy_df, PIXEL_FEATURES, MAX_PIXEL_PER_CLASS),
        ("田んぼ筆ポリゴン_bbox内", polygon_df, POLYGON_FEATURES, MAX_POLYGON_PER_CLASS),
    ]

    count_rows = []
    metric_frames = []
    scan_frames = []
    estimator_frames = []
    sampled_frames = []
    full_frames = []
    for scenario, df, features, max_per_class in scenario_items:
        df = df.dropna(subset=features + [LABEL_COL]).copy()
        sampled = balanced_sample(df, features, max_per_class)
        sampled["scenario"] = scenario
        sampled_frames.append(sampled)
        full_save = df.copy()
        full_save["scenario"] = scenario
        full_frames.append(full_save)
        count_rows.append(
            {
                "scenario": scenario,
                "available_positive": int((df[LABEL_COL] == 1).sum()),
                "available_negative": int((df[LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[LABEL_COL] == 1).sum()),
                "sampled_negative": int((sampled[LABEL_COL] == 0).sum()),
                "feature_count": len(features),
            }
        )
        metrics, scans, estimators = evaluate_scenario(sampled, features, scenario)
        metric_frames.append(metrics)
        scan_frames.append(scans)
        estimator_frames.append(estimators)

    counts = pd.DataFrame(count_rows)
    metrics = pd.concat(metric_frames, ignore_index=True)
    scans = pd.concat(scan_frames, ignore_index=True)
    estimators = pd.concat(estimator_frames, ignore_index=True)
    sampled_all = pd.concat(sampled_frames, ignore_index=True)

    counts.to_csv(OUT / "母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン.csv", index=False, encoding="utf-8-sig")
    sampled_all.to_csv(OUT / "抽出データ.csv", index=False, encoding="utf-8-sig")
    polygon_df.to_csv(OUT / "bbox内_田んぼ筆ポリゴン特徴量.csv", index=False, encoding="utf-8-sig")

    paddy_est = estimators[estimators["scenario"] == "田んぼ画素_bbox内"].sort_values("balanced_accuracy", ascending=False).iloc[0]
    poly_est = estimators[estimators["scenario"] == "田んぼ筆ポリゴン_bbox内"].sort_values("balanced_accuracy", ascending=False).iloc[0]
    export_paddy_pixel_gis(paddy_df, paddy_est, shape)
    export_paddy_polygon_gis(polygon_df.dropna(subset=POLYGON_FEATURES + [LABEL_COL]), poly_est, polygon_ids, source_features)

    plot_results(metrics, counts)
    write_report(counts, metrics)
    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    print(best[["scenario", "mode", "model", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
