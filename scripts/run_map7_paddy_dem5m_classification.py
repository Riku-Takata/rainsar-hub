from __future__ import annotations

import json
import math
import urllib.error
import urllib.request
from io import BytesIO
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd
from PIL import Image
import rasterio
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

import analyze_map7_bbox_paddy_pixel_polygon_20000 as base


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_paddy_dem5m_classification"
CACHE = ROOT / "output/gsi_dem5m_tiles"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 3
PIXEL_PER_CLASS = 20000
POLYGON_PER_CLASS = 20000
LABEL_COL = "label"
DEM_ZOOM = 15

DEM_SOURCES = [
    ("DEM5A", "https://cyberjapandata.gsi.go.jp/xyz/dem5a_png/{z}/{x}/{y}.png"),
    ("DEM5B", "https://cyberjapandata.gsi.go.jp/xyz/dem5b_png/{z}/{x}/{y}.png"),
    ("DEM5C", "https://cyberjapandata.gsi.go.jp/xyz/dem5c_png/{z}/{x}/{y}.png"),
]

PIXEL_DEM_FEATURES = ["elevation_m", "slope_deg", "relative_elevation_7x7_m"]
POLYGON_DEM_FEATURES = [
    "elevation_mean_m",
    "elevation_std_m",
    "elevation_min_m",
    "elevation_median_m",
    "elevation_max_m",
    "slope_mean_deg",
    "slope_std_deg",
    "relative_elevation_mean_m",
    "relative_elevation_std_m",
    "relative_elevation_min_m",
]


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


def template_profile() -> dict:
    with rasterio.open(base.DIFF_RASTERS["0_3h"]) as src:
        return src.profile.copy()


def write_raster(path: Path, data: np.ndarray, dtype: str, nodata) -> None:
    profile = template_profile()
    profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def lonlat_to_tile(lon: np.ndarray, lat: np.ndarray, zoom: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    n = 2**zoom
    lat_rad = np.deg2rad(lat)
    xt = (lon + 180.0) / 360.0 * n
    yt = (1.0 - np.arcsinh(np.tan(lat_rad)) / math.pi) / 2.0 * n
    x_tile = np.floor(xt).astype(np.int64)
    y_tile = np.floor(yt).astype(np.int64)
    px = np.clip(np.floor((xt - x_tile) * 256).astype(np.int64), 0, 255)
    py = np.clip(np.floor((yt - y_tile) * 256).astype(np.int64), 0, 255)
    return x_tile, y_tile, px, py


def decode_dem_png(data: bytes) -> np.ndarray:
    rgb = np.asarray(Image.open(BytesIO(data)).convert("RGB"), dtype=np.int32)
    value = rgb[:, :, 0] * 65536 + rgb[:, :, 1] * 256 + rgb[:, :, 2]
    dem = value.astype(np.float32)
    high = dem >= 2**23
    dem[high] = dem[high] - 2**24
    dem *= 0.01
    nodata = (rgb[:, :, 0] == 128) & (rgb[:, :, 1] == 0) & (rgb[:, :, 2] == 0)
    dem[nodata] = np.nan
    return dem


def fetch_tile(source: str, url_template: str, z: int, x: int, y: int) -> np.ndarray | None:
    path = CACHE / source / str(z) / str(x) / f"{y}.png"
    if path.exists():
        return decode_dem_png(path.read_bytes())
    url = url_template.format(z=z, x=x, y=y)
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            data = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return decode_dem_png(data)


def build_gsi_dem5m_arrays(valid_mask: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, pd.DataFrame]:
    elev_path = OUT / "GIS_GSI_DEM5m_elevation_m.tif"
    slope_path = OUT / "GIS_GSI_DEM5m_slope_deg.tif"
    rel_path = OUT / "GIS_GSI_DEM5m_relative_elevation_7x7_m.tif"
    source_path = OUT / "GIS_GSI_DEM5m_source_code.tif"
    if elev_path.exists() and slope_path.exists() and rel_path.exists() and source_path.exists():
        elevation = base.read_float(elev_path)
        slope = base.read_float(slope_path)
        relative = base.read_float(rel_path)
        with rasterio.open(source_path) as src:
            source_code = src.read(1)
        rows = []
        for code, source in [(1, "DEM5A"), (2, "DEM5B"), (3, "DEM5C"), (0, "no_data")]:
            rows.append({"source": source, "tile_hit": np.nan, "pixel_hit": int(((source_code == code) & valid_mask).sum())})
        return elevation, slope, relative, pd.DataFrame(rows)

    profile = template_profile()
    transform = profile["transform"]
    rows, cols = np.where(valid_mask)
    lon = transform.c + (cols + 0.5) * transform.a
    lat = transform.f + (rows + 0.5) * transform.e
    x_tile, y_tile, px, py = lonlat_to_tile(lon, lat, DEM_ZOOM)
    tile_keys = pd.DataFrame({"x": x_tile, "y": y_tile}).drop_duplicates().to_records(index=False).tolist()

    elevation = np.full(valid_mask.shape, np.nan, dtype=np.float32)
    source_code = np.zeros(valid_mask.shape, dtype=np.uint8)
    source_stats = {name: {"tile_hit": 0, "pixel_hit": 0} for name, _ in DEM_SOURCES}
    source_stats["no_data"] = {"tile_hit": 0, "pixel_hit": 0}

    tile_lookup = {(int(x), int(y)): i for i, (x, y) in enumerate(tile_keys, start=1)}
    tile_id = np.array([tile_lookup[(int(x), int(y))] for x, y in zip(x_tile, y_tile)], dtype=np.int32)

    for tx, ty in tile_keys:
        m = tile_id == tile_lookup[(int(tx), int(ty))]
        remaining = np.ones(int(m.sum()), dtype=bool)
        values = np.full(int(m.sum()), np.nan, dtype=np.float32)
        codes = np.zeros(int(m.sum()), dtype=np.uint8)
        local_px = px[m]
        local_py = py[m]
        for source_idx, (source_name, url_template) in enumerate(DEM_SOURCES, start=1):
            tile = fetch_tile(source_name, url_template, DEM_ZOOM, int(tx), int(ty))
            if tile is None:
                continue
            source_stats[source_name]["tile_hit"] += 1
            sampled = tile[local_py, local_px]
            ok = remaining & np.isfinite(sampled)
            values[ok] = sampled[ok]
            codes[ok] = source_idx
            remaining[ok] = False
            source_stats[source_name]["pixel_hit"] += int(ok.sum())
            if not remaining.any():
                break
        source_stats["no_data"]["pixel_hit"] += int(remaining.sum())
        elevation[rows[m], cols[m]] = values
        source_code[rows[m], cols[m]] = codes

    slope = compute_slope(elevation, transform)
    relative = compute_relative_elevation(elevation)
    write_raster(OUT / "GIS_GSI_DEM5m_elevation_m.tif", elevation, "float32", np.nan)
    write_raster(OUT / "GIS_GSI_DEM5m_slope_deg.tif", slope, "float32", np.nan)
    write_raster(OUT / "GIS_GSI_DEM5m_relative_elevation_7x7_m.tif", relative, "float32", np.nan)
    write_raster(OUT / "GIS_GSI_DEM5m_source_code.tif", source_code, "uint8", 0)
    stats = pd.DataFrame(
        [{"source": k, **v} for k, v in source_stats.items()]
    )
    return elevation, slope, relative, stats


def compute_slope(elevation: np.ndarray, transform) -> np.ndarray:
    filled = fill_nan_with_global_median(elevation)
    mean_lat = transform.f + transform.e * elevation.shape[0] / 2
    dx = abs(transform.a) * 111_320.0 * math.cos(math.radians(mean_lat))
    dy = abs(transform.e) * 111_320.0
    dz_dy, dz_dx = np.gradient(filled, dy, dx)
    slope = np.degrees(np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))).astype(np.float32)
    slope[~np.isfinite(elevation)] = np.nan
    return slope


def compute_relative_elevation(elevation: np.ndarray) -> np.ndarray:
    try:
        from scipy.ndimage import uniform_filter

        valid = np.isfinite(elevation).astype(np.float32)
        filled = np.where(np.isfinite(elevation), elevation, 0).astype(np.float32)
        s = uniform_filter(filled, size=7, mode="nearest") * 49.0
        c = uniform_filter(valid, size=7, mode="nearest") * 49.0
        local_mean = np.divide(s, c, out=np.full_like(elevation, np.nan, dtype=np.float32), where=c > 0)
    except Exception:
        local_mean = np.full_like(elevation, np.nan, dtype=np.float32)
        padded = np.pad(elevation, 3, mode="edge")
        rows, cols = np.where(np.isfinite(elevation))
        for r, c in zip(rows, cols):
            local_mean[r, c] = np.nanmean(padded[r : r + 7, c : c + 7])
    relative = (elevation - local_mean).astype(np.float32)
    relative[~np.isfinite(elevation)] = np.nan
    return relative


def fill_nan_with_global_median(arr: np.ndarray) -> np.ndarray:
    out = arr.astype(np.float32).copy()
    med = float(np.nanmedian(out))
    out[~np.isfinite(out)] = med
    return out


def add_dem_to_pixel_frame(df: pd.DataFrame, elevation: np.ndarray, slope: np.ndarray, relative: np.ndarray) -> pd.DataFrame:
    out = df.copy()
    r = out["row"].to_numpy(np.int32)
    c = out["col"].to_numpy(np.int32)
    out["elevation_m"] = elevation[r, c]
    out["slope_deg"] = slope[r, c]
    out["relative_elevation_7x7_m"] = relative[r, c]
    return out


def grouped_dem_stats(ids: np.ndarray, values: np.ndarray, valid: np.ndarray, prefix: str, unit: str) -> pd.DataFrame:
    mask = valid & (ids > 0) & np.isfinite(values)
    flat_ids = ids[mask].astype(np.int32)
    flat_values = values[mask].astype(np.float32)
    if flat_ids.size == 0:
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
                f"{prefix}_mean_{unit}": float(np.mean(vals)),
                f"{prefix}_std_{unit}": float(np.std(vals)),
                f"{prefix}_min_{unit}": float(np.min(vals)),
                f"{prefix}_median_{unit}": float(np.median(vals)),
                f"{prefix}_max_{unit}": float(np.max(vals)),
            }
        )
    return pd.DataFrame(rows)


def add_dem_to_polygon_frame(
    df: pd.DataFrame,
    ids: np.ndarray,
    elevation: np.ndarray,
    slope: np.ndarray,
    relative: np.ndarray,
) -> pd.DataFrame:
    bbox = base.read_bool(base.BBOX_MASK)
    valid = bbox & (ids > 0)
    out = df.copy()
    out = out.merge(grouped_dem_stats(ids, elevation, valid, "elevation", "m"), on="feature_seq_id", how="left")
    out = out.merge(grouped_dem_stats(ids, slope, valid, "slope", "deg"), on="feature_seq_id", how="left")
    rel = grouped_dem_stats(ids, relative, valid, "relative_elevation", "m")
    keep = ["feature_seq_id", "relative_elevation_mean_m", "relative_elevation_std_m", "relative_elevation_min_m"]
    out = out.merge(rel[keep], on="feature_seq_id", how="left")
    return out


def model_specs_default() -> dict:
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


def model_specs_grid() -> dict:
    return {}


def threshold_scan(y_true: np.ndarray, prob: np.ndarray, scenario: str, feature_set: str, mode: str, model_name: str) -> tuple[dict, pd.DataFrame]:
    rows = []
    auc = roc_auc_score(y_true, prob)
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.int32)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        rows.append(
            {
                "scenario": scenario,
                "feature_set": feature_set,
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


def evaluate(sampled: pd.DataFrame, features: list[str], scenario: str, feature_set: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    x = sampled[features].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    rows = []
    scans = []
    estimators = []
    for model_name, model in model_specs_default().items():
        model.fit(x_train, y_train)
        best, scan = threshold_scan(y_test, model.predict_proba(x_test)[:, 1], scenario, feature_set, "Default", model_name)
        rows.append(best)
        scans.append(scan)
    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)
    for model_name, (estimator, grid) in model_specs_grid().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        best, scan = threshold_scan(y_test, search.predict_proba(x_test)[:, 1], scenario, feature_set, "GridSearch", model_name)
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        rows.append(best)
        scans.append(scan)
        estimators.append(
            {
                "scenario": scenario,
                "feature_set": feature_set,
                "model": model_name,
                "estimator": search.best_estimator_,
                "threshold": float(best["threshold"]),
                "balanced_accuracy": float(best["balanced_accuracy"]),
                "features": features,
            }
        )
    return pd.DataFrame(rows), pd.concat(scans, ignore_index=True), pd.DataFrame(estimators)


def balanced_sample(df: pd.DataFrame, features: list[str], n_per_class: int) -> pd.DataFrame:
    valid = df.dropna(subset=features + [LABEL_COL]).copy()
    pos = valid[valid[LABEL_COL] == 1]
    neg = valid[valid[LABEL_COL] == 0]
    n = min(len(pos), len(neg), n_per_class)
    return pd.concat([pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)], ignore_index=True).sample(frac=1, random_state=SEED)


def plot_best_comparison(best: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    labels = best["scenario"] + "\n" + best["feature_set"]
    x = np.arange(len(best))
    width = 0.18
    for i, (col, label) in enumerate(
        [("balanced_accuracy", "Balanced Accuracy"), ("precision", "Precision"), ("recall", "Recall"), ("ROC_AUC", "AUC")]
    ):
        ax.bar(x + (i - 1.5) * width, best[col], width, label=label)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=0)
    ax.set_ylim(0.45, 0.85)
    ax.set_ylabel("score")
    ax.set_title("GSI DEM 5m特徴量追加による精度比較")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.18))
    plt.tight_layout()
    plt.savefig(OUT / "図_GSI_DEM5m_精度比較.png", dpi=220, bbox_inches="tight")
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


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame, dem_source_stats: pd.DataFrame) -> None:
    best = metrics.sort_values(["scenario", "feature_set", "mode", "balanced_accuracy"], ascending=[True, True, True, False])
    best = best.groupby(["scenario", "feature_set", "mode"]).head(1).copy()
    if "best_params" not in best.columns:
        best["best_params"] = ""
    overall_best = metrics.sort_values(["scenario", "feature_set", "balanced_accuracy"], ascending=[True, True, False])
    overall_best = overall_best.groupby(["scenario", "feature_set"]).head(1).copy()
    base_best = overall_best[overall_best["feature_set"] == "backscatter_only"][["scenario", "balanced_accuracy"]].rename(columns={"balanced_accuracy": "ba_without_dem"})
    dem_best = overall_best[overall_best["feature_set"] == "backscatter_plus_gsi_dem5m"][["scenario", "balanced_accuracy"]].rename(columns={"balanced_accuracy": "ba_with_dem"})
    diff = base_best.merge(dem_best, on="scenario", how="outer")
    diff["delta_ba"] = diff["ba_with_dem"] - diff["ba_without_dem"]

    lines = [
        "# GSI DEM 5m特徴量を追加した浸水判別結果",
        "",
        "## 条件",
        "",
        "- 対象: bbox内の田んぼ画素、および田んぼ筆ポリゴン。",
        "- DEM: 国土地理院 標高タイル DEM5A -> DEM5B -> DEM5C の順に使用。",
        "- 追加特徴量: 標高、傾斜、7x7近傍に対する相対標高。筆ポリゴン単位では筆内統計量に集約。",
        "- 学習/検証: 7:3の層化分割。今回は実行時間を抑えるため、RandomForestとXGBoostの固定設定で評価し、しきい値を検証データ上で最適化。",
        "",
        "## DEM取得状況",
        "",
        md_table(dem_source_stats),
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## 最良モデルのDEM有無比較",
        "",
        md_table(diff),
        "",
        "## 最良モデル一覧",
        "",
        md_table(best[["scenario", "feature_set", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_params"]]),
        "",
        "## 出力",
        "",
        "- `GIS_GSI_DEM5m_elevation_m.tif`",
        "- `GIS_GSI_DEM5m_slope_deg.tif`",
        "- `GIS_GSI_DEM5m_relative_elevation_7x7_m.tif`",
        "- `GSI_DEM5m_全モデル評価指標.csv`",
        "- `図_GSI_DEM5m_精度比較.png`",
        "",
    ]
    (OUT / "GSI_DEM5m_機械学習精度レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")
    (OUT / "gsi_dem5m_ml_report.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    base.OUT = BASE / "bbox_paddy_pixel_polygon_20000"
    pixel_df, shape = base.build_paddy_pixel_frame()
    polygon_df, polygon_ids, _ = base.build_paddy_polygon_frame(shape)
    bbox = base.read_bool(base.BBOX_MASK)
    paddy = base.read_bool(base.LAND / "map7_paddy_mask.tif")
    dem_valid = bbox & paddy
    elevation, slope, relative, dem_source_stats = build_gsi_dem5m_arrays(dem_valid)
    pixel_df = add_dem_to_pixel_frame(pixel_df, elevation, slope, relative)
    polygon_df = add_dem_to_polygon_frame(polygon_df, polygon_ids, elevation, slope, relative)

    scenarios = [
        ("paddy_pixel", pixel_df, base.PIXEL_FEATURES, base.PIXEL_FEATURES + PIXEL_DEM_FEATURES, PIXEL_PER_CLASS),
        ("paddy_polygon", polygon_df, base.POLYGON_FEATURES, base.POLYGON_FEATURES + POLYGON_DEM_FEATURES, POLYGON_PER_CLASS),
    ]
    counts = []
    metrics = []
    scans = []
    for scenario, df, base_features, dem_features, n_per_class in scenarios:
        for feature_set, features in [("backscatter_only", base_features), ("backscatter_plus_gsi_dem5m", dem_features)]:
            valid = df.dropna(subset=features + [LABEL_COL]).copy()
            sampled = balanced_sample(valid, features, n_per_class)
            m, s, _ = evaluate(sampled, features, scenario, feature_set)
            metrics.append(m)
            scans.append(s)
            counts.append(
                {
                    "scenario": scenario,
                    "feature_set": feature_set,
                    "available_positive": int((valid[LABEL_COL] == 1).sum()),
                    "available_negative": int((valid[LABEL_COL] == 0).sum()),
                    "sampled_positive": int((sampled[LABEL_COL] == 1).sum()),
                    "sampled_negative": int((sampled[LABEL_COL] == 0).sum()),
                    "test_positive": int((sampled[LABEL_COL] == 1).sum() * TEST_SIZE),
                    "test_negative": int((sampled[LABEL_COL] == 0).sum() * TEST_SIZE),
                    "feature_count": len(features),
                }
            )

    counts_df = pd.DataFrame(counts)
    metrics_df = pd.concat(metrics, ignore_index=True)
    scans_df = pd.concat(scans, ignore_index=True)
    counts_df.to_csv(OUT / "GSI_DEM5m_母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics_df.to_csv(OUT / "GSI_DEM5m_全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans_df.to_csv(OUT / "GSI_DEM5m_閾値スキャン.csv", index=False, encoding="utf-8-sig")
    dem_source_stats.to_csv(OUT / "GSI_DEM5m_取得状況.csv", index=False, encoding="utf-8-sig")

    best = metrics_df.sort_values(["scenario", "feature_set", "mode", "balanced_accuracy"], ascending=[True, True, True, False])
    best = best.groupby(["scenario", "feature_set", "mode"]).head(1)
    overall_best = metrics_df.sort_values(["scenario", "feature_set", "balanced_accuracy"], ascending=[True, True, False])
    overall_best = overall_best.groupby(["scenario", "feature_set"]).head(1).copy()
    plot_best_comparison(overall_best)
    write_report(counts_df, metrics_df, dem_source_stats)
    print(best[["scenario", "feature_set", "mode", "model", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
