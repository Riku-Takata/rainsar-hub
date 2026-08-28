from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
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
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_paddy_satoyama_classification"
SRC = BASE / "bbox_balanced_classification"
SAT_CSV = Path(r"D:\sotsuron\20140114_ver1\20140114_ver1\msi_mesh3_zs_mean.csv")
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 3
LABEL_COL = "label"
MAX_PIXEL_PER_CLASS = 10000
MAX_POLYGON_PER_CLASS = 1000


def load_base_module():
    path = ROOT / "scripts/run_map7_bbox_balanced_classification.py"
    spec = importlib.util.spec_from_file_location("bbox_base", path)
    if spec is None or spec.loader is None:
        raise ImportError(path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


B = load_base_module()


PIXEL_FEATURES = B.PIXEL_FEATURES + ["satoyama_mean"]
POLYGON_FEATURES = B.POLYGON_FEATURES + [
    "satoyama_mean",
    "satoyama_median",
    "satoyama_std",
    "satoyama_min",
    "satoyama_max",
    "satoyama_range",
]


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


def mesh3_id_from_lonlat(lon: np.ndarray, lat: np.ndarray) -> np.ndarray:
    p = np.floor(lat * 1.5).astype(np.int64)
    q = np.floor(lon).astype(np.int64) - 100

    lat0 = p / 1.5
    lon0 = q + 100
    r = np.floor((lat - lat0) / (1.0 / 12.0)).astype(np.int64)
    s = np.floor((lon - lon0) / (1.0 / 8.0)).astype(np.int64)
    r = np.clip(r, 0, 7)
    s = np.clip(s, 0, 7)

    lat1 = lat0 + r * (1.0 / 12.0)
    lon1 = lon0 + s * (1.0 / 8.0)
    t = np.floor((lat - lat1) / (1.0 / 120.0)).astype(np.int64)
    u = np.floor((lon - lon1) / (1.0 / 80.0)).astype(np.int64)
    t = np.clip(t, 0, 9)
    u = np.clip(u, 0, 9)

    return p * 1_000_000 + q * 10_000 + r * 1000 + s * 100 + t * 10 + u


def satoyama_grid() -> np.ndarray:
    profile = B.template_profile()
    height = profile["height"]
    width = profile["width"]
    transform = profile["transform"]
    if str(profile["crs"]).upper() != "EPSG:4326":
        raise ValueError(f"Sentinel grid CRS must be EPSG:4326, got {profile['crs']}")

    sat = pd.read_csv(SAT_CSV, usecols=["MESH3_ID", "MEAN"])
    sat["MESH3_ID"] = sat["MESH3_ID"].astype(np.int64)
    sat = sat.dropna(subset=["MEAN"]).drop_duplicates("MESH3_ID")
    mesh_ids_sorted = sat["MESH3_ID"].to_numpy(np.int64)
    values_sorted = sat["MEAN"].to_numpy(np.float32)
    order = np.argsort(mesh_ids_sorted)
    mesh_ids_sorted = mesh_ids_sorted[order]
    values_sorted = values_sorted[order]

    cols = np.arange(width, dtype=np.float64)
    rows = np.arange(height, dtype=np.float64)
    xs = transform.c + (cols + 0.5) * transform.a
    ys = transform.f + (rows + 0.5) * transform.e
    lon, lat = np.meshgrid(xs, ys)
    mesh = mesh3_id_from_lonlat(lon, lat).ravel()
    idx = np.searchsorted(mesh_ids_sorted, mesh)
    valid = (idx < len(mesh_ids_sorted)) & (mesh_ids_sorted[np.minimum(idx, len(mesh_ids_sorted) - 1)] == mesh)
    out = np.full(mesh.shape, np.nan, dtype=np.float32)
    out[valid] = values_sorted[idx[valid]]
    return out.reshape(height, width)


def write_raster(path: Path, arr: np.ndarray, dtype: str = "float32", nodata=np.nan) -> None:
    profile = B.template_profile()
    profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr.astype(dtype), 1)


def build_bbox_mask() -> np.ndarray:
    features = B.load_geojson_features(B.BBOX_GEOJSON)
    profile = B.template_profile()
    return rasterize(
        ((f["geometry"], 1) for f in features if f.get("geometry")),
        out_shape=(profile["height"], profile["width"]),
        transform=profile["transform"],
        fill=0,
        dtype="uint8",
        all_touched=True,
    ).astype(bool)


def add_satoyama_to_pixel(df: pd.DataFrame, sat_grid: np.ndarray) -> pd.DataFrame:
    out = df.copy()
    out["satoyama_mean"] = sat_grid[out["row"].to_numpy(np.int32), out["col"].to_numpy(np.int32)]
    return out


def polygon_satoyama_stats(ids: np.ndarray, sat_grid: np.ndarray, bbox_mask: np.ndarray) -> pd.DataFrame:
    valid = bbox_mask & (ids > 0) & np.isfinite(sat_grid)
    flat_ids = ids[valid].astype(np.int32)
    vals = sat_grid[valid].astype(np.float32)
    if flat_ids.size == 0:
        return pd.DataFrame(columns=["feature_seq_id"])
    order = np.argsort(flat_ids)
    flat_ids = flat_ids[order]
    vals = vals[order]
    unique, starts, counts = np.unique(flat_ids, return_index=True, return_counts=True)
    rows = []
    for uid, start, count in zip(unique, starts, counts):
        v = vals[start : start + count]
        rows.append(
            {
                "feature_seq_id": int(uid),
                "satoyama_valid_pixel_count": int(count),
                "satoyama_mean": float(np.mean(v)),
                "satoyama_median": float(np.median(v)),
                "satoyama_std": float(np.std(v)),
                "satoyama_min": float(np.min(v)),
                "satoyama_max": float(np.max(v)),
                "satoyama_range": float(np.max(v) - np.min(v)),
            }
        )
    return pd.DataFrame(rows)


def balanced_sample(df: pd.DataFrame, features: list[str], n_limit: int) -> pd.DataFrame:
    df = df.dropna(subset=features + [LABEL_COL]).copy()
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(len(pos), len(neg), n_limit)
    if n < 10:
        raise ValueError("評価に必要なpositive/negative数が不足しています")
    return pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)


def model_specs_default() -> dict:
    return {
        "ランダムフォレスト": RandomForestClassifier(n_estimators=200, random_state=SEED, n_jobs=1),
        "XGBoost": XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
    }


def model_specs_grid() -> dict:
    return {
        "ランダムフォレスト": (
            RandomForestClassifier(random_state=SEED, n_jobs=1),
            {
                "n_estimators": [160, 240],
                "max_depth": [8, 12, None],
                "min_samples_leaf": [10, 30, 80],
                "max_features": ["sqrt", "log2"],
            },
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


def threshold_metrics(y_true: np.ndarray, prob: np.ndarray, model: str, mode: str, scenario: str) -> tuple[dict, pd.DataFrame]:
    rows = []
    auc = roc_auc_score(y_true, prob) if len(np.unique(y_true)) == 2 else np.nan
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.int32)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        rows.append(
            {
                "scenario": scenario,
                "mode": mode,
                "model": model,
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


def evaluate(sampled: pd.DataFrame, features: list[str], scenario: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    x = sampled[features].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)

    metric_rows = []
    scan_frames = []
    estimator_rows = []
    for model_name, model in model_specs_default().items():
        model.fit(x_train, y_train)
        prob = model.predict_proba(x_test)[:, 1]
        best, scan = threshold_metrics(y_test, prob, model_name, "通常設定", scenario)
        metric_rows.append(best)
        scan_frames.append(scan)
        estimator_rows.append(
            {
                "scenario": scenario,
                "mode": "通常設定",
                "model": model_name,
                "estimator": model,
                "threshold": float(best["threshold"]),
                "features": features,
                "balanced_accuracy": float(best["balanced_accuracy"]),
            }
        )

    for model_name, (estimator, grid) in model_specs_grid().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        prob = search.predict_proba(x_test)[:, 1]
        best, scan = threshold_metrics(y_test, prob, model_name, "GridSearch", scenario)
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        metric_rows.append(best)
        scan_frames.append(scan)
        estimator_rows.append(
            {
                "scenario": scenario,
                "mode": "GridSearch",
                "model": model_name,
                "estimator": search.best_estimator_,
                "threshold": float(best["threshold"]),
                "features": features,
                "balanced_accuracy": float(best["balanced_accuracy"]),
            }
        )
    return pd.DataFrame(metric_rows), pd.concat(scan_frames, ignore_index=True), pd.DataFrame(estimator_rows)


def feature_importance(estimators: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in estimators.iterrows():
        model = row["estimator"]
        features = row["features"]
        if hasattr(model, "feature_importances_"):
            vals = model.feature_importances_
        else:
            continue
        for feature, value in zip(features, vals):
            rows.append(
                {
                    "scenario": row["scenario"],
                    "mode": row["mode"],
                    "model": row["model"],
                    "feature": feature,
                    "importance": float(value),
                }
            )
    return pd.DataFrame(rows)


def read_baseline_metrics() -> pd.DataFrame:
    candidates = sorted(p for p in SRC.glob("*.csv") if "全モデル評価" in p.stem)
    if not candidates:
        return pd.DataFrame()
    df = pd.read_csv(candidates[0], encoding="utf-8-sig")
    df = df[df["scenario"].isin(["田んぼ画素_bbox内", "田んぼ筆ポリゴン_bbox内"]) & df["model"].isin(["ランダムフォレスト", "XGBoost"])].copy()
    df["feature_set"] = "後方散乱のみ"
    return df


def comparison_with_baseline(metrics: pd.DataFrame) -> pd.DataFrame:
    base = read_baseline_metrics()
    now = metrics.copy()
    now["feature_set"] = "後方散乱+さとやま指数"
    if base.empty:
        return now
    cols = ["feature_set", "scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN"]
    merged = pd.concat([base[cols], now[cols]], ignore_index=True)
    return merged


def satoyama_distribution(pixel_df: pd.DataFrame, poly_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for scope, df, col in [
        ("田んぼ画素", pixel_df, "satoyama_mean"),
        ("田んぼ筆ポリゴン", poly_df, "satoyama_mean"),
    ]:
        for label_value, label_name in [(1, "正解浸水"), (0, "非浸水")]:
            vals = df.loc[df[LABEL_COL] == label_value, col].dropna().to_numpy(np.float32)
            if vals.size == 0:
                continue
            rows.append(
                {
                    "scope": scope,
                    "label": label_name,
                    "count": int(vals.size),
                    "mean": float(np.mean(vals)),
                    "std": float(np.std(vals)),
                    "p25": float(np.percentile(vals, 25)),
                    "median": float(np.median(vals)),
                    "p75": float(np.percentile(vals, 75)),
                    "min": float(np.min(vals)),
                    "max": float(np.max(vals)),
                }
            )
    return pd.DataFrame(rows)


def plot_performance(comparison: pd.DataFrame) -> None:
    sub = comparison[comparison["mode"].isin(["GridSearch", "全特徴量_GridSearch"])].copy()
    if sub.empty:
        sub = comparison.copy()
    sub["mode"] = sub["mode"].replace({"全特徴量_GridSearch": "GridSearch", "全特徴量_通常設定": "通常設定"})
    sub = sub[sub["mode"] == "GridSearch"]
    for metric in ["balanced_accuracy", "F1", "ROC_AUC"]:
        fig, ax = plt.subplots(figsize=(10.5, 5.2))
        labels = []
        values = []
        colors = []
        for scenario in ["田んぼ画素_bbox内", "田んぼ筆ポリゴン_bbox内"]:
            for model in ["ランダムフォレスト", "XGBoost"]:
                for feature_set, color in [("後方散乱のみ", "#9ecae1"), ("後方散乱+さとやま指数", "#31a354")]:
                    row = sub[(sub["scenario"] == scenario) & (sub["model"] == model) & (sub["feature_set"] == feature_set)]
                    if row.empty:
                        continue
                    labels.append(f"{scenario.replace('_bbox内','')}\n{model}\n{feature_set}")
                    values.append(float(row[metric].iloc[0]))
                    colors.append(color)
        ax.bar(np.arange(len(values)), values, color=colors)
        ax.set_xticks(np.arange(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_ylim(0, 0.85)
        ax.set_ylabel(metric)
        ax.set_title(f"さとやま指数追加前後の{metric}比較")
        ax.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        fig.savefig(OUT / f"図_さとやま追加前後_{metric}比較.png", dpi=240, bbox_inches="tight")
        plt.close(fig)


def plot_satoyama_distribution(dist: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8.0, 4.8))
    x = np.arange(len(dist))
    ax.bar(x, dist["mean"], yerr=dist["std"], color=["#e34a33" if v == "正解浸水" else "#3182bd" for v in dist["label"]], alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels((dist["scope"] + "\n" + dist["label"]).tolist())
    ax.set_ylabel("さとやま指数 MEAN")
    ax.set_title("正解ラベル別のさとやま指数分布")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    fig.savefig(OUT / "図_正解ラベル別_さとやま指数分布.png", dpi=240, bbox_inches="tight")
    plt.close(fig)


def plot_feature_importance(imp: pd.DataFrame) -> None:
    if imp.empty:
        return
    for (scenario, mode, model), sub in imp.groupby(["scenario", "mode", "model"]):
        top = sub.sort_values("importance", ascending=False).head(12)
        fig, ax = plt.subplots(figsize=(8.0, 5.0))
        ax.barh(top["feature"][::-1], top["importance"][::-1], color="#756bb1")
        ax.set_title(f"{scenario} {mode} {model}\n特徴量重要度 Top12")
        ax.set_xlabel("importance")
        ax.grid(axis="x", alpha=0.3)
        safe = f"{scenario}_{mode}_{model}".replace("+", "plus").replace("/", "_").replace("\\", "_")
        plt.tight_layout()
        fig.savefig(OUT / f"図_特徴量重要度_{safe}.png", dpi=240, bbox_inches="tight")
        plt.close(fig)


def fmt_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    shown = shown.fillna("")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in shown.columns) + " |")
    return "\n".join(lines)


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame, comparison: pd.DataFrame, dist: pd.DataFrame, imp: pd.DataFrame) -> None:
    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    grid_compare = comparison[comparison["mode"].isin(["GridSearch", "全特徴量_GridSearch"])].copy()
    grid_compare["mode"] = grid_compare["mode"].replace({"全特徴量_GridSearch": "GridSearch"})
    lines = [
        "# さとやま指数を追加した浸水域判別レポート",
        "",
        "## 目的",
        "",
        "`msi_mesh3_zs_mean.csv` の3次メッシュ別さとやま指数を特徴量に追加し、田んぼ画素および田んぼ筆ポリゴン単位で浸水域/非浸水域の判別精度が改善するか確認した。",
        "",
        "## 使用データ",
        "",
        f"- さとやま指数: `{SAT_CSV}`",
        "- 列: `MESH3_ID`, `MEAN`",
        "- SentinelグリッドはEPSG:4326のため、画素中心の緯度経度から3次メッシュIDを計算して結合した。",
        "- 画素単位では `satoyama_mean` を追加した。",
        "- 筆ポリゴン単位では筆内画素の `satoyama_mean`, `median`, `std`, `min`, `max`, `range` を追加した。",
        "",
        "## 母数と抽出数",
        "",
        fmt_table(counts),
        "",
        "## さとやま指数追加モデルの評価",
        "",
        fmt_table(best[["scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_params"]]),
        "",
        "## 後方散乱のみとの比較",
        "",
        fmt_table(grid_compare[["feature_set", "scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]]),
        "",
        "## 正解ラベル別のさとやま指数分布",
        "",
        fmt_table(dist),
        "",
        "## さとやま関連特徴量の重要度",
        "",
    ]
    if imp.empty:
        lines.append("特徴量重要度は算出できなかった。")
    else:
        sat_imp = imp[imp["feature"].str.contains("satoyama", regex=False)].sort_values("importance", ascending=False)
        lines.append(fmt_table(sat_imp.head(30)))
    lines.extend(
        [
            "",
            "## 図",
            "",
            "- `図_さとやま追加前後_balanced_accuracy比較.png`",
            "- `図_さとやま追加前後_F1比較.png`",
            "- `図_さとやま追加前後_ROC_AUC比較.png`",
            "- `図_正解ラベル別_さとやま指数分布.png`",
            "- `図_特徴量重要度_*.png`",
            "",
            "## GIS/中間データ",
            "",
            "- `satoyama_index_on_sentinel_grid.tif`: Sentinelグリッドへ付与したさとやま指数",
            "- `評価指標_さとやま追加.csv`",
            "- `後方散乱のみ_vs_さとやま追加_比較.csv`",
            "- `さとやま指数_正解ラベル別分布.csv`",
            "- `特徴量重要度_さとやま追加.csv`",
        ]
    )
    (OUT / "さとやま指数_浸水判別レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)

    print("building satoyama grid...")
    sat_grid = satoyama_grid()
    write_raster(OUT / "satoyama_index_on_sentinel_grid.tif", sat_grid)

    bbox_mask = build_bbox_mask()
    paddy_df, shape = B.build_pixel_frame(B.LAND / "map7_paddy_mask.tif", "田んぼ画素_bbox内", bbox_mask)
    paddy_df = add_satoyama_to_pixel(paddy_df, sat_grid)

    polygon_df, polygon_ids, _ = B.build_polygon_frame(bbox_mask, shape)
    polygon_df["scenario"] = "田んぼ筆ポリゴン_bbox内"
    sat_poly = polygon_satoyama_stats(polygon_ids, sat_grid, bbox_mask)
    polygon_df = polygon_df.merge(sat_poly, on="feature_seq_id", how="left")

    scenarios = [
        ("田んぼ画素_bbox内", paddy_df, PIXEL_FEATURES, MAX_PIXEL_PER_CLASS),
        ("田んぼ筆ポリゴン_bbox内", polygon_df, POLYGON_FEATURES, MAX_POLYGON_PER_CLASS),
    ]

    counts_rows = []
    metric_frames = []
    scan_frames = []
    estimator_frames = []
    sample_frames = []
    for scenario, df, features, limit in scenarios:
        valid = df.dropna(subset=features + [LABEL_COL]).copy()
        sampled = balanced_sample(valid, features, limit)
        sampled["scenario"] = scenario
        sample_frames.append(sampled)
        counts_rows.append(
            {
                "scenario": scenario,
                "available_positive": int((valid[LABEL_COL] == 1).sum()),
                "available_negative": int((valid[LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[LABEL_COL] == 1).sum()),
                "sampled_negative": int((sampled[LABEL_COL] == 0).sum()),
                "feature_count": len(features),
                "satoyama_missing_rows_removed": int(len(df) - len(valid)),
            }
        )
        print(f"evaluating {scenario}...")
        metrics, scans, estimators = evaluate(sampled, features, scenario)
        metric_frames.append(metrics)
        scan_frames.append(scans)
        estimator_frames.append(estimators)

    counts = pd.DataFrame(counts_rows)
    metrics = pd.concat(metric_frames, ignore_index=True)
    scans = pd.concat(scan_frames, ignore_index=True)
    estimators = pd.concat(estimator_frames, ignore_index=True)
    sampled = pd.concat(sample_frames, ignore_index=True)
    imp = feature_importance(estimators)
    comparison = comparison_with_baseline(metrics)
    dist = satoyama_distribution(paddy_df.dropna(subset=["satoyama_mean"]), polygon_df.dropna(subset=["satoyama_mean"]))

    counts.to_csv(OUT / "母数と抽出数_さとやま追加.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "評価指標_さとやま追加.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン_さとやま追加.csv", index=False, encoding="utf-8-sig")
    sampled.to_csv(OUT / "抽出データ_さとやま追加.csv", index=False, encoding="utf-8-sig")
    comparison.to_csv(OUT / "後方散乱のみ_vs_さとやま追加_比較.csv", index=False, encoding="utf-8-sig")
    dist.to_csv(OUT / "さとやま指数_正解ラベル別分布.csv", index=False, encoding="utf-8-sig")
    imp.to_csv(OUT / "特徴量重要度_さとやま追加.csv", index=False, encoding="utf-8-sig")

    plot_performance(comparison)
    plot_satoyama_distribution(dist)
    plot_feature_importance(imp)
    write_report(counts, metrics, comparison, dist, imp)

    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    print(best[["scenario", "mode", "model", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
