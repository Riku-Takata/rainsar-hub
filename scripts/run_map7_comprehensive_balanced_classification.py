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
OUT = BASE / "comprehensive_balanced_classification"
POLYGON_FEATURE_CSV = BASE / "paddy_polygon_1000_classification/map7_paddy_polygon_features_all.csv"
PADDY_GEOJSON = ROOT / "output/gsi_h30_geojson_s1/map7_land_polygons/map7_fude_paddy_polygons_from_db.geojson"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 3
LABEL_COL = "label"
MAX_PIXEL_PER_CLASS = 10000
MAX_POLYGON_PER_CLASS = 1000


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


def build_pixel_frame(mask_path: Path, scenario: str) -> tuple[pd.DataFrame, tuple[int, int]]:
    d0 = read_float(BASE / "map7_mean_diff_0_3h.tif")
    d3 = read_float(BASE / "map7_mean_diff_3_6h.tif")
    d6 = read_float(BASE / "map7_mean_diff_6_12h.tif")
    d12 = read_float(BASE / "map7_mean_diff_12_24h.tif")
    truth = read_bool(BASE / "map7_inundation_truth_mask.tif")
    mask = read_bool(mask_path)
    valid = np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12) & mask
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
    return df, d0.shape


def build_polygon_frame() -> pd.DataFrame:
    df = pd.read_csv(POLYGON_FEATURE_CSV, encoding="utf-8-sig")
    df = df.dropna(subset=POLYGON_FEATURES + ["truth_pixel_ratio"]).copy()
    df["scenario"] = "田んぼ筆ポリゴン"
    df[LABEL_COL] = (df["truth_pixel_ratio"] > 0).astype(np.uint8)
    return df


def balanced_sample(df: pd.DataFrame, features: list[str], max_per_class: int) -> pd.DataFrame:
    df = df.dropna(subset=features + [LABEL_COL]).copy()
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(len(pos), len(neg), max_per_class)
    sampled = pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)
    return sampled


def model_specs_default() -> dict:
    return {
        "ロジスティック回帰": Pipeline(
            [("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]
        ),
        "決定木": DecisionTreeClassifier(random_state=SEED),
        "ランダムフォレスト": RandomForestClassifier(n_estimators=200, random_state=SEED, n_jobs=1),
        "XGBoost": XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
    }


def model_specs_grid() -> dict:
    return {
        "ロジスティック回帰": (
            Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
            {"model__C": [0.1, 1.0], "model__class_weight": [None, "balanced"]},
        ),
        "決定木": (
            DecisionTreeClassifier(random_state=SEED),
            {"max_depth": [5, 8], "min_samples_leaf": [30, 80], "class_weight": [None]},
        ),
        "ランダムフォレスト": (
            RandomForestClassifier(random_state=SEED, n_jobs=1),
            {"n_estimators": [160], "max_depth": [10, None], "min_samples_leaf": [30, 80], "max_features": ["sqrt"]},
        ),
        "XGBoost": (
            XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            {"n_estimators": [160], "max_depth": [2, 3], "learning_rate": [0.03, 0.05], "min_child_weight": [3], "subsample": [0.85], "colsample_bytree": [0.85]},
        ),
    }


def threshold_metrics(y_true: np.ndarray, prob: np.ndarray, model_name: str, mode: str, scenario: str) -> tuple[dict, pd.DataFrame]:
    rows = []
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
                "ROC_AUC": roc_auc_score(y_true, prob),
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
    best_estimators = []

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
        best_estimators.append(
            {
                "scenario": scenario,
                "model": model_name,
                "estimator": search.best_estimator_,
                "threshold": float(best["threshold"]),
                "balanced_accuracy": float(best["balanced_accuracy"]),
                "features": features,
            }
        )

    metrics = pd.DataFrame(metric_rows).sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False])
    scans = pd.concat(scan_frames, ignore_index=True)
    estimators = pd.DataFrame(best_estimators)
    return metrics, scans, estimators


def write_raster(path: Path, data: np.ndarray, dtype: str, nodata) -> None:
    with rasterio.open(BASE / "map7_mean_diff_0_3h.tif") as src:
        profile = src.profile.copy()
    profile.update(count=1, dtype=dtype, nodata=nodata, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)


def load_paddy_features() -> list[dict]:
    return json.loads(PADDY_GEOJSON.read_text(encoding="utf-8")).get("features", [])


def rasterize_polygon_ids(features: list[dict], shape: tuple[int, int]) -> np.ndarray:
    with rasterio.open(BASE / "map7_mean_diff_0_3h.tif") as src:
        transform = src.transform
    return rasterize(
        ((f["geometry"], idx) for idx, f in enumerate(features, start=1) if f.get("geometry")),
        out_shape=shape,
        transform=transform,
        fill=0,
        dtype="int32",
        all_touched=False,
    )


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
    write_raster(OUT / "GIS_田んぼ画素_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_田んぼ画素_浸水判定.tif", pred_arr, "uint8", 0)
    out_df = paddy_df[["row", "col", LABEL_COL]].copy()
    out_df["predicted_probability"] = prob
    out_df["predicted_inundated"] = pred.astype(np.uint8)
    out_df.to_csv(OUT / "GIS_田んぼ画素_浸水判定一覧.csv", index=False, encoding="utf-8-sig")


def export_paddy_polygon_gis(poly_df: pd.DataFrame, estimator_row: pd.Series, shape: tuple[int, int]) -> None:
    features_cols = estimator_row["features"]
    model = estimator_row["estimator"]
    threshold = float(estimator_row["threshold"])
    prob = model.predict_proba(poly_df[features_cols].to_numpy(np.float32))[:, 1]
    pred = prob >= threshold
    out_df = poly_df[
        ["feature_seq_id", "polygon_uuid", "local_government_cd", "pref_id", "land_type", "truth_pixel_count", "sentinel_pixel_count", "truth_pixel_ratio", LABEL_COL]
    ].copy()
    out_df["predicted_probability"] = prob
    out_df["predicted_inundated"] = pred.astype(np.uint8)
    out_df.to_csv(OUT / "GIS_田んぼ筆ポリゴン_浸水判定一覧.csv", index=False, encoding="utf-8-sig")

    source_features = load_paddy_features()
    selected_features = []
    pred_by_id = dict(zip(out_df["feature_seq_id"].astype(int), pred.astype(bool)))
    prob_by_id = dict(zip(out_df["feature_seq_id"].astype(int), prob.astype(float)))
    for _, row in out_df[out_df["predicted_inundated"] == 1].iterrows():
        seq_id = int(row["feature_seq_id"])
        f = source_features[seq_id - 1]
        props = dict(f.get("properties", {}))
        props.update(
            {
                "predicted_probability": float(prob_by_id[seq_id]),
                "predicted_inundated": 1,
                "truth_pixel_count": int(row["truth_pixel_count"]),
                "sentinel_pixel_count": int(row["sentinel_pixel_count"]),
                "truth_pixel_ratio": float(row["truth_pixel_ratio"]),
            }
        )
        selected_features.append({"type": "Feature", "geometry": f["geometry"], "properties": props})
    (OUT / "GIS_田んぼ筆ポリゴン_浸水判定.geojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": selected_features}, ensure_ascii=False),
        encoding="utf-8",
    )

    polygon_ids = rasterize_polygon_ids(source_features, shape)
    selected_ids = set(int(i) for i in out_df.loc[out_df["predicted_inundated"] == 1, "feature_seq_id"])
    pred_arr = np.isin(polygon_ids, list(selected_ids)).astype(np.uint8)
    prob_arr = np.full(shape, np.nan, dtype=np.float32)
    for seq_id, p in prob_by_id.items():
        prob_arr[polygon_ids == seq_id] = np.float32(p)
    write_raster(OUT / "GIS_田んぼ筆ポリゴン_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_田んぼ筆ポリゴン_浸水判定.tif", pred_arr, "uint8", 0)


def plot_confusion(row: pd.Series, path: Path, title: str) -> None:
    cm = np.array([[int(row["TP"]), int(row["FN"])], [int(row["FP"]), int(row["TN"])]])
    fig, ax = plt.subplots(figsize=(5.2, 4.4))
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


def plot_results(metrics: pd.DataFrame) -> None:
    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    best["条件"] = best["scenario"] + "\n" + best["mode"].str.replace("全特徴量_", "", regex=False)
    fig, ax = plt.subplots(figsize=(11, 5.5))
    x = np.arange(len(best))
    width = 0.18
    for i, (col, label, color) in enumerate(
        [
            ("balanced_accuracy", "Balanced Acc.", "#4c78a8"),
            ("precision", "Precision", "#f58518"),
            ("recall", "Recall", "#54a24b"),
            ("ROC_AUC", "AUC", "#e45756"),
        ]
    ):
        ax.bar(x + (i - 1.5) * width, best[col], width, label=label, color=color)
    ax.set_xticks(x)
    ax.set_xticklabels(best["条件"], fontproperties=FONT)
    ax.set_ylim(0.45, 0.85)
    ax.set_ylabel("スコア", fontproperties=FONT)
    ax.set_title("画素単位・筆単位の浸水域判別精度比較", fontproperties=FONT)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.18))
    plt.tight_layout()
    plt.savefig(OUT / "図_条件別モデル性能比較.png", dpi=200, bbox_inches="tight")
    plt.close()

    for _, row in best.iterrows():
        safe = f"{row['scenario']}_{row['mode']}_{row['model']}".replace("/", "_").replace("\n", "_")
        plot_confusion(row, OUT / f"混同行列_{safe}.png", f"{row['scenario']} {row['mode']} {row['model']}")


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
        "# map7 浸水域判別 総合比較",
        "",
        "## 対象条件",
        "",
        "- 道路+田んぼ画素: `map7_paddy_or_road_mask.tif` 内の画素",
        "- 田んぼ画素: `map7_paddy_mask.tif` 内の画素",
        "- 田んぼ筆ポリゴン: 正解浸水域と1画素以上重なる筆を positive",
        "- すべて positive / negative の数を同数に揃えて評価",
        f"- 画素単位は最大 {MAX_PIXEL_PER_CLASS:,} / {MAX_PIXEL_PER_CLASS:,}、筆単位は最大 {MAX_POLYGON_PER_CLASS:,} / {MAX_POLYGON_PER_CLASS:,} で抽出",
        "- `全特徴量_通常設定`: 全特徴量を用いるが、パラメータチューニングなし",
        "- `全特徴量_GridSearch`: 全特徴量を用い、GridSearchCVでパラメータチューニング",
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## 各条件の最良モデル",
        "",
        md_table(best[["scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN"]]),
        "",
        "## 全モデル結果",
        "",
        md_table(metrics[["scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]]),
        "",
        "## GIS出力",
        "",
        "- `GIS_田んぼ画素_浸水判定.tif`",
        "- `GIS_田んぼ画素_浸水確率.tif`",
        "- `GIS_田んぼ画素_浸水判定一覧.csv`",
        "- `GIS_田んぼ筆ポリゴン_浸水判定.geojson`",
        "- `GIS_田んぼ筆ポリゴン_浸水判定.tif`",
        "- `GIS_田んぼ筆ポリゴン_浸水確率.tif`",
        "- `GIS_田んぼ筆ポリゴン_浸水判定一覧.csv`",
        "",
    ]
    (OUT / "総合比較レポート.md").write_text("\n".join(report), encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    paddy_or_road_df, shape = build_pixel_frame(LAND / "map7_paddy_or_road_mask.tif", "道路+田んぼ画素")
    paddy_df, _ = build_pixel_frame(LAND / "map7_paddy_mask.tif", "田んぼ画素")
    polygon_df = build_polygon_frame()

    scenario_items = [
        ("道路+田んぼ画素", paddy_or_road_df, PIXEL_FEATURES, MAX_PIXEL_PER_CLASS),
        ("田んぼ画素", paddy_df, PIXEL_FEATURES, MAX_PIXEL_PER_CLASS),
        ("田んぼ筆ポリゴン", polygon_df, POLYGON_FEATURES, MAX_POLYGON_PER_CLASS),
    ]

    count_rows = []
    metric_frames = []
    scan_frames = []
    estimator_rows = []
    sampled_frames = []
    for scenario, df, features, max_per_class in scenario_items:
        sampled = balanced_sample(df, features, max_per_class)
        sampled["scenario"] = scenario
        sampled_frames.append(sampled)
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
        estimator_rows.append(estimators)

    counts = pd.DataFrame(count_rows)
    metrics = pd.concat(metric_frames, ignore_index=True)
    scans = pd.concat(scan_frames, ignore_index=True)
    estimators = pd.concat(estimator_rows, ignore_index=True)
    sampled_all = pd.concat(sampled_frames, ignore_index=True)

    counts.to_csv(OUT / "母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン.csv", index=False, encoding="utf-8-sig")
    sampled_all.to_csv(OUT / "抽出データ.csv", index=False, encoding="utf-8-sig")

    # GIS outputs use best GridSearch model for paddy pixel and polygon.
    paddy_est = estimators[estimators["scenario"] == "田んぼ画素"].sort_values("balanced_accuracy", ascending=False).iloc[0]
    poly_est = estimators[estimators["scenario"] == "田んぼ筆ポリゴン"].sort_values("balanced_accuracy", ascending=False).iloc[0]
    export_paddy_pixel_gis(paddy_df, paddy_est, shape)
    export_paddy_polygon_gis(polygon_df, poly_est, shape)

    plot_results(metrics)
    write_report(counts, metrics)
    print(metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1).to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
