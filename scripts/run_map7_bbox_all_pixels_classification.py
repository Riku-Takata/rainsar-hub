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
OUT = BASE / "bbox_all_pixels_classification"
BBOX_MASK = BASE / "bbox_balanced_classification/GIS_bbox_union_mask.tif"
BBOX_POLYGON_GEOJSON = BASE / "bbox_balanced_classification/GIS_bbox_田んぼ筆ポリゴン_浸水判定.geojson"
BBOX_POLYGON_CSV = BASE / "bbox_balanced_classification/GIS_bbox_田んぼ筆ポリゴン_浸水判定一覧.csv"
PADDY_GEOJSON = ROOT / "output/gsi_h30_geojson_s1/map7_land_polygons/map7_fude_paddy_polygons_from_db.geojson"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 3
LABEL_COL = "label"

DIFF_RASTERS = {
    "0_3h": BASE / "map7_mean_diff_0_3h.tif",
    "3_6h": BASE / "map7_mean_diff_3_6h.tif",
    "6_12h": BASE / "map7_mean_diff_6_12h.tif",
    "12_24h": BASE / "map7_mean_diff_12_24h.tif",
}

FEATURES = [
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


def add_features(df: pd.DataFrame) -> pd.DataFrame:
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


def build_all_pixel_frame() -> tuple[pd.DataFrame, tuple[int, int]]:
    d0 = read_float(DIFF_RASTERS["0_3h"])
    d3 = read_float(DIFF_RASTERS["3_6h"])
    d6 = read_float(DIFF_RASTERS["6_12h"])
    d12 = read_float(DIFF_RASTERS["12_24h"])
    bbox = read_bool(BBOX_MASK)
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
    return add_features(df), d0.shape


def balanced_sample(df: pd.DataFrame) -> pd.DataFrame:
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(len(pos), len(neg))
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
            {"max_depth": [5, 8, None], "min_samples_leaf": [30, 80], "class_weight": [None, "balanced"]},
        ),
        "ランダムフォレスト": (
            RandomForestClassifier(random_state=SEED, n_jobs=1),
            {"n_estimators": [160, 240], "max_depth": [10, None], "min_samples_leaf": [30, 80], "max_features": ["sqrt"]},
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


def threshold_scan(y_true: np.ndarray, prob: np.ndarray, model_name: str, mode: str) -> tuple[dict, pd.DataFrame]:
    rows = []
    auc = roc_auc_score(y_true, prob)
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.int32)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        rows.append(
            {
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


def evaluate(sampled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    x = sampled[FEATURES].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    rows = []
    scans = []
    best_grid = None

    for model_name, model in model_specs_default().items():
        model.fit(x_train, y_train)
        best, scan = threshold_scan(y_test, model.predict_proba(x_test)[:, 1], model_name, "全特徴量_通常設定")
        rows.append(best)
        scans.append(scan)

    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)
    for model_name, (estimator, grid) in model_specs_grid().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        best, scan = threshold_scan(y_test, search.predict_proba(x_test)[:, 1], model_name, "全特徴量_GridSearch")
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        rows.append(best)
        scans.append(scan)
        candidate = {
            "model_name": model_name,
            "estimator": search.best_estimator_,
            "threshold": float(best["threshold"]),
            "balanced_accuracy": float(best["balanced_accuracy"]),
        }
        if best_grid is None or candidate["balanced_accuracy"] > best_grid["balanced_accuracy"]:
            best_grid = candidate

    return pd.DataFrame(rows), pd.concat(scans, ignore_index=True), best_grid


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


def plot_summary(metrics: pd.DataFrame) -> None:
    best = metrics.sort_values(["mode", "balanced_accuracy"], ascending=[True, False]).groupby("mode").head(1)
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
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
    ax.set_xticklabels(best["mode"].str.replace("全特徴量_", "", regex=False).tolist(), fontproperties=FONT)
    ax.set_ylim(0.4, 0.85)
    ax.set_ylabel("スコア", fontproperties=FONT)
    ax.set_title("bbox内全画素を対象にした浸水判別", fontproperties=FONT)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.18))
    plt.tight_layout()
    plt.savefig(OUT / "図_全画素モデル性能比較.png", dpi=200, bbox_inches="tight")
    plt.close()

    for _, row in best.iterrows():
        plot_confusion(row, OUT / f"混同行列_{row['mode']}_{row['model']}.png", f"{row['mode']} {row['model']}")


def export_prediction(full_df: pd.DataFrame, model_info: dict, shape: tuple[int, int]) -> None:
    prob = model_info["estimator"].predict_proba(full_df[FEATURES].to_numpy(np.float32))[:, 1]
    pred = prob >= model_info["threshold"]
    prob_arr = np.full(shape, np.nan, dtype=np.float32)
    pred_arr = np.zeros(shape, dtype=np.uint8)
    truth_arr = np.zeros(shape, dtype=np.uint8)
    rows = full_df["row"].to_numpy(np.int32)
    cols = full_df["col"].to_numpy(np.int32)
    prob_arr[rows, cols] = prob.astype(np.float32)
    pred_arr[rows, cols] = pred.astype(np.uint8)
    truth_arr[rows, cols] = full_df[LABEL_COL].to_numpy(np.uint8)
    write_raster(OUT / "GIS_bbox全画素_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_bbox全画素_浸水判定.tif", pred_arr, "uint8", 0)
    write_raster(OUT / "GIS_bbox全画素_正解浸水域.tif", truth_arr, "uint8", 0)
    out = full_df[["row", "col", LABEL_COL]].copy()
    out["predicted_probability"] = prob
    out["predicted_inundated"] = pred.astype(np.uint8)
    out.to_csv(OUT / "GIS_bbox全画素_浸水判定一覧.csv", index=False, encoding="utf-8-sig")


def make_polygon_unit_display_raster() -> None:
    bbox_out = BASE / "bbox_balanced_classification"
    if not BBOX_POLYGON_GEOJSON.exists():
        return
    features = json.loads(BBOX_POLYGON_GEOJSON.read_text(encoding="utf-8")).get("features", [])
    profile = template_profile()
    display = rasterize(
        ((f["geometry"], 1) for f in features if f.get("geometry")),
        out_shape=(profile["height"], profile["width"]),
        transform=profile["transform"],
        fill=0,
        dtype="uint8",
        all_touched=True,
    )
    write_raster(bbox_out / "GIS_bbox_田んぼ筆ポリゴン_浸水判定_筆単位表示.tif", display, "uint8", 0)


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


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame, best_grid: dict) -> None:
    best = metrics.sort_values(["mode", "balanced_accuracy"], ascending=[True, False]).groupby("mode").head(1)
    report = [
        "# bbox内全画素を対象にした浸水域判別レポート",
        "",
        "## 条件",
        "",
        "- 田んぼ・道路マスクは使わず、bbox内にあり、4つの経過時間帯すべてで差分値が有効な全画素を対象にしました。",
        "- 正解浸水域は既存のTIF値に基づく `map7_inundation_truth_mask.tif` をそのまま使用しました。",
        "- 非浸水域は、bbox内の有効画素から正解浸水域を除いたすべての画素です。",
        "- 学習・検証では、浸水域と非浸水域の画素数を同数にそろえました。",
        "- GridSearchの最良モデルをbbox内全有効画素へ適用し、GISデータを作成しました。",
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## 最良モデル",
        "",
        md_table(best[["mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_params"]]),
        "",
        "## 全モデル結果",
        "",
        md_table(metrics[["mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "best_cv_balanced_accuracy", "best_params"]]),
        "",
        "## GIS出力",
        "",
        "- `GIS_bbox全画素_浸水判定.tif`",
        "- `GIS_bbox全画素_浸水確率.tif`",
        "- `GIS_bbox全画素_正解浸水域.tif`",
        "- `GIS_bbox全画素_浸水判定一覧.csv`",
        "",
        "## 筆ポリゴン表示補足",
        "",
        "- 既存の筆ポリゴン判定GeoJSONは1筆単位の判定です。",
        "- 表示用に `GIS_bbox_田んぼ筆ポリゴン_浸水判定_筆単位表示.tif` を追加しました。これは予測された筆ポリゴン全体を `all_touched=True` でラスタ化したものです。",
        "",
    ]
    (OUT / "bbox内全画素_浸水判別レポート.md").write_text("\n".join(report), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    full_df, shape = build_all_pixel_frame()
    sampled = balanced_sample(full_df)
    counts = pd.DataFrame(
        [
            {
                "target": "bbox内全有効画素",
                "available_positive": int((full_df[LABEL_COL] == 1).sum()),
                "available_negative": int((full_df[LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[LABEL_COL] == 1).sum()),
                "sampled_negative": int((sampled[LABEL_COL] == 0).sum()),
                "feature_count": len(FEATURES),
            }
        ]
    )
    metrics, scans, best_grid = evaluate(sampled)
    counts.to_csv(OUT / "母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン.csv", index=False, encoding="utf-8-sig")
    sampled.to_csv(OUT / "抽出データ.csv", index=False, encoding="utf-8-sig")
    export_prediction(full_df, best_grid, shape)
    make_polygon_unit_display_raster()
    plot_summary(metrics)
    write_report(counts, metrics, best_grid)
    best = metrics.sort_values(["mode", "balanced_accuracy"], ascending=[True, False]).groupby("mode").head(1)
    print(best[["mode", "model", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
