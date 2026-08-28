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
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/kanoya_rain_s1"
DIFF_DIR = BASE / "kurume_signature_diff_analysis"
PADDY_REPORT = BASE / "kanoya_paddy_inundation_model_report"
OUT = BASE / "kanoya_paddy_polygon_unit_report"
PADDY_GEOJSON = PADDY_REPORT / "kanoya_fude_paddy_polygons_from_db.geojson"
TRUTH_MASK = DIFF_DIR / "kanoya_inundation_mask_0p5_1p7_on_diff_scene.tif"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
LABEL_COL = "label"

DIFF_RASTERS = {
    "0_3h": DIFF_DIR / "kanoya_mean_diff_0_3h.tif",
    "3_6h": DIFF_DIR / "kanoya_mean_diff_3_6h.tif",
    "6_12h": DIFF_DIR / "kanoya_mean_diff_6_12h.tif",
    "12_24h": DIFF_DIR / "kanoya_mean_diff_12_24h.tif",
}

FEATURES = [
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


def build_polygon_frame() -> tuple[pd.DataFrame, np.ndarray, gpd.GeoDataFrame]:
    features = load_features(PADDY_GEOJSON)
    profile = template_profile()
    shape = (profile["height"], profile["width"])
    ids = rasterize_polygon_ids(features, shape)
    truth = read_bool(TRUTH_MASK)
    diffs = {name: read_float(path) for name, path in DIFF_RASTERS.items()}
    valid = ids > 0
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
    profile_values = df[["diff_0_3h_mean", "diff_3_6h_mean", "diff_6_12h_mean", "diff_12_24h_mean"]].to_numpy(np.float32)
    df["profile_mean"] = np.nanmean(profile_values, axis=1)
    df["profile_std"] = np.nanstd(profile_values, axis=1)
    df["profile_range"] = np.nanmax(profile_values, axis=1) - np.nanmin(profile_values, axis=1)
    df["negative_bin_count"] = (profile_values < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h_mean"] >= df["diff_3_6h_mean"]).astype(int)
        + (df["diff_3_6h_mean"] >= df["diff_6_12h_mean"]).astype(int)
        + (df["diff_6_12h_mean"] <= df["diff_12_24h_mean"]).astype(int)
    )

    gdf = gpd.read_file(PADDY_GEOJSON).reset_index().rename(columns={"index": "feature_index"})
    gdf["feature_seq_id"] = gdf["feature_index"] + 1
    return df, ids, gdf


def balanced_sample(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=FEATURES + [LABEL_COL]).copy()
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(len(pos), len(neg))
    if n < 8:
        return pd.DataFrame()
    return pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)


def model_specs() -> dict:
    return {
        "ロジスティック回帰": (
            Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
            {"model__C": [0.05, 0.5, 1.0, 10.0], "model__class_weight": [None, "balanced"]},
        ),
        "ランダムフォレスト": (
            RandomForestClassifier(random_state=SEED, n_jobs=1),
            {"n_estimators": [160, 240], "max_depth": [6, 10, None], "min_samples_leaf": [5, 10, 20], "max_features": ["sqrt"]},
        ),
        "XGBoost": (
            XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            {
                "n_estimators": [120, 200],
                "max_depth": [2, 3],
                "learning_rate": [0.03, 0.05],
                "min_child_weight": [1, 3],
                "subsample": [0.85],
                "colsample_bytree": [0.85],
            },
        ),
    }


def threshold_scan(y_true: np.ndarray, prob: np.ndarray, model_name: str) -> tuple[dict, pd.DataFrame]:
    rows = []
    auc = roc_auc_score(y_true, prob)
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.int32)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        rows.append(
            {
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


def evaluate(sampled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict | None]:
    if sampled.empty:
        return pd.DataFrame(), pd.DataFrame(), None
    x = sampled[FEATURES].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    cv_splits = min(5, int(min(np.bincount(y_train))))
    cv = StratifiedKFold(n_splits=cv_splits, shuffle=True, random_state=SEED)
    rows = []
    scans = []
    best_est = None
    for model_name, (estimator, grid) in model_specs().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        prob = search.predict_proba(x_test)[:, 1]
        best, scan = threshold_scan(y_test, prob, model_name)
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        rows.append(best)
        scans.append(scan)
        candidate = {
            "estimator": search.best_estimator_,
            "threshold": float(best["threshold"]),
            "features": FEATURES,
            "balanced_accuracy": float(best["balanced_accuracy"]),
            "model": model_name,
        }
        if best_est is None or candidate["balanced_accuracy"] > best_est["balanced_accuracy"]:
            best_est = candidate
    return pd.DataFrame(rows), pd.concat(scans, ignore_index=True), best_est


def predict(df: pd.DataFrame, best_est: dict | None) -> pd.DataFrame:
    out = df.dropna(subset=FEATURES + [LABEL_COL]).copy()
    if best_est is None:
        out["predicted_probability"] = np.nan
        out["predicted_inundated"] = 0
        return out
    prob = best_est["estimator"].predict_proba(out[FEATURES].to_numpy(np.float32))[:, 1]
    out["predicted_probability"] = prob
    out["predicted_inundated"] = (prob >= best_est["threshold"]).astype(np.uint8)
    return out


def export_gis(pred_df: pd.DataFrame, ids: np.ndarray, gdf: gpd.GeoDataFrame) -> None:
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
    merged.to_file(OUT / "GIS_鹿屋田んぼ筆ポリゴン_全判定.geojson", driver="GeoJSON")
    selected.to_file(OUT / "GIS_鹿屋田んぼ筆ポリゴン_浸水判定.geojson", driver="GeoJSON")
    selected.drop(columns="geometry").to_csv(OUT / "GIS_鹿屋田んぼ筆ポリゴン_浸水判定一覧.csv", index=False, encoding="utf-8-sig")

    prob_arr = np.full(ids.shape, np.nan, dtype=np.float32)
    pred_arr = np.zeros(ids.shape, dtype=np.uint8)
    prob_by_id = dict(zip(pred_df["feature_seq_id"].astype(int), pred_df["predicted_probability"].astype(float)))
    selected_ids = set(int(x) for x in selected["feature_seq_id"])
    for seq_id, prob in prob_by_id.items():
        prob_arr[ids == seq_id] = np.float32(prob)
    pred_arr[np.isin(ids, list(selected_ids))] = 1
    write_raster(OUT / "GIS_鹿屋田んぼ筆ポリゴン_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_鹿屋田んぼ筆ポリゴン_浸水判定.tif", pred_arr, "uint8", 0)

    fig, ax = plt.subplots(figsize=(8, 8))
    if not merged.empty:
        merged.boundary.plot(ax=ax, color="#c8c8c8", linewidth=0.15)
    if not selected.empty:
        selected.plot(ax=ax, color="#e45756", edgecolor="#8b1a1a", linewidth=0.25, alpha=0.85)
    ax.set_title("鹿屋: 浸水と判定された田んぼ筆ポリゴン", fontproperties=FONT)
    ax.set_axis_off()
    plt.tight_layout()
    plt.savefig(OUT / "図_鹿屋田んぼ筆ポリゴン_浸水判定.png", dpi=250, bbox_inches="tight")
    plt.close()


def plot_confusion(row: pd.Series) -> None:
    cm = np.array([[int(row["TP"]), int(row["FN"])], [int(row["FP"]), int(row["TN"])]])
    fig, ax = plt.subplots(figsize=(5.6, 4.8))
    im = ax.imshow(cm, cmap="Blues")
    labels = [["TP\n浸水を検出", "FN\n浸水を未検出"], ["FP\n非浸水を誤検出", "TN\n非浸水を非検出"]]
    max_v = max(cm.max(), 1)
    for i in range(2):
        for j in range(2):
            color = "white" if cm[i, j] > max_v * 0.55 else "black"
            ax.text(j, i, f"{labels[i][j]}\n{cm[i, j]:,}", ha="center", va="center", color=color, fontproperties=FONT)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["浸水と予測", "非浸水と予測"], fontproperties=FONT)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["実際に浸水", "実際に非浸水"], fontproperties=FONT)
    ax.set_title("鹿屋 田んぼ筆ポリゴン単位 混同行列", fontproperties=FONT)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    plt.tight_layout()
    plt.savefig(OUT / "混同行列_鹿屋田んぼ筆ポリゴン.png", dpi=220, bbox_inches="tight")
    plt.close()


def plot_metrics(metrics: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.8))
    best = metrics.sort_values("balanced_accuracy", ascending=False).copy()
    labels = best["model"].tolist()
    x = np.arange(len(best))
    width = 0.18
    for i, (col, name, color) in enumerate(
        [
            ("balanced_accuracy", "BA", "#4c78a8"),
            ("precision", "Precision", "#f58518"),
            ("recall", "Recall", "#54a24b"),
            ("ROC_AUC", "AUC", "#e45756"),
        ]
    ):
        ax.bar(x + (i - 1.5) * width, best[col], width, label=name, color=color)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontproperties=FONT)
    ax.set_ylim(0.3, 1.0)
    ax.set_title("鹿屋 田んぼ筆ポリゴン単位 モデル性能", fontproperties=FONT)
    ax.set_ylabel("スコア", fontproperties=FONT)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.18))
    plt.tight_layout()
    plt.savefig(OUT / "図_鹿屋田んぼ筆ポリゴン_モデル性能.png", dpi=220, bbox_inches="tight")
    plt.close()


def md_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    shown = shown.fillna("")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in shown.columns) + " |")
    return "\n".join(lines)


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame, pred_df: pd.DataFrame) -> None:
    best = metrics.sort_values("balanced_accuracy", ascending=False).head(1)
    pred_counts = pd.DataFrame(
        [
            {
                "all_polygons": len(pred_df),
                "truth_inundated_polygons": int(pred_df[LABEL_COL].sum()),
                "predicted_inundated_polygons": int(pred_df["predicted_inundated"].sum()),
            }
        ]
    )
    lines = [
        "# 鹿屋 田んぼ筆ポリゴン単位 浸水判定レポート",
        "",
        "## 条件",
        "",
        "- 正解浸水域: `D:/sotsuron/kanoya/Inun_shinkawacho.tif` の `0.5 <= 値 <= 1.7` を差分ラスタグリッドへ投影したマスク",
        "- 対象: DBから取得済みの田んぼ筆ポリゴン",
        "- 単位: 1筆ポリゴンごと",
        "- 特徴量: 各筆内の後方散乱強度差分の平均・標準偏差と、時間変化特徴",
        "- 評価: positive / negative を同数抽出し、train/test = 7:3、GridSearchCVでチューニング",
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## 最良モデル",
        "",
        md_table(best[["model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_cv_balanced_accuracy", "best_params"]]),
        "",
        "## 全モデル結果",
        "",
        md_table(metrics[["model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "best_cv_balanced_accuracy", "best_params"]]),
        "",
        "## 全筆へ適用した判定数",
        "",
        md_table(pred_counts),
        "",
        "## 出力",
        "",
        "- `GIS_鹿屋田んぼ筆ポリゴン_浸水判定.geojson`",
        "- `GIS_鹿屋田んぼ筆ポリゴン_全判定.geojson`",
        "- `GIS_鹿屋田んぼ筆ポリゴン_浸水判定.tif`",
        "- `GIS_鹿屋田んぼ筆ポリゴン_浸水確率.tif`",
        "- `図_鹿屋田んぼ筆ポリゴン_浸水判定.png`",
        "- `混同行列_鹿屋田んぼ筆ポリゴン.png`",
        "",
        "## 注意",
        "",
        "正解浸水域と重なる田んぼ筆ポリゴン数が少ない場合、検証精度は不安定になります。久留米と比較すると、鹿屋の正解浸水域は田んぼ内でかなり小さいため、過大評価・過小評価の両方に注意が必要です。",
        "",
    ]
    (OUT / "鹿屋_田んぼ筆ポリゴン単位_浸水判定レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    polygon_df, ids, gdf = build_polygon_frame()
    valid_df = polygon_df.dropna(subset=FEATURES + [LABEL_COL]).copy()
    sampled = balanced_sample(valid_df)
    counts = pd.DataFrame(
        [
            {
                "available_positive": int((valid_df[LABEL_COL] == 1).sum()),
                "available_negative": int((valid_df[LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[LABEL_COL] == 1).sum()) if not sampled.empty else 0,
                "sampled_negative": int((sampled[LABEL_COL] == 0).sum()) if not sampled.empty else 0,
                "feature_count": len(FEATURES),
            }
        ]
    )
    metrics, scans, best_est = evaluate(sampled)
    pred_df = predict(valid_df, best_est)

    polygon_df.to_csv(OUT / "鹿屋_田んぼ筆ポリゴン特徴量.csv", index=False, encoding="utf-8-sig")
    sampled.to_csv(OUT / "抽出データ.csv", index=False, encoding="utf-8-sig")
    counts.to_csv(OUT / "母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン.csv", index=False, encoding="utf-8-sig")
    pred_df.to_csv(OUT / "鹿屋_田んぼ筆ポリゴン_全判定一覧.csv", index=False, encoding="utf-8-sig")

    export_gis(pred_df, ids, gdf)
    best = metrics.sort_values("balanced_accuracy", ascending=False).iloc[0]
    plot_confusion(best)
    plot_metrics(metrics)
    write_report(counts, metrics, pred_df)
    print(counts.to_string(index=False))
    print(metrics.sort_values("balanced_accuracy", ascending=False)[["model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC", "TP", "FP", "FN", "TN"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
