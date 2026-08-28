from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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
from xgboost import XGBClassifier

import analyze_kanoya_paddy_polygon_unit as poly_base
import analyze_kanoya_paddy_polygon_unit_bbox as bbox_base


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/kanoya_rain_s1"
DIFF_DIR = BASE / "kurume_signature_diff_analysis"
PADDY_REPORT = BASE / "kanoya_paddy_inundation_model_report"
OUT = BASE / "kanoya_paddy_pixel_unit_bbox_report"
PADDY_MASK = PADDY_REPORT / "kanoya_paddy_mask.tif"
TRUTH_MASK = DIFF_DIR / "kanoya_inundation_mask_0p5_1p7_on_diff_scene.tif"

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


def template_profile() -> dict:
    with rasterio.open(DIFF_RASTERS["0_3h"]) as src:
        return src.profile.copy()


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


def build_frame() -> tuple[pd.DataFrame, tuple[int, int]]:
    d0 = read_float(DIFF_RASTERS["0_3h"])
    d3 = read_float(DIFF_RASTERS["3_6h"])
    d6 = read_float(DIFF_RASTERS["6_12h"])
    d12 = read_float(DIFF_RASTERS["12_24h"])
    bbox_mask = bbox_base.bbox_mask_from_tif()
    paddy = read_bool(PADDY_MASK)
    truth = read_bool(TRUTH_MASK)
    valid = bbox_mask & paddy & np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12)
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
    df = df.dropna(subset=FEATURES + [LABEL_COL]).copy()
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(len(pos), len(neg))
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


def evaluate(sampled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
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
            "balanced_accuracy": float(best["balanced_accuracy"]),
            "model": model_name,
        }
        if best_est is None or candidate["balanced_accuracy"] > best_est["balanced_accuracy"]:
            best_est = candidate
    return pd.DataFrame(rows), pd.concat(scans, ignore_index=True), best_est


def predict(df: pd.DataFrame, best_est: dict) -> pd.DataFrame:
    out = df.dropna(subset=FEATURES + [LABEL_COL]).copy()
    prob = best_est["estimator"].predict_proba(out[FEATURES].to_numpy(np.float32))[:, 1]
    out["predicted_probability"] = prob
    out["predicted_inundated"] = (prob >= best_est["threshold"]).astype(np.uint8)
    return out


def export_gis(pred_df: pd.DataFrame, shape: tuple[int, int]) -> None:
    prob_arr = np.full(shape, np.nan, dtype=np.float32)
    pred_arr = np.zeros(shape, dtype=np.uint8)
    rows = pred_df["row"].to_numpy(np.int32)
    cols = pred_df["col"].to_numpy(np.int32)
    prob_arr[rows, cols] = pred_df["predicted_probability"].to_numpy(np.float32)
    pred_arr[rows, cols] = pred_df["predicted_inundated"].to_numpy(np.uint8)
    write_raster(OUT / "GIS_鹿屋bbox内_田んぼ画素_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_鹿屋bbox内_田んぼ画素_浸水判定.tif", pred_arr, "uint8", 0)
    pred_df[["row", "col", LABEL_COL, "predicted_probability", "predicted_inundated"]].to_csv(
        OUT / "GIS_鹿屋bbox内_田んぼ画素_浸水判定一覧.csv", index=False, encoding="utf-8-sig"
    )


def plot_confusion(row: pd.Series) -> None:
    cm = np.array([[int(row["TP"]), int(row["FN"])], [int(row["FP"]), int(row["TN"])]])
    fig, ax = plt.subplots(figsize=(5.6, 4.8))
    im = ax.imshow(cm, cmap="Blues")
    labels = [["TP\n浸水を検出", "FN\n浸水を未検出"], ["FP\n非浸水を誤検出", "TN\n非浸水を非検出"]]
    max_v = max(cm.max(), 1)
    for i in range(2):
        for j in range(2):
            color = "white" if cm[i, j] > max_v * 0.55 else "black"
            ax.text(j, i, f"{labels[i][j]}\n{cm[i, j]:,}", ha="center", va="center", color=color, fontproperties=poly_base.FONT)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["浸水と予測", "非浸水と予測"], fontproperties=poly_base.FONT)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["実際に浸水", "実際に非浸水"], fontproperties=poly_base.FONT)
    ax.set_title("鹿屋 bbox内 田んぼ画素単位 混同行列", fontproperties=poly_base.FONT)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    plt.tight_layout()
    plt.savefig(OUT / "混同行列_鹿屋bbox内_田んぼ画素.png", dpi=220, bbox_inches="tight")
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
                "all_bbox_paddy_pixels": len(pred_df),
                "truth_inundated_pixels": int(pred_df[LABEL_COL].sum()),
                "predicted_inundated_pixels": int(pred_df["predicted_inundated"].sum()),
            }
        ]
    )
    lines = [
        "# 鹿屋 bbox内 田んぼ画素単位 浸水判定レポート",
        "",
        "## 条件",
        "",
        "- `Inun_shinkawacho.tif` の有効画素bbox内にある田んぼ画素だけを対象にしました。",
        "- 正解浸水域は `0.5 <= TIF値 <= 1.7` です。",
        "- positive / negative を同数抽出し、train/test = 7:3 に分けました。",
        "- GridSearchCVは訓練データ内だけで実行しました。",
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
        "## bbox内全田んぼ画素へ適用した判定数",
        "",
        md_table(pred_counts),
        "",
        "## 注意",
        "",
        "田んぼ画素単位でも正解浸水画素数は少ないため、精度はサンプル分割に影響されやすいです。",
        "",
    ]
    (OUT / "鹿屋_bbox内_田んぼ画素単位_浸水判定レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    bbox_base.OUT = OUT
    df, shape = build_frame()
    sampled = balanced_sample(df)
    counts = pd.DataFrame(
        [
            {
                "available_positive": int((df[LABEL_COL] == 1).sum()),
                "available_negative": int((df[LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[LABEL_COL] == 1).sum()),
                "sampled_negative": int((sampled[LABEL_COL] == 0).sum()),
                "train_positive": int((sampled[LABEL_COL] == 1).sum() * (1 - TEST_SIZE)),
                "train_negative": int((sampled[LABEL_COL] == 0).sum() * (1 - TEST_SIZE)),
                "test_positive": int((sampled[LABEL_COL] == 1).sum() * TEST_SIZE),
                "test_negative": int((sampled[LABEL_COL] == 0).sum() * TEST_SIZE),
                "feature_count": len(FEATURES),
            }
        ]
    )
    metrics, scans, best_est = evaluate(sampled)
    pred_df = predict(df, best_est)
    counts.to_csv(OUT / "母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン.csv", index=False, encoding="utf-8-sig")
    sampled.to_csv(OUT / "抽出データ.csv", index=False, encoding="utf-8-sig")
    pred_df.to_csv(OUT / "鹿屋_bbox内_田んぼ画素_全判定一覧.csv", index=False, encoding="utf-8-sig")
    export_gis(pred_df, shape)
    best = metrics.sort_values("balanced_accuracy", ascending=False).iloc[0]
    plot_confusion(best)
    write_report(counts, metrics, pred_df)
    print(counts.to_string(index=False))
    print(metrics.sort_values("balanced_accuracy", ascending=False)[["model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC", "TP", "FP", "FN", "TN"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
