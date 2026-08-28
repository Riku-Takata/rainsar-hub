from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterio.warp import reproject
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
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_per_tif_all_pixels"
BBOX_GEOJSON = ROOT / "output/kurume_tif_bboxes/kurume_tif_valid_bboxes.geojson"
KURUME_DIR = Path(r"D:\sotsuron\kurume")
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
N_PER_CLASS = 20000
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


def bbox_features() -> list[dict]:
    return json.loads(BBOX_GEOJSON.read_text(encoding="utf-8")).get("features", [])


def bbox_mask(feature: dict) -> np.ndarray:
    profile = template_profile()
    return rasterize(
        [(feature["geometry"], 1)],
        out_shape=(profile["height"], profile["width"]),
        transform=profile["transform"],
        fill=0,
        dtype="uint8",
        all_touched=True,
    ).astype(bool)


def truth_mask(tif_path: Path) -> np.ndarray:
    profile = template_profile()
    dst = np.zeros((profile["height"], profile["width"]), dtype=np.uint8)
    with rasterio.open(tif_path) as src:
        arr = src.read(1).astype(np.float32)
        valid = np.isfinite(arr)
        if src.nodata is not None:
            valid &= arr != src.nodata
        truth = ((arr >= 0.5) & (arr <= 1.7) & valid).astype(np.uint8)
        reproject(
            source=truth,
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=profile["transform"],
            dst_crs=profile["crs"],
            resampling=Resampling.nearest,
            src_nodata=0,
            dst_nodata=0,
        )
    return dst > 0


def build_frame(tif_name: str, mask: np.ndarray, truth: np.ndarray, diffs: dict[str, np.ndarray]) -> pd.DataFrame:
    valid = mask.copy()
    for arr in diffs.values():
        valid &= np.isfinite(arr)
    rows, cols = np.where(valid)
    df = pd.DataFrame(
        {
            "tif": tif_name,
            "row": rows,
            "col": cols,
            LABEL_COL: truth[valid].astype(np.uint8),
            "diff_0_3h": diffs["0_3h"][valid],
            "diff_3_6h": diffs["3_6h"][valid],
            "diff_6_12h": diffs["6_12h"][valid],
            "diff_12_24h": diffs["12_24h"][valid],
        }
    )
    return add_features(df)


def balanced_sample(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=FEATURES + [LABEL_COL]).copy()
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(len(pos), len(neg), N_PER_CLASS)
    if n < 8:
        return pd.DataFrame()
    return pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)


def specs() -> dict:
    return {
        "ロジスティック回帰": (
            Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
            {"model__C": [0.05, 0.5, 1.0], "model__class_weight": [None, "balanced"]},
        ),
        "ランダムフォレスト": (
            RandomForestClassifier(random_state=SEED, n_jobs=1),
            {"n_estimators": [160], "max_depth": [10, None], "min_samples_leaf": [10, 30], "max_features": ["sqrt"]},
        ),
        "XGBoost": (
            XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            {
                "n_estimators": [120],
                "max_depth": [2, 3],
                "learning_rate": [0.03, 0.05],
                "min_child_weight": [1, 3],
                "subsample": [0.85],
                "colsample_bytree": [0.85],
            },
        ),
    }


def scan_threshold(y_true: np.ndarray, prob: np.ndarray, tif: str, model: str) -> tuple[dict, pd.DataFrame]:
    rows = []
    auc = roc_auc_score(y_true, prob) if len(np.unique(y_true)) == 2 else np.nan
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.int32)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        rows.append(
            {
                "tif": tif,
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
    return scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict(), scan


def evaluate(df: pd.DataFrame, tif: str) -> tuple[pd.DataFrame, pd.DataFrame, dict | None, pd.DataFrame]:
    sample = balanced_sample(df)
    if sample.empty:
        return pd.DataFrame(), pd.DataFrame(), None, sample
    x = sample[FEATURES].to_numpy(np.float32)
    y = sample[LABEL_COL].to_numpy(np.int32)
    test_size = TEST_SIZE if min(np.bincount(y)) >= 20 else 0.4
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size, stratify=y, random_state=SEED)
    cv_splits = min(5, int(min(np.bincount(y_train))))
    if cv_splits < 2:
        return pd.DataFrame(), pd.DataFrame(), None, sample
    cv = StratifiedKFold(n_splits=cv_splits, shuffle=True, random_state=SEED)
    rows = []
    scans = []
    best_est = None
    for name, (estimator, grid) in specs().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        prob = search.predict_proba(x_test)[:, 1]
        best, scan = scan_threshold(y_test, prob, tif, name)
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        rows.append(best)
        scans.append(scan)
        candidate = {
            "estimator": search.best_estimator_,
            "threshold": float(best["threshold"]),
            "features": FEATURES,
            "balanced_accuracy": float(best["balanced_accuracy"]),
            "model": name,
        }
        if best_est is None or candidate["balanced_accuracy"] > best_est["balanced_accuracy"]:
            best_est = candidate
    return pd.DataFrame(rows), pd.concat(scans, ignore_index=True), best_est, sample


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


def export_tif_outputs(tif_name: str, out_dir: Path, truth: np.ndarray, mask: np.ndarray, pred_df: pd.DataFrame, shape: tuple[int, int]) -> None:
    safe = Path(tif_name).stem
    write_raster(out_dir / f"{safe}_truth_mask.tif", truth.astype(np.uint8), "uint8", 0)
    write_raster(out_dir / f"{safe}_bbox_mask.tif", mask.astype(np.uint8), "uint8", 0)
    if pred_df.empty:
        return
    prob = np.full(shape, np.nan, dtype=np.float32)
    pred = np.zeros(shape, dtype=np.uint8)
    rows = pred_df["row"].to_numpy(np.int32)
    cols = pred_df["col"].to_numpy(np.int32)
    prob[rows, cols] = pred_df["predicted_probability"].to_numpy(np.float32)
    pred[rows, cols] = pred_df["predicted_inundated"].to_numpy(np.uint8)
    write_raster(out_dir / f"{safe}_all_pixel_probability.tif", prob, "float32", np.nan)
    write_raster(out_dir / f"{safe}_all_pixel_prediction.tif", pred, "uint8", 0)
    pred_df[["row", "col", LABEL_COL, "predicted_probability", "predicted_inundated"]].to_csv(
        out_dir / f"{safe}_all_pixel_predictions.csv", index=False, encoding="utf-8-sig"
    )


def plot_summary(summary: pd.DataFrame) -> None:
    if summary.empty:
        return
    fig, ax = plt.subplots(figsize=(12, 5.5))
    ax.plot(summary["tif"], summary["balanced_accuracy"], marker="o", color="#4c78a8", label="全画素")
    ax.set_ylim(0.35, 1.0)
    ax.set_ylabel("Balanced Accuracy", fontproperties=FONT)
    ax.set_title("TIF別 bbox内全画素の浸水判定精度", fontproperties=FONT)
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT)
    plt.tight_layout()
    plt.savefig(OUT / "図_TIF別_全画素判定精度.png", dpi=200, bbox_inches="tight")
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


def write_report(counts: pd.DataFrame, summary: pd.DataFrame) -> None:
    lines = [
        "# Kurume TIF別 bbox内全画素 浸水判定レポート",
        "",
        "## 条件",
        "",
        "- 田んぼマスクは使用せず、各TIFのbbox内に含まれる全有効画素を対象にしました。",
        "- 各TIFの `0.5 <= 値 <= 1.7` を個別の正解浸水域にしました。",
        "- positive / negative は各TIF内で同数に揃えました。",
        "",
        "## TIF別の母数と抽出数",
        "",
        md_table(counts),
        "",
        "## TIF別の最良精度",
        "",
        md_table(summary),
        "",
        "## GIS出力",
        "",
        "- `per_tif/<TIF名>/*_all_pixel_prediction.tif`",
        "- `per_tif/<TIF名>/*_all_pixel_probability.tif`",
        "- `per_tif/<TIF名>/*_truth_mask.tif`",
        "",
    ]
    (OUT / "TIF別_bbox内全画素_浸水判定レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    per_tif = OUT / "per_tif"
    per_tif.mkdir(exist_ok=True)
    profile = template_profile()
    shape = (profile["height"], profile["width"])
    diffs = {name: read_float(path) for name, path in DIFF_RASTERS.items()}
    counts = []
    metric_frames = []
    scan_frames = []
    summary_rows = []
    for feature in bbox_features():
        tif_name = feature["properties"]["tif"]
        tif_path = KURUME_DIR / tif_name
        out_dir = per_tif / Path(tif_name).stem
        out_dir.mkdir(parents=True, exist_ok=True)
        mask = bbox_mask(feature)
        truth = truth_mask(tif_path)
        df = build_frame(tif_name, mask, truth, diffs)
        sample = balanced_sample(df)
        counts.append(
            {
                "tif": tif_name,
                "available_positive": int((df[LABEL_COL] == 1).sum()),
                "available_negative": int((df[LABEL_COL] == 0).sum()),
                "sampled_positive": int((sample[LABEL_COL] == 1).sum()) if not sample.empty else 0,
                "sampled_negative": int((sample[LABEL_COL] == 0).sum()) if not sample.empty else 0,
                "feature_count": len(FEATURES),
            }
        )
        metrics, scans, best_est, _ = evaluate(df, tif_name)
        if not metrics.empty:
            metric_frames.append(metrics)
            scan_frames.append(scans)
            summary_rows.append(metrics.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict())
        pred_df = predict(df, best_est)
        export_tif_outputs(tif_name, out_dir, truth, mask, pred_df, shape)
        print(f"processed {tif_name}")

    counts_df = pd.DataFrame(counts)
    summary_df = pd.DataFrame(summary_rows)
    counts_df.to_csv(OUT / "TIF別_母数と抽出数.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(OUT / "TIF別_最良モデル評価.csv", index=False, encoding="utf-8-sig")
    if metric_frames:
        pd.concat(metric_frames, ignore_index=True).to_csv(OUT / "TIF別_全モデル評価.csv", index=False, encoding="utf-8-sig")
    if scan_frames:
        pd.concat(scan_frames, ignore_index=True).to_csv(OUT / "TIF別_閾値スキャン.csv", index=False, encoding="utf-8-sig")
    plot_summary(summary_df)
    write_report(counts_df, summary_df)
    print(summary_df[["tif", "model", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
