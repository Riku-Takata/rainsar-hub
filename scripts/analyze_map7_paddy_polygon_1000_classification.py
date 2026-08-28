from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
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
DETECTION_DIR = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
LAND_DIR = ROOT / "output/gsi_h30_geojson_s1/map7_land_polygons"
OUT_DIR = DETECTION_DIR / "paddy_polygon_1000_classification"

PADDY_GEOJSON = LAND_DIR / "map7_fude_paddy_polygons_from_db.geojson"
TRUTH_MASK = DETECTION_DIR / "map7_inundation_truth_mask.tif"
TEMPLATE = DETECTION_DIR / "map7_mean_diff_0_3h.tif"
SEED = 42
N_PER_CLASS = 1000
LABEL_COL = "truth_polygon"

DIFF_RASTERS = {
    "0_3h": DETECTION_DIR / "map7_mean_diff_0_3h.tif",
    "3_6h": DETECTION_DIR / "map7_mean_diff_3_6h.tif",
    "6_12h": DETECTION_DIR / "map7_mean_diff_6_12h.tif",
    "12_24h": DETECTION_DIR / "map7_mean_diff_12_24h.tif",
}


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


def load_features(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8")).get("features", [])


def rasterize_ids(features: list[dict], profile: dict) -> np.ndarray:
    return rasterize(
        (
            (feature["geometry"], idx)
            for idx, feature in enumerate(features, start=1)
            if feature.get("geometry")
        ),
        out_shape=(profile["height"], profile["width"]),
        transform=profile["transform"],
        fill=0,
        dtype="int32",
        all_touched=False,
    )


def grouped_stats(ids: np.ndarray, values: np.ndarray, valid: np.ndarray) -> pd.DataFrame:
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
                "count": int(count),
                "mean": float(np.mean(vals)),
                "median": float(np.median(vals)),
                "std": float(np.std(vals)),
                "p25": float(np.percentile(vals, 25)),
                "p75": float(np.percentile(vals, 75)),
            }
        )
    return pd.DataFrame(rows)


def build_polygon_frame() -> pd.DataFrame:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    features = load_features(PADDY_GEOJSON)
    with rasterio.open(TEMPLATE) as src:
        profile = src.profile.copy()
    ids = rasterize_ids(features, profile)
    truth = read_bool(TRUTH_MASK)
    diff_arrays = {name: read_float(path) for name, path in DIFF_RASTERS.items()}
    valid = np.ones(ids.shape, dtype=bool)
    for arr in diff_arrays.values():
        valid &= np.isfinite(arr)

    base_ids, pixel_counts = np.unique(ids[valid & (ids > 0)], return_counts=True)
    df = pd.DataFrame(
        {
            "feature_seq_id": base_ids.astype(np.int32),
            "sentinel_pixel_count": pixel_counts.astype(np.int32),
        }
    )

    truth_ids, truth_counts = np.unique(ids[valid & truth & (ids > 0)], return_counts=True)
    truth_df = pd.DataFrame(
        {
            "feature_seq_id": truth_ids.astype(np.int32),
            "truth_pixel_count": truth_counts.astype(np.int32),
        }
    )
    df = df.merge(truth_df, on="feature_seq_id", how="left")
    df["truth_pixel_count"] = df["truth_pixel_count"].fillna(0).astype(np.int32)
    df[LABEL_COL] = (df["truth_pixel_count"] > 0).astype(np.uint8)
    df["truth_pixel_ratio"] = df["truth_pixel_count"] / df["sentinel_pixel_count"]

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

    for name, arr in diff_arrays.items():
        stats = grouped_stats(ids, arr, valid).rename(
            columns={
                "count": f"diff_{name}_valid_pixel_count",
                "mean": f"diff_{name}_mean",
                "median": f"diff_{name}_median",
                "std": f"diff_{name}_std",
                "p25": f"diff_{name}_p25",
                "p75": f"diff_{name}_p75",
            }
        )
        df = df.merge(stats, on="feature_seq_id", how="left")

    df["early_mean_0_6h"] = (df["diff_0_3h_mean"] + df["diff_3_6h_mean"]) / 2
    df["late_mean_6_24h"] = (df["diff_6_12h_mean"] + df["diff_12_24h_mean"]) / 2
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df["diff_0_3h_mean"] - df["diff_3_6h_mean"]
    df["drop_3_6_to_6_12"] = df["diff_3_6h_mean"] - df["diff_6_12h_mean"]
    df["recovery_6_12_to_12_24"] = df["diff_12_24h_mean"] - df["diff_6_12h_mean"]
    df["drop_0_3_to_6_12"] = df["diff_0_3h_mean"] - df["diff_6_12h_mean"]
    df["change_0_3_to_6_12"] = df["diff_6_12h_mean"] - df["diff_0_3h_mean"]
    profile_values = df[
        ["diff_0_3h_mean", "diff_3_6h_mean", "diff_6_12h_mean", "diff_12_24h_mean"]
    ].to_numpy(np.float32)
    df["profile_mean"] = np.nanmean(profile_values, axis=1)
    df["profile_std"] = np.nanstd(profile_values, axis=1)
    df["profile_range"] = np.nanmax(profile_values, axis=1) - np.nanmin(profile_values, axis=1)
    df["negative_bin_count"] = (profile_values < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h_mean"] >= df["diff_3_6h_mean"]).astype(int)
        + (df["diff_3_6h_mean"] >= df["diff_6_12h_mean"]).astype(int)
        + (df["diff_6_12h_mean"] <= df["diff_12_24h_mean"]).astype(int)
    )
    return df


FEATURE_COLS = [
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


def sample_balanced(df: pd.DataFrame) -> pd.DataFrame:
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    n = min(N_PER_CLASS, len(pos), len(neg))
    return pd.concat(
        [
            pos.sample(n=n, random_state=SEED),
            neg.sample(n=n, random_state=SEED),
        ],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)


def threshold_scan(y_true: np.ndarray, prob: np.ndarray, model_name: str) -> tuple[dict, pd.DataFrame]:
    rows = []
    for threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= threshold).astype(np.int32)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        row = {
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
        rows.append(row)
    scan = pd.DataFrame(rows)
    best = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
    return best, scan


def train_models(sampled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    x = sampled[FEATURE_COLS].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.3, stratify=y, random_state=SEED
    )
    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=SEED)
    specs = {
        "logistic_regression": (
            Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
            {"model__C": [0.01, 0.1, 1.0, 10.0], "model__class_weight": [None, "balanced"]},
        ),
        "decision_tree": (
            DecisionTreeClassifier(random_state=SEED),
            {"max_depth": [3, 5, 8, None], "min_samples_leaf": [10, 30, 50, 100], "class_weight": [None, "balanced"]},
        ),
        "random_forest": (
            RandomForestClassifier(random_state=SEED, n_jobs=1),
            {"n_estimators": [160, 300], "max_depth": [5, 10, None], "min_samples_leaf": [10, 30, 50], "max_features": ["sqrt", 0.7]},
        ),
        "xgboost": (
            XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            {"n_estimators": [160, 300], "max_depth": [2, 3], "learning_rate": [0.03, 0.05], "min_child_weight": [1, 3, 10], "subsample": [0.85], "colsample_bytree": [0.85]},
        ),
    }
    metrics = []
    scans = []
    importances = []
    for name, (estimator, grid) in specs.items():
        search = GridSearchCV(
            estimator,
            grid,
            scoring="balanced_accuracy",
            cv=cv,
            n_jobs=1,
            refit=True,
        )
        search.fit(x_train, y_train)
        prob = search.predict_proba(x_test)[:, 1]
        best, scan = threshold_scan(y_test, prob, name)
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        metrics.append(best)
        scan["best_cv_balanced_accuracy"] = float(search.best_score_)
        scans.append(scan)

        model = search.best_estimator_
        if hasattr(model, "feature_importances_"):
            vals = model.feature_importances_
        elif hasattr(model, "named_steps") and hasattr(model.named_steps["model"], "coef_"):
            vals = np.abs(model.named_steps["model"].coef_[0])
        else:
            vals = np.zeros(len(FEATURE_COLS))
        imp = pd.DataFrame({"model": name, "feature": FEATURE_COLS, "importance": vals})
        importances.append(imp.sort_values("importance", ascending=False))
    return (
        pd.DataFrame(metrics).sort_values("balanced_accuracy", ascending=False),
        pd.concat(scans, ignore_index=True),
        pd.concat(importances, ignore_index=True),
    )


def cohen_d(pos: pd.Series, neg: pd.Series) -> float:
    pos = pos.dropna().to_numpy(float)
    neg = neg.dropna().to_numpy(float)
    pooled = np.sqrt(((len(pos) - 1) * pos.var(ddof=1) + (len(neg) - 1) * neg.var(ddof=1)) / (len(pos) + len(neg) - 2))
    return float((pos.mean() - neg.mean()) / pooled) if pooled > 0 else np.nan


def feature_stats(sampled: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feature in FEATURE_COLS:
        pos = sampled.loc[sampled[LABEL_COL] == 1, feature]
        neg = sampled.loc[sampled[LABEL_COL] == 0, feature]
        rows.append(
            {
                "feature": feature,
                "truth_mean": pos.mean(),
                "nontruth_mean": neg.mean(),
                "mean_difference": pos.mean() - neg.mean(),
                "cohen_d": cohen_d(pos, neg),
                "abs_cohen_d": abs(cohen_d(pos, neg)),
                "truth_median": pos.median(),
                "nontruth_median": neg.median(),
            }
        )
    return pd.DataFrame(rows).sort_values("abs_cohen_d", ascending=False)


def plot_profiles(sampled: pd.DataFrame) -> None:
    cols = ["diff_0_3h_mean", "diff_3_6h_mean", "diff_6_12h_mean", "diff_12_24h_mean"]
    labels = ["0-3h", "3-6h", "6-12h", "12-24h"]
    x = np.arange(len(cols))
    fig, ax = plt.subplots(figsize=(7, 4))
    for label, name, color in [(1, "truth polygon", "#d62728"), (0, "nontruth polygon", "#1f77b4")]:
        sub = sampled[sampled[LABEL_COL] == label]
        means = [sub[c].mean() for c in cols]
        q25 = [sub[c].quantile(0.25) for c in cols]
        q75 = [sub[c].quantile(0.75) for c in cols]
        ax.plot(x, means, marker="o", label=name, color=color)
        ax.fill_between(x, q25, q75, color=color, alpha=0.18)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("polygon mean diff")
    ax.set_title("Paddy polygon backscatter difference profile")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "paddy_polygon_1000_diff_profile.png", dpi=180)
    plt.close(fig)


def md_table(df: pd.DataFrame, digits: int = 3) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.{digits}f}")
    shown = shown.fillna("")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    polygons = build_polygon_frame()
    polygons.to_csv(OUT_DIR / "map7_paddy_polygon_features_all.csv", index=False, encoding="utf-8-sig")

    sampled = sample_balanced(polygons)
    sampled.to_csv(OUT_DIR / "map7_paddy_polygon_1000_each_sample.csv", index=False, encoding="utf-8-sig")

    counts = pd.DataFrame(
        [
            {"label": "truth_polygon", "available_polygons": int((polygons[LABEL_COL] == 1).sum()), "sampled_polygons": int((sampled[LABEL_COL] == 1).sum())},
            {"label": "nontruth_polygon", "available_polygons": int((polygons[LABEL_COL] == 0).sum()), "sampled_polygons": int((sampled[LABEL_COL] == 0).sum())},
        ]
    )
    counts.to_csv(OUT_DIR / "map7_paddy_polygon_1000_sampling_summary.csv", index=False, encoding="utf-8-sig")

    stats = feature_stats(sampled)
    stats.to_csv(OUT_DIR / "map7_paddy_polygon_1000_feature_separation.csv", index=False, encoding="utf-8-sig")
    metrics, scans, importances = train_models(sampled)
    metrics.to_csv(OUT_DIR / "map7_paddy_polygon_1000_model_metrics.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT_DIR / "map7_paddy_polygon_1000_threshold_scans.csv", index=False, encoding="utf-8-sig")
    importances.to_csv(OUT_DIR / "map7_paddy_polygon_1000_feature_importance.csv", index=False, encoding="utf-8-sig")
    plot_profiles(sampled)

    report = [
        "# map7 田んぼ筆ポリゴン単位 1000筆/1000筆 分類分析",
        "",
        "## ラベル定義",
        "",
        "- positive: 正解浸水域マスクと1画素以上重なる田んぼ筆",
        "- negative: Sentinel評価グリッド上にあるが、正解浸水域と重ならない田んぼ筆",
        "- 各クラスからランダムに1,000筆を抽出",
        "- 各筆の特徴量は、筆内Sentinel画素の後方散乱強度差分を平均・標準偏差などで集約",
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## モデル結果",
        "",
        md_table(metrics[["model", "threshold", "best_cv_balanced_accuracy", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN"]]),
        "",
        "## クラス間分離が大きい特徴量",
        "",
        md_table(stats.head(12)[["feature", "truth_mean", "nontruth_mean", "mean_difference", "cohen_d", "truth_median", "nontruth_median"]]),
        "",
        "## 出力",
        "",
        "- `map7_paddy_polygon_features_all.csv`: 全田んぼ筆の特徴量",
        "- `map7_paddy_polygon_1000_each_sample.csv`: 抽出した1000筆/1000筆",
        "- `map7_paddy_polygon_1000_model_metrics.csv`: モデル指標",
        "- `map7_paddy_polygon_1000_feature_separation.csv`: 特徴量分離",
        "- `map7_paddy_polygon_1000_diff_profile.png`: 筆単位の平均差分プロファイル",
        "",
    ]
    (OUT_DIR / "map7_paddy_polygon_1000_classification_report.md").write_text("\n".join(report), encoding="utf-8")
    print(metrics.to_string(index=False))
    print(f"Wrote {OUT_DIR}")


if __name__ == "__main__":
    main()
