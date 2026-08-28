#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Build Kanoya inundation features and run model tuning reports."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize
from scipy.stats import loguniform, randint, uniform
from sqlalchemy import text
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

os.environ.setdefault("DB_HOST", "127.0.0.1")
os.environ.setdefault("DB_PORT", "3307")

from app.db.session import SessionLocal  # noqa: E402

KANoya_DIR = ROOT / "output" / "kanoya_rain_s1"
DIFF_DIR = KANoya_DIR / "kurume_signature_diff_analysis"
OUT_DIR = KANoya_DIR / "kanoya_paddy_inundation_model_report"
TRUTH_TIF = Path(r"D:\sotsuron\kanoya\Inun_shinkawacho.tif")
TRUTH_ON_SCENE = DIFF_DIR / "kanoya_inundation_mask_0p5_1p7_on_diff_scene.tif"
PAIR_CSV = DIFF_DIR / "kanoya_diff_signature_pairs.csv"
COUNT_CSV = DIFF_DIR / "kanoya_diff_elapsed_bin_counts.csv"
PADDY_GEOJSON = OUT_DIR / "kanoya_fude_paddy_polygons_from_db.geojson"
PADDY_MASK_TIF = OUT_DIR / "kanoya_paddy_mask.tif"

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 3

RASTERS = {
    "diff_0_3h": DIFF_DIR / "kanoya_mean_diff_0_3h.tif",
    "diff_3_6h": DIFF_DIR / "kanoya_mean_diff_3_6h.tif",
    "diff_6_12h": DIFF_DIR / "kanoya_mean_diff_6_12h.tif",
    "diff_12_24h": DIFF_DIR / "kanoya_mean_diff_12_24h.tif",
}

FEATURE_DEFINITIONS = {
    "diff_0_3h": "降雨開始後0-3hの平均差分 target - before (dB)",
    "diff_3_6h": "降雨開始後3-6hの平均差分 target - before (dB)",
    "diff_6_12h": "降雨開始後6-12hの平均差分 target - before (dB)",
    "diff_12_24h": "降雨開始後12-24hの平均差分 target - before (dB)",
    "early_mean_0_6h": "(diff_0_3h + diff_3_6h) / 2",
    "late_mean_6_24h": "(diff_6_12h + diff_12_24h) / 2",
    "early_minus_late": "early_mean_0_6h - late_mean_6_24h",
    "drop_0_3_to_3_6": "diff_0_3h - diff_3_6h",
    "drop_3_6_to_6_12": "diff_3_6h - diff_6_12h",
    "recovery_6_12_to_12_24": "diff_12_24h - diff_6_12h",
    "drop_0_3_to_6_12": "diff_0_3h - diff_6_12h",
    "change_0_3_to_6_12": "diff_6_12h - diff_0_3h",
    "profile_mean": "4時間帯差分の平均",
    "profile_std": "4時間帯差分の標準偏差",
    "profile_range": "4時間帯差分の最大値 - 最小値",
    "negative_bin_count": "差分が負になる時間帯数",
    "monotonic_drop_score": "0-3h>=3-6h, 3-6h>=6-12h, 6-12h<=12-24h の成立数",
}
FEATURE_COLS = list(FEATURE_DEFINITIONS.keys())


def read_float(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        nodata = src.nodata
    if nodata is not None and np.isfinite(nodata):
        arr[arr == nodata] = np.nan
    return arr


def geometry_from_db(value) -> dict:
    if isinstance(value, str):
        return json.loads(value)
    return value


def fetch_paddy_geometries(bounds) -> list[dict]:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT polygon_uuid, land_type, issue_year, edit_year, local_government_cd,
                       point_lng, point_lat, pref_id, geometry
                FROM fude_polygons
                WHERE land_type = 100
                  AND point_lat BETWEEN :min_lat AND :max_lat
                  AND point_lng BETWEEN :min_lon AND :max_lon
                ORDER BY pref_id, local_government_cd, polygon_uuid
                """
            ),
            {
                "min_lon": bounds.left,
                "min_lat": bounds.bottom,
                "max_lon": bounds.right,
                "max_lat": bounds.top,
            },
        ).fetchall()
    finally:
        db.close()

    features = []
    for row in rows:
        features.append(
            {
                "type": "Feature",
                "geometry": geometry_from_db(row.geometry),
                "properties": {
                    "polygon_uuid": row.polygon_uuid,
                    "land_type": row.land_type,
                    "issue_year": row.issue_year,
                    "edit_year": row.edit_year,
                    "local_government_cd": row.local_government_cd,
                    "point_lng": row.point_lng,
                    "point_lat": row.point_lat,
                    "pref_id": row.pref_id,
                },
            }
        )
    return features


def build_paddy_mask(template_path: Path) -> tuple[np.ndarray, int]:
    with rasterio.open(template_path) as src:
        profile = src.profile.copy()
        out_shape = (src.height, src.width)
        transform = src.transform
        bounds = src.bounds

    features = fetch_paddy_geometries(bounds)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PADDY_GEOJSON.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False),
        encoding="utf-8",
    )
    if features:
        mask = rasterize(
            [(feature["geometry"], 1) for feature in features],
            out_shape=out_shape,
            transform=transform,
            fill=0,
            default_value=1,
            dtype=np.uint8,
            all_touched=True,
        ).astype(bool)
    else:
        mask = np.zeros(out_shape, dtype=bool)

    profile.update(count=1, dtype="uint8", nodata=0, compress="deflate")
    with rasterio.open(PADDY_MASK_TIF, "w", **profile) as dst:
        dst.write(mask.astype(np.uint8), 1)
    return mask, len(features)


def metric_row(model_name: str, y_true: np.ndarray, pred: np.ndarray, score: np.ndarray) -> dict:
    tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
    return {
        "model": model_name,
        "TP": int(tp),
        "FP": int(fp),
        "FN": int(fn),
        "TN": int(tn),
        "accuracy": accuracy_score(y_true, pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, pred),
        "precision": precision_score(y_true, pred, zero_division=0),
        "recall": recall_score(y_true, pred, zero_division=0),
        "specificity": tn / (tn + fp) if tn + fp else 0.0,
        "F1": f1_score(y_true, pred, zero_division=0),
        "ROC_AUC": roc_auc_score(y_true, score),
    }


def threshold_scan(model_name: str, y_true: np.ndarray, prob: np.ndarray) -> pd.DataFrame:
    rows = []
    for th in np.linspace(0.05, 0.95, 181):
        pred = (prob >= th).astype(int)
        row = metric_row(model_name, y_true, pred, prob)
        row["threshold"] = float(th)
        rows.append(row)
    return pd.DataFrame(rows)


def md_table(df: pd.DataFrame, digits: int = 3) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.{digits}f}")
    shown = shown.fillna("")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]).replace("\n", "<br>") for c in shown.columns) + " |")
    return "\n".join(lines)


def build_feature_frame() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    arrays = {name: read_float(path) for name, path in RASTERS.items()}
    shape = next(iter(arrays.values())).shape
    for name, arr in arrays.items():
        if arr.shape != shape:
            raise ValueError(f"Raster shape mismatch: {name} {arr.shape} != {shape}")

    with rasterio.open(TRUTH_ON_SCENE) as src:
        truth = src.read(1) == 1
    paddy, paddy_feature_count = build_paddy_mask(RASTERS["diff_0_3h"])
    valid = paddy.copy()
    for arr in arrays.values():
        valid &= np.isfinite(arr)

    rr, cc = np.where(valid)
    df = pd.DataFrame({"row": rr, "col": cc})
    for name, arr in arrays.items():
        df[name] = arr[valid].astype(np.float32)
    df["label_inundation"] = truth[valid].astype(np.int8)

    df["early_mean_0_6h"] = (df["diff_0_3h"] + df["diff_3_6h"]) / 2.0
    df["late_mean_6_24h"] = (df["diff_6_12h"] + df["diff_12_24h"]) / 2.0
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df["diff_0_3h"] - df["diff_3_6h"]
    df["drop_3_6_to_6_12"] = df["diff_3_6h"] - df["diff_6_12h"]
    df["recovery_6_12_to_12_24"] = df["diff_12_24h"] - df["diff_6_12h"]
    df["drop_0_3_to_6_12"] = df["diff_0_3h"] - df["diff_6_12h"]
    df["change_0_3_to_6_12"] = df["diff_6_12h"] - df["diff_0_3h"]
    profile = df[["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]].to_numpy(dtype=np.float32)
    df["profile_mean"] = profile.mean(axis=1)
    df["profile_std"] = profile.std(axis=1)
    df["profile_range"] = profile.max(axis=1) - profile.min(axis=1)
    df["negative_bin_count"] = (profile < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h"] >= df["diff_3_6h"]).astype(int)
        + (df["diff_3_6h"] >= df["diff_6_12h"]).astype(int)
        + (df["diff_6_12h"] <= df["diff_12_24h"]).astype(int)
    )

    rng = np.random.default_rng(SEED)
    pos = df.index[df["label_inundation"] == 1].to_numpy()
    neg = df.index[df["label_inundation"] == 0].to_numpy()
    n = min(len(pos), len(neg), 10000)
    if n == 0:
        raise RuntimeError(
            f"田んぼ限定の学習データを作成できません。"
            f"paddy_pixels={int(valid.sum())}, inundated_paddy_pixels={len(pos)}, non_inundated_paddy_pixels={len(neg)}"
        )
    chosen = np.concatenate([rng.choice(pos, n, replace=False), rng.choice(neg, n, replace=False)])
    balanced = df.loc[chosen].sample(frac=1.0, random_state=SEED).reset_index(drop=True)
    metadata = {
        "paddy_feature_count_from_db": paddy_feature_count,
        "paddy_valid_pixels": int(valid.sum()),
        "paddy_inundated_pixels": int(len(pos)),
        "paddy_non_inundated_pixels": int(len(neg)),
        "balanced_pixels_per_class": int(n),
        "paddy_geojson": str(PADDY_GEOJSON),
        "paddy_mask_tif": str(PADDY_MASK_TIF),
    }
    return df, balanced, metadata


def model_specs_grid() -> dict:
    return {
        "logistic_regression": {
            "estimator": Pipeline(
                [
                    ("scaler", StandardScaler()),
                    ("model", LogisticRegression(max_iter=3000, solver="lbfgs", random_state=SEED)),
                ]
            ),
            "grid": {"model__C": [0.01, 0.1, 1.0, 10.0], "model__class_weight": [None, "balanced"]},
        },
        "decision_tree": {
            "estimator": DecisionTreeClassifier(random_state=SEED),
            "grid": {
                "max_depth": [3, 5, 8, None],
                "min_samples_leaf": [20, 50, 100],
                "criterion": ["gini", "entropy"],
                "class_weight": [None, "balanced"],
            },
        },
        "random_forest": {
            "estimator": RandomForestClassifier(n_jobs=1, random_state=SEED),
            "grid": {
                "n_estimators": [160, 300],
                "max_depth": [6, 10, None],
                "min_samples_leaf": [20, 50, 100],
                "max_features": ["sqrt", 0.7],
                "class_weight": [None, "balanced"],
            },
        },
        "xgboost": {
            "estimator": XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            "grid": {
                "n_estimators": [160, 300],
                "max_depth": [2, 3],
                "learning_rate": [0.03, 0.05],
                "min_child_weight": [3, 10],
                "subsample": [0.85],
                "colsample_bytree": [0.85],
                "reg_lambda": [1.0],
            },
        },
    }


def model_specs_randomized() -> dict:
    return {
        "logistic_regression": {
            "estimator": Pipeline(
                [
                    ("scaler", StandardScaler()),
                    ("model", LogisticRegression(max_iter=3000, solver="saga", random_state=SEED)),
                ]
            ),
            "params": {"model__C": loguniform(1e-3, 30.0), "model__penalty": ["l1", "l2"], "model__class_weight": [None, "balanced"]},
            "n_iter": 20,
        },
        "decision_tree": {
            "estimator": DecisionTreeClassifier(random_state=SEED),
            "params": {
                "max_depth": [2, 3, 4, 5, 6, 8, 10, None],
                "min_samples_leaf": randint(10, 251),
                "min_samples_split": randint(20, 501),
                "criterion": ["gini", "entropy", "log_loss"],
                "class_weight": [None, "balanced"],
                "ccp_alpha": uniform(0.0, 0.004),
            },
            "n_iter": 28,
        },
        "random_forest": {
            "estimator": RandomForestClassifier(random_state=SEED, n_jobs=1),
            "params": {
                "n_estimators": randint(120, 401),
                "max_depth": [4, 6, 8, 10, 12, None],
                "min_samples_leaf": randint(10, 201),
                "min_samples_split": randint(20, 401),
                "max_features": ["sqrt", "log2", 0.5, 0.7, 1.0],
                "class_weight": [None, "balanced", "balanced_subsample"],
                "bootstrap": [True],
                "criterion": ["gini", "entropy"],
            },
            "n_iter": 24,
        },
        "xgboost": {
            "estimator": XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            "params": {
                "n_estimators": randint(120, 401),
                "max_depth": randint(2, 6),
                "learning_rate": loguniform(0.015, 0.16),
                "min_child_weight": randint(1, 16),
                "subsample": uniform(0.65, 0.35),
                "colsample_bytree": uniform(0.65, 0.35),
                "gamma": uniform(0.0, 2.0),
                "reg_lambda": loguniform(0.5, 8.0),
                "reg_alpha": loguniform(1e-3, 1.0),
            },
            "n_iter": 28,
        },
    }


def feature_importance(model_name: str, estimator) -> pd.DataFrame:
    if model_name == "logistic_regression":
        coef = estimator.named_steps["model"].coef_[0]
        return pd.DataFrame({"feature": FEATURE_COLS, "importance": coef, "importance_abs": np.abs(coef)}).sort_values(
            "importance_abs", ascending=False
        )
    if hasattr(estimator, "feature_importances_"):
        return pd.DataFrame({"feature": FEATURE_COLS, "importance": estimator.feature_importances_}).sort_values(
            "importance", ascending=False
        )
    return pd.DataFrame({"feature": FEATURE_COLS, "importance": np.nan})


def run_searches(balanced: pd.DataFrame, out_dir: Path, mode: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    X = balanced[FEATURE_COLS].to_numpy(dtype=np.float32)
    y = balanced["label_inundation"].to_numpy(dtype=np.int32)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE, random_state=SEED, stratify=y)
    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)

    metrics = []
    thresholds = []
    importances = []
    best_params = {}
    specs = model_specs_grid() if mode == "grid" else model_specs_randomized()

    for name, spec in specs.items():
        print(f"{mode} tuning: {name}")
        if mode == "grid":
            search = GridSearchCV(
                spec["estimator"],
                spec["grid"],
                scoring="balanced_accuracy",
                cv=cv,
                n_jobs=1,
                verbose=1,
                return_train_score=True,
            )
        else:
            search = RandomizedSearchCV(
                spec["estimator"],
                spec["params"],
                n_iter=spec["n_iter"],
                scoring="balanced_accuracy",
                cv=cv,
                n_jobs=1,
                verbose=1,
                random_state=SEED,
                return_train_score=True,
            )
        search.fit(X_train, y_train)
        best_params[name] = search.best_params_
        pd.DataFrame(search.cv_results_).to_csv(out_dir / f"{mode}_{name}_cv_results.csv", index=False, encoding="utf-8-sig")

        estimator = search.best_estimator_
        prob = estimator.predict_proba(X_test)[:, 1]
        pred_05 = (prob >= 0.5).astype(int)
        row = metric_row(f"{name}_threshold_0.5", y_test, pred_05, prob)
        row["selected_threshold"] = 0.5
        row["best_cv_balanced_accuracy"] = search.best_score_
        metrics.append(row)

        scan = threshold_scan(name, y_test, prob)
        scan.to_csv(out_dir / f"{mode}_{name}_threshold_scan.csv", index=False, encoding="utf-8-sig")
        best_ba = scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
        best_ba["model"] = name
        best_ba["selection"] = "best_balanced_accuracy"
        thresholds.append(best_ba)

        pred_best = (prob >= best_ba["threshold"]).astype(int)
        row = metric_row(f"{name}_bestBA_threshold_{best_ba['threshold']:.3f}", y_test, pred_best, prob)
        row["selected_threshold"] = best_ba["threshold"]
        row["best_cv_balanced_accuracy"] = search.best_score_
        metrics.append(row)

        imp = feature_importance(name, estimator)
        imp.insert(0, "model", name)
        imp.insert(0, "search", mode)
        imp.to_csv(out_dir / f"{mode}_{name}_feature_importance.csv", index=False, encoding="utf-8-sig")
        importances.append(imp)
        if name == "decision_tree":
            (out_dir / f"{mode}_decision_tree_rules.txt").write_text(
                export_text(estimator, feature_names=FEATURE_COLS, decimals=3),
                encoding="utf-8",
            )

    metrics_df = pd.DataFrame(metrics).sort_values("balanced_accuracy", ascending=False)
    thresholds_df = pd.DataFrame(thresholds)
    importances_df = pd.concat(importances, ignore_index=True)
    metrics_df.to_csv(out_dir / f"{mode}_test_metrics.csv", index=False, encoding="utf-8-sig")
    thresholds_df.to_csv(out_dir / f"{mode}_best_thresholds.csv", index=False, encoding="utf-8-sig")
    importances_df.to_csv(out_dir / f"{mode}_feature_importance.csv", index=False, encoding="utf-8-sig")
    (out_dir / f"{mode}_best_params.json").write_text(json.dumps(best_params, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return metrics_df, thresholds_df, importances_df, best_params


def summarize_features(df: pd.DataFrame, balanced: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for source_name, source_df in [("paddy_valid_pixels", df), ("paddy_balanced_sample", balanced)]:
        for label, sub in source_df.groupby("label_inundation"):
            zone = "inundated_truth" if int(label) == 1 else "non_inundated"
            for feature in FEATURE_COLS:
                vals = sub[feature].to_numpy(dtype=np.float64)
                q = np.nanpercentile(vals, [5, 25, 50, 75, 95])
                rows.append(
                    {
                        "source": source_name,
                        "zone": zone,
                        "feature": feature,
                        "count": int(np.isfinite(vals).sum()),
                        "mean": float(np.nanmean(vals)),
                        "std": float(np.nanstd(vals)),
                        "p05": float(q[0]),
                        "p25": float(q[1]),
                        "median": float(q[2]),
                        "p75": float(q[3]),
                        "p95": float(q[4]),
                    }
                )
    return pd.DataFrame(rows)


def plot_profiles(stats: pd.DataFrame, out_dir: Path) -> None:
    sub = stats[(stats["source"] == "paddy_balanced_sample") & (stats["feature"].isin(["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]))]
    labels = ["0-3h", "3-6h", "6-12h", "12-24h"]
    features = ["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]
    fig, ax = plt.subplots(figsize=(7, 4))
    for zone, color in [("inundated_truth", "tab:red"), ("non_inundated", "tab:blue")]:
        z = sub[sub["zone"] == zone].set_index("feature").loc[features]
        ax.plot(labels, z["mean"], marker="o", label=zone, color=color)
        ax.fill_between(labels, z["p25"], z["p75"], color=color, alpha=0.15)
    ax.axhline(0, color="black", lw=0.8)
    ax.set_ylabel("target - before (dB)")
    ax.set_title("Kanoya balanced pixels: mean diff profile")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_dir / "kanoya_balanced_diff_profile.png", dpi=160)
    plt.close(fig)


def plot_importance(importances: pd.DataFrame, out_dir: Path, name: str) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(11, 7))
    for ax, model in zip(axes.ravel(), ["logistic_regression", "decision_tree", "random_forest", "xgboost"]):
        sub = importances[importances["model"] == model].copy()
        if "importance_abs" in sub.columns and sub["importance_abs"].notna().any():
            sub = sub.sort_values("importance_abs", ascending=False)
            values = sub["importance_abs"]
        else:
            sub = sub.sort_values("importance", ascending=False)
            values = sub["importance"]
        sub = sub.head(8).iloc[::-1]
        vals = values.loc[sub.index]
        ax.barh(sub["feature"], vals)
        ax.set_title(model)
    fig.tight_layout()
    fig.savefig(out_dir / f"{name}_feature_importance.png", dpi=160)
    plt.close(fig)


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    full_df, balanced, metadata = build_feature_frame()
    full_df.to_csv(OUT_DIR / "kanoya_paddy_all_valid_pixel_features.csv", index=False, encoding="utf-8-sig")
    balanced.to_csv(OUT_DIR / "kanoya_paddy_balanced_pixel_features.csv", index=False, encoding="utf-8-sig")
    (OUT_DIR / "summary.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    stats = summarize_features(full_df, balanced)
    stats.to_csv(OUT_DIR / "kanoya_feature_stats.csv", index=False, encoding="utf-8-sig")
    plot_profiles(stats, OUT_DIR)

    grid_metrics, grid_thresholds, grid_importance, grid_params = run_searches(balanced, OUT_DIR, "grid")
    random_metrics, random_thresholds, random_importance, random_params = run_searches(balanced, OUT_DIR, "randomized")
    all_metrics = pd.concat([grid_metrics.assign(search="grid"), random_metrics.assign(search="randomized")], ignore_index=True)
    all_importance = pd.concat([grid_importance, random_importance], ignore_index=True)
    all_metrics.to_csv(OUT_DIR / "kanoya_grid_randomized_test_metrics.csv", index=False, encoding="utf-8-sig")
    all_importance.to_csv(OUT_DIR / "kanoya_grid_randomized_feature_importance.csv", index=False, encoding="utf-8-sig")
    plot_importance(grid_importance, OUT_DIR, "grid")
    plot_importance(random_importance, OUT_DIR, "randomized")

    counts = pd.DataFrame(
        [
            {"dataset": "paddy_valid_pixels", "label": "inundated_truth", "pixels": int((full_df["label_inundation"] == 1).sum())},
            {"dataset": "paddy_valid_pixels", "label": "non_inundated", "pixels": int((full_df["label_inundation"] == 0).sum())},
            {"dataset": "paddy_balanced_sample", "label": "inundated_truth", "pixels": int((balanced["label_inundation"] == 1).sum())},
            {"dataset": "paddy_balanced_sample", "label": "non_inundated", "pixels": int((balanced["label_inundation"] == 0).sum())},
        ]
    )
    counts.to_csv(OUT_DIR / "kanoya_pixel_counts.csv", index=False, encoding="utf-8-sig")

    pair_df = pd.read_csv(PAIR_CSV, encoding="utf-8-sig") if PAIR_CSV.exists() else pd.DataFrame()
    count_df = pd.read_csv(COUNT_CSV, encoding="utf-8-sig") if COUNT_CSV.exists() else pd.DataFrame()

    report = []
    report.append("# Kanoya 田んぼ内浸水域モデル チューニングレポート\n")
    report.append("## 使用データ\n")
    report.append(f"- 正解浸水域TIF: `{TRUTH_TIF}`")
    report.append("- 正解ラベル: `0.5 <= TIF値 <= 1.7` を浸水域として、解析グリッドへ投影済みのマスクを使用")
    report.append(f"- 入力差分raster: `{DIFF_DIR.relative_to(ROOT)}`")
    report.append("- 対象領域: DB の筆ポリゴン `land_type=100` を Kanoya 解析グリッドへ rasterize した田んぼ画素のみ")
    report.append("- 特徴量: Sentinel-1 VV の `target - before` 差分から作成")
    report.append("- 学習検証: train/test = 70/30、StratifiedKFold(3)、評価主指標は balanced_accuracy")
    report.append("")
    report.append("### 画素数\n")
    report.append(f"- DBから抽出した田んぼ筆ポリゴン数: {metadata['paddy_feature_count_from_db']:,}")
    report.append(f"- 田んぼマスク: `{Path(metadata['paddy_mask_tif']).relative_to(ROOT)}`")
    report.append(md_table(counts))
    report.append("")
    if not count_df.empty:
        report.append("### 経過時間帯別の衛星データ数\n")
        report.append(md_table(count_df))
        report.append("")
    if not pair_df.empty:
        report.append("### 使用した衛星データペア\n")
        report.append(md_table(pair_df[["rain_day_jst", "elapsed_h", "elapsed_bin", "target_stac_id", "pair_stac_id", "valid_pixel_count", "mean_diff"]], 3))
        report.append("")

    report.append("## 特徴量\n")
    report.append(md_table(pd.DataFrame([{"feature": k, "definition": v} for k, v in FEATURE_DEFINITIONS.items()])))
    report.append("")

    report.append("## GridSearchCV 結果\n")
    metric_cols = [
        "model",
        "selected_threshold",
        "best_cv_balanced_accuracy",
        "precision",
        "recall",
        "specificity",
        "balanced_accuracy",
        "F1",
        "ROC_AUC",
        "TP",
        "FP",
        "FN",
        "TN",
    ]
    report.append(md_table(grid_metrics[metric_cols], 3))
    report.append("")
    report.append("### GridSearchCV 最良パラメータ\n")
    report.append(md_table(pd.DataFrame([{"model": k, "best_params": json.dumps(v, ensure_ascii=False)} for k, v in grid_params.items()])))
    report.append("")

    report.append("## RandomizedSearchCV 結果\n")
    report.append(md_table(random_metrics[metric_cols], 3))
    report.append("")
    report.append("### RandomizedSearchCV 最良パラメータ\n")
    report.append(md_table(pd.DataFrame([{"model": k, "best_params": json.dumps(v, ensure_ascii=False, default=str)} for k, v in random_params.items()])))
    report.append("")

    report.append("## 特徴量の分布概要\n")
    top_features = ["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h", "early_minus_late", "drop_0_3_to_6_12", "change_0_3_to_6_12"]
    shown_stats = stats[(stats["source"] == "paddy_balanced_sample") & (stats["feature"].isin(top_features))]
    report.append(md_table(shown_stats, 3))
    report.append("")

    report.append("## 図\n")
    report.append("- `kanoya_balanced_diff_profile.png`: 正解浸水域と非浸水域の経過時間別差分プロファイル")
    report.append("- `grid_feature_importance.png`: GridSearchCV の特徴量重要度")
    report.append("- `randomized_feature_importance.png`: RandomizedSearchCV の特徴量重要度")
    report.append("")

    report.append("## 注意点\n")
    report.append("- このレポートは田んぼマスク内の画素のみを対象にしています。")
    report.append(f"- 田んぼ内の正解浸水域は {metadata['paddy_inundated_pixels']:,} 画素のため、バランスデータは各クラス {metadata['balanced_pixels_per_class']:,} 画素です。")
    report.append("- 画素単位でランダム分割しているため、空間的に近い画素が train/test に分かれる可能性があります。空間汎化性能を見る場合は、領域単位の分割が必要です。")

    (OUT_DIR / "kanoya_paddy_model_tuning_report.md").write_text("\n".join(report), encoding="utf-8")
    print(all_metrics.sort_values("balanced_accuracy", ascending=False)[["search", *metric_cols]].to_string(index=False))
    print(f"saved: {OUT_DIR / 'kanoya_paddy_model_tuning_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
