from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
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
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_all_pixels_20000_feature_diagnostics"
BBOX_MASK = BASE / "bbox_balanced_classification/GIS_bbox_union_mask.tif"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 5
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


def build_frame() -> tuple[pd.DataFrame, tuple[int, int]]:
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
    n = min(N_PER_CLASS, len(pos), len(neg))
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
    return scan.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict(), scan


def evaluate(sampled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict, pd.DataFrame]:
    x = sampled[FEATURES].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    train_idx, test_idx = train_test_split(
        np.arange(len(sampled)), test_size=TEST_SIZE, stratify=y, random_state=SEED
    )
    x_train, y_train = x[train_idx], y[train_idx]
    x_test, y_test = x[test_idx], y[test_idx]
    test_df = sampled.iloc[test_idx].copy().reset_index(drop=True)
    rows = []
    scans = []
    best_grid = None

    for model_name, model in model_specs_default().items():
        model.fit(x_train, y_train)
        best, scan = threshold_scan(y_test, model.predict_proba(x_test)[:, 1], model_name, "通常設定")
        rows.append(best)
        scans.append(scan)

    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)
    for model_name, (estimator, grid) in model_specs_grid().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        prob = search.predict_proba(x_test)[:, 1]
        best, scan = threshold_scan(y_test, prob, model_name, "GridSearch")
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        rows.append(best)
        scans.append(scan)
        candidate = {
            "model_name": model_name,
            "estimator": search.best_estimator_,
            "threshold": float(best["threshold"]),
            "balanced_accuracy": float(best["balanced_accuracy"]),
            "test_probability": prob,
            "test_index_df": test_df.copy(),
        }
        if best_grid is None or candidate["balanced_accuracy"] > best_grid["balanced_accuracy"]:
            best_grid = candidate

    return pd.DataFrame(rows), pd.concat(scans, ignore_index=True), best_grid, test_df


def summarize_features(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    rows = []
    for group, g in df.groupby(group_col):
        for feature in FEATURES:
            vals = g[feature].to_numpy(np.float32)
            rows.append(
                {
                    group_col: group,
                    "feature": feature,
                    "count": int(len(vals)),
                    "mean": float(np.mean(vals)),
                    "std": float(np.std(vals)),
                    "median": float(np.median(vals)),
                    "p10": float(np.percentile(vals, 10)),
                    "p25": float(np.percentile(vals, 25)),
                    "p75": float(np.percentile(vals, 75)),
                    "p90": float(np.percentile(vals, 90)),
                }
            )
    return pd.DataFrame(rows)


def effect_size_by_truth(df: pd.DataFrame) -> pd.DataFrame:
    pos = df[df[LABEL_COL] == 1]
    neg = df[df[LABEL_COL] == 0]
    rows = []
    for feature in FEATURES:
        a = pos[feature].to_numpy(np.float32)
        b = neg[feature].to_numpy(np.float32)
        pooled = np.sqrt((np.var(a) + np.var(b)) / 2)
        rows.append(
            {
                "feature": feature,
                "inundated_mean": float(np.mean(a)),
                "non_inundated_mean": float(np.mean(b)),
                "mean_diff": float(np.mean(a) - np.mean(b)),
                "abs_cohens_d": float(abs((np.mean(a) - np.mean(b)) / pooled)) if pooled > 0 else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("abs_cohens_d", ascending=False)


def add_predictions(df: pd.DataFrame, model_info: dict) -> pd.DataFrame:
    out = df.copy()
    prob = model_info["estimator"].predict_proba(out[FEATURES].to_numpy(np.float32))[:, 1]
    pred = prob >= model_info["threshold"]
    out["predicted_probability"] = prob
    out["predicted_inundated"] = pred.astype(np.uint8)
    conditions = [
        (out[LABEL_COL] == 1) & (out["predicted_inundated"] == 1),
        (out[LABEL_COL] == 1) & (out["predicted_inundated"] == 0),
        (out[LABEL_COL] == 0) & (out["predicted_inundated"] == 1),
        (out[LABEL_COL] == 0) & (out["predicted_inundated"] == 0),
    ]
    out["confusion_group"] = np.select(conditions, ["TP", "FN", "FP", "TN"], default="unknown")
    return out


def export_prediction(full_pred: pd.DataFrame, shape: tuple[int, int]) -> None:
    prob_arr = np.full(shape, np.nan, dtype=np.float32)
    pred_arr = np.zeros(shape, dtype=np.uint8)
    group_arr = np.zeros(shape, dtype=np.uint8)
    rows = full_pred["row"].to_numpy(np.int32)
    cols = full_pred["col"].to_numpy(np.int32)
    prob_arr[rows, cols] = full_pred["predicted_probability"].to_numpy(np.float32)
    pred_arr[rows, cols] = full_pred["predicted_inundated"].to_numpy(np.uint8)
    group_code = {"TP": 1, "FN": 2, "FP": 3, "TN": 4}
    group_arr[rows, cols] = full_pred["confusion_group"].map(group_code).to_numpy(np.uint8)
    write_raster(OUT / "GIS_bbox全画素20000_浸水確率.tif", prob_arr, "float32", np.nan)
    write_raster(OUT / "GIS_bbox全画素20000_浸水判定.tif", pred_arr, "uint8", 0)
    write_raster(OUT / "GIS_bbox全画素20000_混同行列群.tif", group_arr, "uint8", 0)


def plot_confusion(row: pd.Series, path: Path) -> None:
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
    ax.set_title("検証データの混同行列", fontproperties=FONT)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    plt.tight_layout()
    plt.savefig(path, dpi=200, bbox_inches="tight")
    plt.close()


def plot_feature_distributions(df: pd.DataFrame, effect: pd.DataFrame) -> None:
    top_features = effect.head(8)["feature"].tolist()
    plot_df = df.copy()
    plot_df["正解ラベル"] = np.where(plot_df[LABEL_COL] == 1, "浸水域", "非浸水域")
    for feature in top_features:
        fig, ax = plt.subplots(figsize=(7.5, 4.6))
        data = [
            plot_df.loc[plot_df["正解ラベル"] == "浸水域", feature].sample(
                n=min(20000, (plot_df["正解ラベル"] == "浸水域").sum()), random_state=SEED
            ),
            plot_df.loc[plot_df["正解ラベル"] == "非浸水域", feature].sample(
                n=min(20000, (plot_df["正解ラベル"] == "非浸水域").sum()), random_state=SEED
            ),
        ]
        ax.hist(data[0], bins=80, alpha=0.55, density=True, label="浸水域", color="#e45756")
        ax.hist(data[1], bins=80, alpha=0.55, density=True, label="非浸水域", color="#4c78a8")
        ax.set_title(f"正解ラベル別の分布: {feature}", fontproperties=FONT)
        ax.set_xlabel(feature)
        ax.set_ylabel("密度", fontproperties=FONT)
        ax.legend(prop=FONT)
        ax.grid(alpha=0.25)
        plt.tight_layout()
        plt.savefig(OUT / f"分布_正解別_{feature}.png", dpi=200, bbox_inches="tight")
        plt.close()


def plot_confusion_group_boxplots(pred_df: pd.DataFrame, effect: pd.DataFrame) -> None:
    top_features = effect.head(8)["feature"].tolist()
    sample_frames = []
    for group, g in pred_df.groupby("confusion_group"):
        sample_frames.append(g.sample(n=min(15000, len(g)), random_state=SEED))
    plot_df = pd.concat(sample_frames, ignore_index=True)
    order = ["TP", "FN", "FP", "TN"]
    for feature in top_features:
        fig, ax = plt.subplots(figsize=(7.8, 4.8))
        data = [plot_df.loc[plot_df["confusion_group"] == group, feature].to_numpy(np.float32) for group in order]
        ax.boxplot(data, tick_labels=order, showfliers=False)
        ax.set_title(f"予測群別の特徴量分布: {feature}", fontproperties=FONT)
        ax.set_ylabel(feature)
        ax.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(OUT / f"箱ひげ_予測群別_{feature}.png", dpi=200, bbox_inches="tight")
        plt.close()


def plot_feature_importance(model_info: dict) -> pd.DataFrame:
    model = model_info["estimator"]
    final_model = model.named_steps["model"] if hasattr(model, "named_steps") else model
    if not hasattr(final_model, "feature_importances_"):
        return pd.DataFrame()
    imp = pd.DataFrame({"feature": FEATURES, "importance": final_model.feature_importances_}).sort_values("importance", ascending=False)
    fig, ax = plt.subplots(figsize=(8, 5.2))
    top = imp.head(12).iloc[::-1]
    ax.barh(top["feature"], top["importance"], color="#4c78a8")
    ax.set_title("最良モデルの特徴量重要度", fontproperties=FONT)
    ax.set_xlabel("importance")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT / "図_特徴量重要度.png", dpi=200, bbox_inches="tight")
    plt.close()
    return imp


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


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame, effect: pd.DataFrame, full_group_counts: pd.DataFrame, importance: pd.DataFrame) -> None:
    best = metrics.sort_values(["mode", "balanced_accuracy"], ascending=[True, False]).groupby("mode").head(1)
    explanation = [
        "FPが多い主な理由は、後方散乱強度差分だけでは浸水域と非浸水域の分布が大きく重なっているためです。",
        "特に建物・構造物・畦畔・水田内の粗い表面などは、降雨後に強い散乱変化や時間変化を示すことがあり、浸水域に似た時系列特徴として扱われます。",
        "今回のモデルは地物種別や建物マスクを使っていないため、bbox内の全画素を対象にすると、浸水と似た後方散乱挙動を持つ非浸水画素を除外できません。",
    ]
    lines = [
        "# bbox内全画素 20,000画素抽出による特徴診断レポート",
        "",
        "## 条件",
        "",
        "- bbox内の全有効画素を対象にしました。",
        "- 正解浸水域20,000画素、非浸水域20,000画素を抽出しました。",
        "- 訓練:検証 = 7:3 で分割しました。",
        f"- GridSearchCVは訓練データ内で {CV_SPLITS}-fold Stratified CV を行いました。",
        "- 特徴量は4時間帯の後方散乱強度差分と、その時系列統計量です。",
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## モデル精度",
        "",
        md_table(best[["mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_params"]]),
        "",
        "## bbox全画素へ適用したときの予測群数",
        "",
        md_table(full_group_counts),
        "",
        "## 浸水域と非浸水域を分ける特徴量",
        "",
        md_table(effect.head(10)),
        "",
        "## 誤検出が多い理由",
        "",
        *[f"- {x}" for x in explanation],
        "",
    ]
    if not importance.empty:
        lines.extend(["## 最良モデルの特徴量重要度", "", md_table(importance.head(10)), ""])
    lines.extend(
        [
            "## 主な出力",
            "",
            "- `全モデル評価指標.csv`",
            "- `正解ラベル別_特徴量統計.csv`",
            "- `全画素_予測群別_特徴量統計.csv`",
            "- `特徴量効果量.csv`",
            "- `GIS_bbox全画素20000_浸水判定.tif`",
            "- `GIS_bbox全画素20000_浸水確率.tif`",
            "- `GIS_bbox全画素20000_混同行列群.tif`",
            "- `分布_正解別_*.png`",
            "- `箱ひげ_予測群別_*.png`",
            "",
        ]
    )
    (OUT / "bbox全画素_20000特徴診断レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    full_df, shape = build_frame()
    sampled = balanced_sample(full_df)
    metrics, scans, best_grid, _ = evaluate(sampled)
    sampled_pred = add_predictions(sampled, best_grid)
    full_pred = add_predictions(full_df, best_grid)
    effect = effect_size_by_truth(full_df)
    truth_summary = summarize_features(full_df.assign(truth_label=np.where(full_df[LABEL_COL] == 1, "浸水域", "非浸水域")), "truth_label")
    group_summary = summarize_features(full_pred, "confusion_group")
    importance = plot_feature_importance(best_grid)

    counts = pd.DataFrame(
        [
            {
                "target": "bbox内全有効画素",
                "available_positive": int((full_df[LABEL_COL] == 1).sum()),
                "available_negative": int((full_df[LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[LABEL_COL] == 1).sum()),
                "sampled_negative": int((sampled[LABEL_COL] == 0).sum()),
                "train_positive": int((sampled_pred[LABEL_COL] == 1).sum() * (1 - TEST_SIZE)),
                "train_negative": int((sampled_pred[LABEL_COL] == 0).sum() * (1 - TEST_SIZE)),
                "test_positive": int(N_PER_CLASS * TEST_SIZE),
                "test_negative": int(N_PER_CLASS * TEST_SIZE),
                "feature_count": len(FEATURES),
            }
        ]
    )
    full_group_counts = full_pred["confusion_group"].value_counts().reindex(["TP", "FN", "FP", "TN"]).fillna(0).astype(int).reset_index()
    full_group_counts.columns = ["confusion_group", "pixel_count"]

    counts.to_csv(OUT / "母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン.csv", index=False, encoding="utf-8-sig")
    sampled.to_csv(OUT / "抽出データ_20000ずつ.csv", index=False, encoding="utf-8-sig")
    sampled_pred.to_csv(OUT / "抽出データ_予測付き.csv", index=False, encoding="utf-8-sig")
    full_pred[["row", "col", LABEL_COL, "predicted_probability", "predicted_inundated", "confusion_group"]].to_csv(
        OUT / "bbox全画素_予測一覧.csv", index=False, encoding="utf-8-sig"
    )
    truth_summary.to_csv(OUT / "正解ラベル別_特徴量統計.csv", index=False, encoding="utf-8-sig")
    group_summary.to_csv(OUT / "全画素_予測群別_特徴量統計.csv", index=False, encoding="utf-8-sig")
    effect.to_csv(OUT / "特徴量効果量.csv", index=False, encoding="utf-8-sig")
    if not importance.empty:
        importance.to_csv(OUT / "特徴量重要度.csv", index=False, encoding="utf-8-sig")

    export_prediction(full_pred, shape)
    best_row = metrics.sort_values(["mode", "balanced_accuracy"], ascending=[True, False]).groupby("mode").head(1)
    grid_best = best_row[best_row["mode"] == "GridSearch"].iloc[0]
    plot_confusion(grid_best, OUT / "混同行列_検証データ_GridSearch最良.png")
    plot_feature_distributions(full_df, effect)
    plot_confusion_group_boxplots(full_pred, effect)
    write_report(counts, metrics, effect, full_group_counts, importance)

    print(best_row[["mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]].to_string(index=False))
    print(full_group_counts.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
