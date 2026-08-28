from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, confusion_matrix, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
INPUT_CSV = BASE_DIR / "paddy_polygon_1000_classification/map7_paddy_polygon_features_all.csv"
OUT_DIR = BASE_DIR / "paddy_polygon_inundation_ratio_thresholds"

SEED = 42
N_PER_CLASS = 1000
THRESHOLDS = [0.0, 0.05, 0.10, 0.20, 0.30, 0.50]

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


def threshold_label(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    if threshold == 0.0:
        use = df.copy()
        use["label"] = (use["truth_pixel_ratio"] > 0).astype(np.uint8)
        use["excluded_low_positive"] = False
        return use
    keep = (df["truth_pixel_ratio"] == 0) | (df["truth_pixel_ratio"] >= threshold)
    use = df[keep].copy()
    use["label"] = (use["truth_pixel_ratio"] >= threshold).astype(np.uint8)
    use["excluded_low_positive"] = False
    return use


def sample_balanced(df: pd.DataFrame) -> pd.DataFrame:
    pos = df[df["label"] == 1]
    neg = df[df["label"] == 0]
    n = min(N_PER_CLASS, len(pos), len(neg))
    return pd.concat(
        [pos.sample(n=n, random_state=SEED), neg.sample(n=n, random_state=SEED)],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)


def models() -> dict:
    return {
        "logistic_regression": Pipeline(
            [("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]
        ),
        "decision_tree": DecisionTreeClassifier(max_depth=5, min_samples_leaf=30, random_state=SEED),
        "random_forest": RandomForestClassifier(
            n_estimators=300,
            max_depth=10,
            min_samples_leaf=30,
            max_features=0.7,
            random_state=SEED,
            n_jobs=1,
        ),
        "xgboost": XGBClassifier(
            objective="binary:logistic",
            eval_metric="logloss",
            n_estimators=160,
            max_depth=3,
            learning_rate=0.03,
            min_child_weight=10,
            subsample=0.85,
            colsample_bytree=0.85,
            random_state=SEED,
            n_jobs=1,
        ),
    }


def best_metrics(y_true: np.ndarray, prob: np.ndarray, model_name: str, threshold_ratio: float) -> tuple[dict, pd.DataFrame]:
    rows = []
    for pred_threshold in np.linspace(0.05, 0.95, 181):
        pred = (prob >= pred_threshold).astype(np.int32)
        tn, fp, fn, tp = confusion_matrix(y_true, pred, labels=[0, 1]).ravel()
        rows.append(
            {
                "inundation_ratio_threshold": threshold_ratio,
                "model": model_name,
                "prediction_threshold": float(pred_threshold),
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


def evaluate(sampled: pd.DataFrame, threshold_ratio: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    x = sampled[FEATURE_COLS].to_numpy(np.float32)
    y = sampled["label"].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, stratify=y, random_state=SEED)
    metric_rows = []
    scan_rows = []
    for name, model in models().items():
        model.fit(x_train, y_train)
        prob = model.predict_proba(x_test)[:, 1]
        best, scan = best_metrics(y_test, prob, name, threshold_ratio)
        metric_rows.append(best)
        scan_rows.append(scan)
    return pd.DataFrame(metric_rows), pd.concat(scan_rows, ignore_index=True)


def cohen_d(pos: pd.Series, neg: pd.Series) -> float:
    pos = pos.dropna().to_numpy(float)
    neg = neg.dropna().to_numpy(float)
    pooled = np.sqrt(((len(pos) - 1) * pos.var(ddof=1) + (len(neg) - 1) * neg.var(ddof=1)) / (len(pos) + len(neg) - 2))
    return float((pos.mean() - neg.mean()) / pooled) if pooled > 0 else np.nan


def feature_separation(sampled: pd.DataFrame, threshold_ratio: float) -> pd.DataFrame:
    rows = []
    for feature in FEATURE_COLS:
        pos = sampled.loc[sampled["label"] == 1, feature]
        neg = sampled.loc[sampled["label"] == 0, feature]
        rows.append(
            {
                "inundation_ratio_threshold": threshold_ratio,
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
    return pd.DataFrame(rows).sort_values(["inundation_ratio_threshold", "abs_cohen_d"], ascending=[True, False])


def plot_metric(metrics: pd.DataFrame) -> None:
    best = metrics.sort_values(["inundation_ratio_threshold", "balanced_accuracy"], ascending=[True, False]).groupby("inundation_ratio_threshold").head(1)
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(best["inundation_ratio_threshold"] * 100, best["balanced_accuracy"], marker="o", label="balanced accuracy")
    ax.plot(best["inundation_ratio_threshold"] * 100, best["ROC_AUC"], marker="s", label="ROC AUC")
    ax.set_xlabel("positive definition: inundated pixel ratio threshold (%)")
    ax.set_ylabel("score")
    ax.set_ylim(0.5, max(0.85, float(best[["balanced_accuracy", "ROC_AUC"]].max().max()) + 0.03))
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "best_model_score_by_inundation_ratio_threshold.png", dpi=180)
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
    df = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")
    df = df.dropna(subset=FEATURE_COLS + ["truth_pixel_ratio"]).copy()

    count_rows = []
    sample_frames = []
    metric_frames = []
    scan_frames = []
    sep_frames = []
    for threshold in THRESHOLDS:
        labeled = threshold_label(df, threshold)
        excluded = int(((df["truth_pixel_ratio"] > 0) & (df["truth_pixel_ratio"] < threshold)).sum()) if threshold > 0 else 0
        pos_count = int((labeled["label"] == 1).sum())
        neg_count = int((labeled["label"] == 0).sum())
        sampled = sample_balanced(labeled)
        sampled["inundation_ratio_threshold"] = threshold
        sample_frames.append(sampled)
        count_rows.append(
            {
                "inundation_ratio_threshold": threshold,
                "positive_polygons": pos_count,
                "negative_polygons": neg_count,
                "excluded_low_ratio_positive_polygons": excluded,
                "sampled_positive_polygons": int((sampled["label"] == 1).sum()),
                "sampled_negative_polygons": int((sampled["label"] == 0).sum()),
            }
        )
        metrics, scans = evaluate(sampled, threshold)
        metric_frames.append(metrics)
        scan_frames.append(scans)
        sep_frames.append(feature_separation(sampled, threshold))

    counts = pd.DataFrame(count_rows)
    metrics = pd.concat(metric_frames, ignore_index=True).sort_values(["inundation_ratio_threshold", "balanced_accuracy"], ascending=[True, False])
    scans = pd.concat(scan_frames, ignore_index=True)
    separations = pd.concat(sep_frames, ignore_index=True)
    samples = pd.concat(sample_frames, ignore_index=True)

    counts.to_csv(OUT_DIR / "polygon_count_by_inundation_ratio_threshold.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT_DIR / "model_metrics_by_inundation_ratio_threshold.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT_DIR / "threshold_scans_by_inundation_ratio_threshold.csv", index=False, encoding="utf-8-sig")
    separations.to_csv(OUT_DIR / "feature_separation_by_inundation_ratio_threshold.csv", index=False, encoding="utf-8-sig")
    samples.to_csv(OUT_DIR / "sampled_polygons_by_inundation_ratio_threshold.csv", index=False, encoding="utf-8-sig")
    plot_metric(metrics)

    best_by_threshold = metrics.groupby("inundation_ratio_threshold", group_keys=False).head(1)
    top_features = separations.groupby("inundation_ratio_threshold", group_keys=False).head(8)
    report = [
        "# 田んぼ筆ポリゴン単位: 浸水率閾値別の分類比較",
        "",
        "## ラベル定義",
        "",
        "- positive: `truth_pixel_count / sentinel_pixel_count >= 閾値`",
        "- negative: `truth_pixel_ratio == 0`",
        "- `0 < truth_pixel_ratio < 閾値` の筆は、弱い浸水筆として曖昧なため除外",
        "- 各閾値で positive / negative から最大1,000筆ずつランダム抽出",
        "",
        "## 閾値別の母数",
        "",
        md_table(counts),
        "",
        "## 各閾値の最良モデル",
        "",
        md_table(best_by_threshold[["inundation_ratio_threshold", "model", "prediction_threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN"]]),
        "",
        "## 全モデル結果",
        "",
        md_table(metrics[["inundation_ratio_threshold", "model", "prediction_threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]]),
        "",
        "## 閾値別に分離が大きい特徴量",
        "",
        md_table(top_features[["inundation_ratio_threshold", "feature", "truth_mean", "nontruth_mean", "mean_difference", "cohen_d"]]),
        "",
        "## 出力",
        "",
        "- `polygon_count_by_inundation_ratio_threshold.csv`",
        "- `model_metrics_by_inundation_ratio_threshold.csv`",
        "- `feature_separation_by_inundation_ratio_threshold.csv`",
        "- `sampled_polygons_by_inundation_ratio_threshold.csv`",
        "- `best_model_score_by_inundation_ratio_threshold.png`",
        "",
    ]
    (OUT_DIR / "polygon_inundation_ratio_threshold_report.md").write_text("\n".join(report), encoding="utf-8")
    print(best_by_threshold.to_string(index=False))
    print(f"Wrote {OUT_DIR}")


if __name__ == "__main__":
    main()
