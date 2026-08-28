from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd
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
SRC = BASE / "bbox_paddy_satoyama_classification"
OUT = BASE / "bbox_paddy_satoyama_only_classification"
SAMPLED = SRC / "抽出データ_さとやま追加.csv"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

SEED = 42
TEST_SIZE = 0.30
CV_SPLITS = 3
LABEL_COL = "label"

FEATURES_BY_SCENARIO = {
    "田んぼ画素_bbox内": ["satoyama_mean"],
    "田んぼ筆ポリゴン_bbox内": [
        "satoyama_mean",
        "satoyama_median",
        "satoyama_std",
        "satoyama_min",
        "satoyama_max",
        "satoyama_range",
    ],
}


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


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
                "max_depth": [3, 5, 8, None],
                "min_samples_leaf": [5, 10, 30, 80],
                "max_features": ["sqrt", "log2", None],
            },
        ),
        "XGBoost": (
            XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            {
                "n_estimators": [80, 120, 180],
                "max_depth": [1, 2, 3],
                "learning_rate": [0.03, 0.05, 0.08],
                "min_child_weight": [1, 3, 5],
                "subsample": [0.8, 0.95],
                "colsample_bytree": [0.8, 0.95],
            },
        ),
    }


def threshold_metrics(y_true: np.ndarray, prob: np.ndarray, scenario: str, mode: str, model: str) -> tuple[dict, pd.DataFrame]:
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


def evaluate(df: pd.DataFrame, scenario: str, features: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    valid = df.dropna(subset=features + [LABEL_COL]).copy()
    x = valid[features].to_numpy(np.float32)
    y = valid[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    cv = StratifiedKFold(n_splits=CV_SPLITS, shuffle=True, random_state=SEED)

    metric_rows = []
    scans = []
    importances = []
    for model_name, model in model_specs_default().items():
        model.fit(x_train, y_train)
        prob = model.predict_proba(x_test)[:, 1]
        best, scan = threshold_metrics(y_test, prob, scenario, "通常設定", model_name)
        metric_rows.append(best)
        scans.append(scan)
        importances.extend(importance_rows(model, features, scenario, "通常設定", model_name))

    for model_name, (estimator, grid) in model_specs_grid().items():
        search = GridSearchCV(estimator, grid, scoring="balanced_accuracy", cv=cv, n_jobs=1, refit=True)
        search.fit(x_train, y_train)
        prob = search.predict_proba(x_test)[:, 1]
        best, scan = threshold_metrics(y_test, prob, scenario, "GridSearch", model_name)
        best["best_cv_balanced_accuracy"] = float(search.best_score_)
        best["best_params"] = json.dumps(search.best_params_, ensure_ascii=False)
        metric_rows.append(best)
        scans.append(scan)
        importances.extend(importance_rows(search.best_estimator_, features, scenario, "GridSearch", model_name))

    return pd.DataFrame(metric_rows), pd.concat(scans, ignore_index=True), pd.DataFrame(importances)


def importance_rows(model, features: list[str], scenario: str, mode: str, model_name: str) -> list[dict]:
    if not hasattr(model, "feature_importances_"):
        return []
    return [
        {
            "scenario": scenario,
            "mode": mode,
            "model": model_name,
            "feature": feature,
            "importance": float(value),
        }
        for feature, value in zip(features, model.feature_importances_)
    ]


def read_comparison() -> pd.DataFrame:
    rows = []
    for feature_set, path in [
        ("後方散乱のみ", BASE / "bbox_balanced_classification"),
        ("後方散乱+さとやま指数", SRC),
    ]:
        matches = sorted(p for p in path.glob("*.csv") if ("全モデル評価" in p.stem or "評価指標" in p.stem))
        if not matches:
            continue
        df = pd.read_csv(matches[0], encoding="utf-8-sig")
        df = df[df["scenario"].isin(FEATURES_BY_SCENARIO.keys()) & df["model"].isin(["ランダムフォレスト", "XGBoost"])].copy()
        df["feature_set"] = feature_set
        rows.append(df)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def plot_metric_comparison(comp: pd.DataFrame) -> None:
    if comp.empty:
        return
    comp = comp.copy()
    comp["mode"] = comp["mode"].replace({"全特徴量_GridSearch": "GridSearch", "全特徴量_通常設定": "通常設定"})
    comp = comp[comp["mode"] == "GridSearch"]
    for metric in ["balanced_accuracy", "F1", "ROC_AUC"]:
        labels = []
        values = []
        colors = []
        color_map = {
            "後方散乱のみ": "#9ecae1",
            "後方散乱+さとやま指数": "#31a354",
            "さとやま指数のみ": "#fd8d3c",
        }
        for scenario in FEATURES_BY_SCENARIO:
            for model in ["ランダムフォレスト", "XGBoost"]:
                for feature_set in ["後方散乱のみ", "後方散乱+さとやま指数", "さとやま指数のみ"]:
                    row = comp[(comp["scenario"] == scenario) & (comp["model"] == model) & (comp["feature_set"] == feature_set)]
                    if row.empty:
                        continue
                    labels.append(f"{scenario.replace('_bbox内','')}\n{model}\n{feature_set}")
                    values.append(float(row[metric].iloc[0]))
                    colors.append(color_map[feature_set])
        fig, ax = plt.subplots(figsize=(12, 5.6))
        ax.bar(np.arange(len(values)), values, color=colors)
        ax.set_xticks(np.arange(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_ylim(0, 1.0)
        ax.set_ylabel(metric)
        ax.set_title(f"特徴量セット別 {metric} 比較")
        ax.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        fig.savefig(OUT / f"図_特徴量セット別_{metric}_比較.png", dpi=240, bbox_inches="tight")
        plt.close(fig)


def plot_importance(imp: pd.DataFrame) -> None:
    if imp.empty:
        return
    for (scenario, mode, model), sub in imp.groupby(["scenario", "mode", "model"]):
        sub = sub.sort_values("importance", ascending=True)
        fig, ax = plt.subplots(figsize=(7.4, 4.8))
        ax.barh(sub["feature"], sub["importance"], color="#fd8d3c")
        ax.set_title(f"{scenario} {mode} {model}\nさとやま指数のみの特徴量重要度")
        ax.set_xlabel("importance")
        ax.grid(axis="x", alpha=0.3)
        safe = f"{scenario}_{mode}_{model}".replace("/", "_").replace("\\", "_")
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


def write_report(metrics: pd.DataFrame, comparison: pd.DataFrame, imp: pd.DataFrame, counts: pd.DataFrame) -> None:
    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    grid_compare = comparison.copy()
    grid_compare["mode"] = grid_compare["mode"].replace({"全特徴量_GridSearch": "GridSearch", "全特徴量_通常設定": "通常設定"})
    grid_compare = grid_compare[grid_compare["mode"] == "GridSearch"]
    lines = [
        "# さとやま指数のみの浸水域判別レポート",
        "",
        "## 条件",
        "",
        "- 前回作成した同数抽出データ `抽出データ_さとやま追加.csv` を使用した。",
        "- 画素単位では `satoyama_mean` のみを特徴量にした。",
        "- 筆ポリゴン単位では `satoyama_mean`, `satoyama_median`, `satoyama_std`, `satoyama_min`, `satoyama_max`, `satoyama_range` のみを特徴量にした。",
        "- Random Forest と XGBoost について、通常設定とGridSearchを評価した。",
        "- 閾値は検証データ上でBalanced Accuracyが最大になる値を採用した。",
        "",
        "## 母数",
        "",
        fmt_table(counts),
        "",
        "## さとやま指数のみの評価",
        "",
        fmt_table(best[["scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_params"]]),
        "",
        "## 特徴量セット比較",
        "",
        fmt_table(grid_compare[["feature_set", "scenario", "mode", "model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]]),
        "",
        "## 特徴量重要度",
        "",
        fmt_table(imp),
        "",
        "## 図",
        "",
        "- `図_特徴量セット別_balanced_accuracy_比較.png`",
        "- `図_特徴量セット別_F1_比較.png`",
        "- `図_特徴量セット別_ROC_AUC_比較.png`",
        "- `図_特徴量重要度_*.png`",
        "",
        "## 解釈",
        "",
        "さとやま指数のみでも一定の判別性能が出る場合、浸水/非浸水の違いに土地環境・空間分布の差が強く含まれていることを示す。これは有用な補助特徴量である一方、ランダム分割では位置的な偏りを学習して精度が高く見える可能性があるため、地域外検証やTIF別検証での確認が必要である。",
    ]
    (OUT / "さとやま指数のみ_浸水判別レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    sampled = pd.read_csv(SAMPLED, encoding="utf-8-sig")
    metrics_all = []
    scans_all = []
    imps_all = []
    counts = []
    for scenario, features in FEATURES_BY_SCENARIO.items():
        df = sampled[sampled["scenario"] == scenario].copy()
        valid = df.dropna(subset=features + [LABEL_COL]).copy()
        counts.append(
            {
                "scenario": scenario,
                "rows": int(len(valid)),
                "positive": int((valid[LABEL_COL] == 1).sum()),
                "negative": int((valid[LABEL_COL] == 0).sum()),
                "feature_count": len(features),
                "features": ", ".join(features),
            }
        )
        metrics, scans, imp = evaluate(valid, scenario, features)
        metrics["feature_set"] = "さとやま指数のみ"
        scans["feature_set"] = "さとやま指数のみ"
        imp["feature_set"] = "さとやま指数のみ"
        metrics_all.append(metrics)
        scans_all.append(scans)
        imps_all.append(imp)
        print(f"evaluated {scenario}")

    metrics = pd.concat(metrics_all, ignore_index=True)
    scans = pd.concat(scans_all, ignore_index=True)
    imp = pd.concat(imps_all, ignore_index=True)
    counts_df = pd.DataFrame(counts)
    comparison = pd.concat([read_comparison(), metrics], ignore_index=True)

    counts_df.to_csv(OUT / "母数_さとやま指数のみ.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "評価指標_さとやま指数のみ.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン_さとやま指数のみ.csv", index=False, encoding="utf-8-sig")
    imp.to_csv(OUT / "特徴量重要度_さとやま指数のみ.csv", index=False, encoding="utf-8-sig")
    comparison.to_csv(OUT / "特徴量セット別_比較.csv", index=False, encoding="utf-8-sig")

    plot_metric_comparison(comparison)
    plot_importance(imp)
    write_report(metrics, comparison, imp, counts_df)

    best = metrics.sort_values(["scenario", "mode", "balanced_accuracy"], ascending=[True, True, False]).groupby(["scenario", "mode"]).head(1)
    print(best[["scenario", "mode", "model", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
