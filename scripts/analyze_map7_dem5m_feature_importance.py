from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.metrics import balanced_accuracy_score
from sklearn.model_selection import train_test_split

import analyze_map7_bbox_paddy_pixel_polygon_20000 as base
import export_map7_paddy_dem5m_probabilities as prob_base
import run_map7_paddy_dem5m_classification as dem_base


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_paddy_dem5m_feature_importance"
SEED = 42
TEST_SIZE = 0.30
LABEL_COL = "label"


def train_best_xgboost(sampled: pd.DataFrame, features: list[str], threshold: float):
    x = sampled[features].to_numpy(np.float32)
    y = sampled[LABEL_COL].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=TEST_SIZE, stratify=y, random_state=SEED)
    model = prob_base.models()["XGBoost"]
    model.fit(x_train, y_train)

    def scorer(estimator, x_eval, y_eval):
        pred = (estimator.predict_proba(x_eval)[:, 1] >= threshold).astype(np.uint8)
        return balanced_accuracy_score(y_eval, pred)

    perm = permutation_importance(
        model,
        x_test,
        y_test,
        scoring=scorer,
        n_repeats=8,
        random_state=SEED,
        n_jobs=1,
    )
    intrinsic = getattr(model, "feature_importances_", np.zeros(len(features), dtype=np.float32))
    df = pd.DataFrame(
        {
            "feature": features,
            "xgboost_importance": intrinsic,
            "permutation_importance_mean": perm.importances_mean,
            "permutation_importance_std": perm.importances_std,
        }
    )
    df = df.sort_values("permutation_importance_mean", ascending=False).reset_index(drop=True)
    pred = (model.predict_proba(x_test)[:, 1] >= threshold).astype(np.uint8)
    score = balanced_accuracy_score(y_test, pred)
    return df, score


def plot_importance(df: pd.DataFrame, title: str, path: Path, top_n: int = 15) -> None:
    shown = df.head(top_n).iloc[::-1]
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(shown["feature"], shown["permutation_importance_mean"], xerr=shown["permutation_importance_std"], color="#4c78a8")
    ax.set_xlabel("Permutation Importance / Balanced Accuracy低下量")
    ax.set_title(title)
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(path, dpi=220, bbox_inches="tight")
    plt.close()


def md_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: f"{x:.4f}")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def write_report(pixel_imp: pd.DataFrame, polygon_imp: pd.DataFrame, scores: dict[str, float]) -> None:
    lines = [
        "# DEM5mありモデルの特徴量貢献度",
        "",
        "## 方法",
        "",
        "- 対象はGSI DEM 5mを加えた田んぼ画素モデル・田んぼ筆ポリゴンモデルです。",
        "- モデルは前回の再算出と同じXGBoost設定です。",
        "- 検証データ上で各特徴量をシャッフルし、Balanced Accuracyがどれだけ低下するかをPermutation Importanceとして算出しました。",
        "- `xgboost_importance` はXGBoost内部の重要度、`permutation_importance_mean` は検証データでの精度低下量です。",
        "",
        "## 検証スコア",
        "",
        md_table(pd.DataFrame([{"scenario": k, "balanced_accuracy": v} for k, v in scores.items()])),
        "",
        "## 田んぼ画素 上位特徴量",
        "",
        md_table(pixel_imp.head(15)),
        "",
        "## 田んぼ筆ポリゴン 上位特徴量",
        "",
        md_table(polygon_imp.head(15)),
        "",
        "## 主な出力",
        "",
        "- `田んぼ画素_DEM5m_特徴量重要度.csv`",
        "- `田んぼ筆ポリゴン_DEM5m_特徴量重要度.csv`",
        "- `図_田んぼ画素_DEM5m_特徴量重要度.png`",
        "- `図_田んぼ筆ポリゴン_DEM5m_特徴量重要度.png`",
        "",
    ]
    (OUT / "DEM5m特徴量貢献度レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pixel_df, polygon_df, _, _, _ = prob_base.load_frames()

    pixel_features = base.PIXEL_FEATURES + dem_base.PIXEL_DEM_FEATURES
    polygon_features = base.POLYGON_FEATURES + dem_base.POLYGON_DEM_FEATURES

    pixel_sample = prob_base.balanced_sample(pixel_df, pixel_features, prob_base.PIXEL_PER_CLASS)
    polygon_sample = prob_base.balanced_sample(polygon_df, polygon_features, prob_base.POLYGON_PER_CLASS)

    pixel_imp, pixel_score = train_best_xgboost(pixel_sample, pixel_features, threshold=0.565)
    polygon_imp, polygon_score = train_best_xgboost(polygon_sample, polygon_features, threshold=0.445)

    pixel_imp.to_csv(OUT / "田んぼ画素_DEM5m_特徴量重要度.csv", index=False, encoding="utf-8-sig")
    polygon_imp.to_csv(OUT / "田んぼ筆ポリゴン_DEM5m_特徴量重要度.csv", index=False, encoding="utf-8-sig")
    plot_importance(pixel_imp, "田んぼ画素 DEM5mありモデル 特徴量重要度", OUT / "図_田んぼ画素_DEM5m_特徴量重要度.png")
    plot_importance(polygon_imp, "田んぼ筆ポリゴン DEM5mありモデル 特徴量重要度", OUT / "図_田んぼ筆ポリゴン_DEM5m_特徴量重要度.png")
    write_report(pixel_imp, polygon_imp, {"田んぼ画素": pixel_score, "田んぼ筆ポリゴン": polygon_score})
    print("田んぼ画素 top10")
    print(pixel_imp.head(10).to_string(index=False))
    print("田んぼ筆ポリゴン top10")
    print(polygon_imp.head(10).to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
