from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
SRC_DIR = BASE / "bbox_balanced_classification"
OUT = BASE / "paper_pixel_polygon_classification_summary"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")

TARGET_SCENARIOS = ["田んぼ画素_bbox内", "田んぼ筆ポリゴン_bbox内"]
TARGET_MODELS = ["ランダムフォレスト", "XGBoost"]
METRICS = ["balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


def find_csv(stem_part: str) -> Path:
    matches = sorted(p for p in SRC_DIR.glob("*.csv") if stem_part in p.stem)
    if not matches:
        raise FileNotFoundError(f"{SRC_DIR} に {stem_part} を含むCSVがありません")
    return matches[0]


def load_metrics() -> pd.DataFrame:
    df = pd.read_csv(find_csv("全モデル評価"), encoding="utf-8-sig")
    df = df[df["scenario"].isin(TARGET_SCENARIOS) & df["model"].isin(TARGET_MODELS)].copy()
    for col in METRICS + ["threshold", "TP", "FP", "FN", "TN"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def preferred_rows(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    # 論文本文では画素単位はGridSearch同士の比較、筆ポリゴン単位はRF通常設定とXGB GridSearchが対応しやすい。
    choices = [
        ("田んぼ画素_bbox内", "全特徴量_GridSearch", "ランダムフォレスト"),
        ("田んぼ画素_bbox内", "全特徴量_GridSearch", "XGBoost"),
        ("田んぼ筆ポリゴン_bbox内", "全特徴量_通常設定", "ランダムフォレスト"),
        ("田んぼ筆ポリゴン_bbox内", "全特徴量_GridSearch", "XGBoost"),
    ]
    for scenario, mode, model in choices:
        sub = df[(df["scenario"] == scenario) & (df["mode"] == mode) & (df["model"] == model)]
        if sub.empty:
            sub = df[(df["scenario"] == scenario) & (df["model"] == model)].sort_values(["F1", "balanced_accuracy"], ascending=False)
        rows.append(sub.iloc[0])
    out = pd.DataFrame(rows).reset_index(drop=True)
    out["unit"] = out["scenario"].map({"田んぼ画素_bbox内": "画素単位", "田んぼ筆ポリゴン_bbox内": "筆ポリゴン単位"})
    return out


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


def latex_table(df: pd.DataFrame) -> str:
    cols = ["unit", "model", "mode", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]
    rename = {
        "unit": "評価単位",
        "model": "モデル",
        "mode": "条件",
        "balanced_accuracy": "BA",
        "precision": "適合率",
        "recall": "再現率",
        "specificity": "特異度",
        "F1": "F1",
        "ROC_AUC": "AUC",
    }
    table = df[cols].rename(columns=rename).copy()
    return table.to_latex(index=False, float_format="%.3f", escape=False)


def plot_model_bars(df: pd.DataFrame, unit: str, filename: str) -> None:
    sub = df[df["unit"] == unit].copy()
    plot_cols = ["balanced_accuracy", "precision", "recall", "specificity", "F1"]
    labels = ["BA", "適合率", "再現率", "特異度", "F1"]
    x = np.arange(len(labels))
    width = 0.36
    fig, ax = plt.subplots(figsize=(8.8, 5.0))
    for i, model in enumerate(TARGET_MODELS):
        vals = sub[sub["model"] == model].iloc[0][plot_cols].astype(float).to_numpy()
        ax.bar(x + (i - 0.5) * width, vals, width=width, label=model)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, 1)
    ax.set_ylabel("スコア")
    ax.set_title(f"{unit}におけるモデル別判別性能")
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    plt.tight_layout()
    fig.savefig(OUT / filename, dpi=240, bbox_inches="tight")
    plt.close(fig)


def plot_f1_comparison(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.4, 4.8))
    labels = ["画素単位", "筆ポリゴン単位"]
    x = np.arange(len(labels))
    width = 0.36
    for i, model in enumerate(TARGET_MODELS):
        vals = []
        for unit in labels:
            vals.append(float(df[(df["unit"] == unit) & (df["model"] == model)]["F1"].iloc[0]))
        ax.bar(x + (i - 0.5) * width, vals, width=width, label=model)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, 0.8)
    ax.set_ylabel("F1スコア")
    ax.set_title("評価単位によるF1スコアの比較")
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    plt.tight_layout()
    fig.savefig(OUT / "図3_評価単位別_F1比較.png", dpi=240, bbox_inches="tight")
    plt.close(fig)


def plot_confusion(df: pd.DataFrame, unit: str, filename: str) -> None:
    sub = df[df["unit"] == unit].copy()
    fig, axes = plt.subplots(1, 2, figsize=(8.6, 3.8))
    for ax, model in zip(axes, TARGET_MODELS):
        row = sub[sub["model"] == model].iloc[0]
        cm = np.array([[row["TN"], row["FP"]], [row["FN"], row["TP"]]], dtype=float)
        im = ax.imshow(cm, cmap="Blues")
        ax.set_title(model)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(["非浸水予測", "浸水予測"])
        ax.set_yticks([0, 1])
        ax.set_yticklabels(["非浸水", "浸水"])
        for i in range(2):
            for j in range(2):
                ax.text(j, i, f"{int(cm[i, j])}", ha="center", va="center", color="black")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    fig.suptitle(f"{unit}の混同行列")
    plt.tight_layout()
    fig.savefig(OUT / filename, dpi=240, bbox_inches="tight")
    plt.close(fig)


def plot_pixel_polygon_concept() -> None:
    grid = np.array(
        [
            [0, 0, 1, 0, 0],
            [0, 1, 1, 0, 0],
            [0, 1, 0, 0, 1],
            [0, 0, 0, 1, 1],
            [0, 0, 0, 0, 1],
        ]
    )
    fig, axes = plt.subplots(1, 2, figsize=(8.2, 4.2))
    axes[0].imshow(grid, cmap="Reds", vmin=0, vmax=1)
    axes[0].set_title("画素単位: 筆内で予測が混在")
    axes[0].set_xticks([])
    axes[0].set_yticks([])
    for i in range(6):
        axes[0].axhline(i - 0.5, color="white", linewidth=1)
        axes[0].axvline(i - 0.5, color="white", linewidth=1)
    axes[1].imshow(np.ones_like(grid), cmap="Reds", vmin=0, vmax=1)
    axes[1].set_title("筆単位: 1画素以上で筆を浸水扱い")
    axes[1].set_xticks([])
    axes[1].set_yticks([])
    for i in range(6):
        axes[1].axhline(i - 0.5, color="white", linewidth=1)
        axes[1].axvline(i - 0.5, color="white", linewidth=1)
    fig.suptitle("画素単位判別から筆ポリゴン単位判別への集約イメージ")
    plt.tight_layout()
    fig.savefig(OUT / "図6_画素単位から筆ポリゴン単位への集約概念図.png", dpi=240, bbox_inches="tight")
    plt.close(fig)


def draft_consistency(df: pd.DataFrame) -> pd.DataFrame:
    draft = [
        ("画素単位", "XGBoost", "F1", 0.581),
        ("画素単位", "ランダムフォレスト", "F1", 0.558),
        ("筆ポリゴン単位", "ランダムフォレスト", "balanced_accuracy", 0.680),
        ("筆ポリゴン単位", "ランダムフォレスト", "recall", 0.727),
        ("筆ポリゴン単位", "ランダムフォレスト", "F1", 0.694),
        ("筆ポリゴン単位", "XGBoost", "balanced_accuracy", 0.680),
        ("筆ポリゴン単位", "XGBoost", "recall", 0.620),
        ("筆ポリゴン単位", "XGBoost", "F1", 0.660),
    ]
    rows = []
    for unit, model, metric, draft_value in draft:
        sub = df[(df["unit"] == unit) & (df["model"] == model)]
        actual = float(sub[metric].iloc[0]) if not sub.empty else np.nan
        rows.append(
            {
                "unit": unit,
                "model": model,
                "metric": metric,
                "draft_value": draft_value,
                "report_value": actual,
                "difference": actual - draft_value if np.isfinite(actual) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def write_report(df: pd.DataFrame, consistency: pd.DataFrame) -> None:
    cols = ["unit", "mode", "model", "threshold", *METRICS, "TP", "FP", "FN", "TN", "best_params"]
    lines = [
        "# 論文用 画素単位・筆ポリゴン単位判別結果整理",
        "",
        "## 使用した評価条件",
        "",
        "- 入力特徴量: 降雨後経過時間帯別の後方散乱強度差分と、その派生特徴量",
        "- 対象: bbox内の田んぼ画素、およびbbox内の田んぼ筆ポリゴン",
        "- 正解: 既存の `map7_inundation_truth_mask.tif`",
        "- 画素単位: positive / negative を10,000画素ずつ抽出",
        "- 筆ポリゴン単位: positive / negative を1,000筆ずつ抽出",
        "- 筆ポリゴン単位の正解ラベル: 筆内に正解浸水画素が1画素以上存在する場合に浸水筆",
        "",
        "## 論文本文に対応する主要結果",
        "",
        fmt_table(df[cols]),
        "",
        "## LaTeX用表",
        "",
        "```latex",
        latex_table(df).strip(),
        "```",
        "",
        "## 本文案の数値との照合",
        "",
        fmt_table(consistency),
        "",
        "## 図表",
        "",
        "- `図1_画素単位_RF_XGB性能比較.png`",
        "- `図2_筆ポリゴン単位_RF_XGB性能比較.png`",
        "- `図3_評価単位別_F1比較.png`",
        "- `図4_画素単位_RF_XGB混同行列.png`",
        "- `図5_筆ポリゴン単位_RF_XGB混同行列.png`",
        "- `図6_画素単位から筆ポリゴン単位への集約概念図.png`",
        "",
        "## 注意",
        "",
        "本文案の数値と既存レポートから再抽出した値には一部差がある。論文に記載する場合は、このフォルダ内の `論文用_主要性能表.csv` の値に統一することを推奨する。",
    ]
    (OUT / "論文用_画素筆ポリゴン判別レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    all_metrics = load_metrics()
    selected = preferred_rows(all_metrics)
    consistency = draft_consistency(selected)

    selected.to_csv(OUT / "論文用_主要性能表.csv", index=False, encoding="utf-8-sig")
    consistency.to_csv(OUT / "論文本文案_数値照合表.csv", index=False, encoding="utf-8-sig")
    (OUT / "論文用_主要性能表.tex").write_text(latex_table(selected), encoding="utf-8-sig")

    plot_model_bars(selected, "画素単位", "図1_画素単位_RF_XGB性能比較.png")
    plot_model_bars(selected, "筆ポリゴン単位", "図2_筆ポリゴン単位_RF_XGB性能比較.png")
    plot_f1_comparison(selected)
    plot_confusion(selected, "画素単位", "図4_画素単位_RF_XGB混同行列.png")
    plot_confusion(selected, "筆ポリゴン単位", "図5_筆ポリゴン単位_RF_XGB混同行列.png")
    plot_pixel_polygon_concept()
    write_report(selected, consistency)

    print(selected[["unit", "mode", "model", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
