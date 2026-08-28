from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "latest_overview"
FIG = OUT / "figures"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")


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


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")


def best_by(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    return (
        df.sort_values(group_cols + ["balanced_accuracy", "F1"], ascending=[True] * len(group_cols) + [False, False])
        .groupby(group_cols, as_index=False)
        .head(1)
        .reset_index(drop=True)
    )


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


def bar_metrics(df: pd.DataFrame, label_col: str, title: str, path: Path, ylim=(0.4, 0.85)) -> None:
    metrics = [("balanced_accuracy", "BA", "#4c78a8"), ("precision", "Precision", "#f58518"), ("recall", "Recall", "#54a24b"), ("ROC_AUC", "AUC", "#e45756")]
    fig, ax = plt.subplots(figsize=(max(9, len(df) * 1.0), 5.2))
    x = np.arange(len(df))
    width = 0.18
    for i, (col, label, color) in enumerate(metrics):
        ax.bar(x + (i - 1.5) * width, df[col], width, label=label, color=color)
    ax.set_xticks(x)
    ax.set_xticklabels(df[label_col].tolist(), rotation=35, ha="right", fontproperties=FONT)
    ax.set_ylim(*ylim)
    ax.set_ylabel("スコア", fontproperties=FONT)
    ax.set_title(title, fontproperties=FONT)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT, ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.23))
    plt.tight_layout()
    plt.savefig(path, dpi=220, bbox_inches="tight")
    plt.close()


def plot_tif_lines(paddy_tif: pd.DataFrame, all_tif: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 5.4))
    ppx = paddy_tif[paddy_tif["unit"] == "pixel"].copy()
    ppoly = paddy_tif[paddy_tif["unit"] == "polygon"].copy()
    all_df = all_tif.copy()
    ax.plot(all_df["tif"], all_df["balanced_accuracy"], marker="o", label="全画素", color="#4c78a8")
    ax.plot(ppx["tif"], ppx["balanced_accuracy"], marker="s", label="田んぼ画素", color="#f58518")
    ax.plot(ppoly["tif"], ppoly["balanced_accuracy"], marker="^", label="田んぼ筆ポリゴン", color="#54a24b")
    ax.set_ylim(0.35, 1.02)
    ax.set_ylabel("Balanced Accuracy", fontproperties=FONT)
    ax.set_title("TIF別の判別精度比較", fontproperties=FONT)
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT)
    plt.tight_layout()
    plt.savefig(FIG / "03_TIF別_BA比較.png", dpi=220, bbox_inches="tight")
    plt.close()


def plot_recall_specificity(paddy_tif: pd.DataFrame, all_tif: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7.2, 6.2))
    for name, df, marker, color in [
        ("全画素", all_tif, "o", "#4c78a8"),
        ("田んぼ画素", paddy_tif[paddy_tif["unit"] == "pixel"], "s", "#f58518"),
        ("田んぼ筆ポリゴン", paddy_tif[paddy_tif["unit"] == "polygon"], "^", "#54a24b"),
    ]:
        ax.scatter(df["recall"], df["specificity"], label=name, marker=marker, s=70, alpha=0.85, color=color)
    ax.set_xlim(0, 1.05)
    ax.set_ylim(0, 1.05)
    ax.set_xlabel("Recall: 浸水を拾う力", fontproperties=FONT)
    ax.set_ylabel("Specificity: 非浸水を除外する力", fontproperties=FONT)
    ax.set_title("TIF別 Recall / Specificity の関係", fontproperties=FONT)
    ax.grid(alpha=0.3)
    ax.legend(prop=FONT)
    plt.tight_layout()
    plt.savefig(FIG / "04_TIF別_Recall_Specificity.png", dpi=220, bbox_inches="tight")
    plt.close()


def plot_effect(effect: pd.DataFrame) -> None:
    top = effect.head(12).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 5.8))
    ax.barh(top["feature"], top["abs_cohens_d"], color="#4c78a8")
    ax.set_xlabel("絶対Cohen's d", fontproperties=FONT)
    ax.set_title("bbox内全画素: 浸水域/非浸水域の特徴量分離度", fontproperties=FONT)
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(FIG / "05_特徴量効果量.png", dpi=220, bbox_inches="tight")
    plt.close()


def plot_counts(counts: pd.DataFrame, title: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 5.4))
    x = np.arange(len(counts))
    width = 0.36
    ax.bar(x - width / 2, counts["available_positive"], width, label="浸水", color="#e45756")
    ax.bar(x + width / 2, counts["available_negative"], width, label="非浸水", color="#4c78a8")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels(counts["label"], rotation=45, ha="right", fontproperties=FONT)
    ax.set_ylabel("対象数（対数）", fontproperties=FONT)
    ax.set_title(title, fontproperties=FONT)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT)
    plt.tight_layout()
    plt.savefig(path, dpi=220, bbox_inches="tight")
    plt.close()


def plot_combined_confusions(rows: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, len(rows), figsize=(5.1 * len(rows), 4.7))
    if len(rows) == 1:
        axes = [axes]
    for ax, (_, row) in zip(axes, rows.iterrows()):
        cm = np.array([[int(row["TP"]), int(row["FN"])], [int(row["FP"]), int(row["TN"])]])
        im = ax.imshow(cm, cmap="Blues")
        labels = [["TP", "FN"], ["FP", "TN"]]
        max_v = max(cm.max(), 1)
        for i in range(2):
            for j in range(2):
                color = "white" if cm[i, j] > max_v * 0.55 else "black"
                ax.text(j, i, f"{labels[i][j]}\n{cm[i, j]:,}", ha="center", va="center", color=color, fontproperties=FONT)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(["浸水予測", "非浸水予測"], fontproperties=FONT)
        ax.set_yticks([0, 1])
        ax.set_yticklabels(["実際に浸水", "実際に非浸水"], fontproperties=FONT)
        ax.set_title(row["label"], fontproperties=FONT)
    fig.colorbar(im, ax=axes, fraction=0.025, pad=0.02)
    plt.tight_layout()
    plt.savefig(FIG / "06_代表ケース_混同行列.png", dpi=220, bbox_inches="tight")
    plt.close()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    FIG.mkdir(parents=True, exist_ok=True)

    comprehensive = read_csv(BASE / "comprehensive_balanced_classification" / "全モデル評価指標.csv")
    bbox_balanced = read_csv(BASE / "bbox_balanced_classification" / "全モデル評価指標.csv")
    bbox_all_20000 = read_csv(BASE / "bbox_all_pixels_20000_feature_diagnostics" / "全モデル評価指標.csv")
    paddy_20000 = read_csv(BASE / "bbox_paddy_pixel_polygon_20000" / "全モデル評価指標.csv")
    per_tif_paddy = read_csv(BASE / "bbox_per_tif_paddy_pixel_polygon" / "TIF別_最良モデル評価.csv")
    per_tif_all = read_csv(BASE / "bbox_per_tif_all_pixels" / "TIF別_最良モデル評価.csv")
    effect = read_csv(BASE / "bbox_all_pixels_20000_feature_diagnostics" / "特徴量効果量.csv")
    counts_paddy = read_csv(BASE / "bbox_paddy_pixel_polygon_20000" / "母数と抽出数.csv")
    counts_tif_paddy = read_csv(BASE / "bbox_per_tif_paddy_pixel_polygon" / "TIF別_母数と抽出数.csv")
    counts_tif_all = read_csv(BASE / "bbox_per_tif_all_pixels" / "TIF別_母数と抽出数.csv")

    comp_best = best_by(comprehensive, ["scenario", "mode"])
    bbox_best = best_by(bbox_balanced, ["scenario", "mode"])
    all_best = best_by(bbox_all_20000, ["mode"])
    paddy_best = best_by(paddy_20000, ["scenario", "mode"])

    # High-level comparison focuses on tuned models where available.
    high_rows = []
    for label, df, scenario_filter in [
        ("全域: 道路+田んぼ画素", comp_best, "道路+田んぼ画素"),
        ("全域: 田んぼ画素", comp_best, "田んぼ画素"),
        ("全域: 田んぼ筆", comp_best, "田んぼ筆ポリゴン"),
        ("bbox: 道路+田んぼ画素", bbox_best, "道路+田んぼ画素_bbox内"),
        ("bbox: 田んぼ画素", bbox_best, "田んぼ画素_bbox内"),
        ("bbox: 田んぼ筆", bbox_best, "田んぼ筆ポリゴン_bbox内"),
        ("bbox全画素 20,000", all_best.assign(scenario="bbox全画素"), "bbox全画素"),
        ("bbox田んぼ画素 20,000", paddy_best, "田んぼ画素"),
        ("bbox田んぼ筆 1,233", paddy_best, "田んぼ筆ポリゴン"),
    ]:
        if "scenario" in df.columns:
            sub = df[df["scenario"] == scenario_filter].copy()
        else:
            sub = df.copy()
        if "mode" in sub.columns:
            gs = sub[sub["mode"].astype(str).str.contains("GridSearch", na=False)]
            sub = gs if not gs.empty else sub
        if not sub.empty:
            row = sub.sort_values("balanced_accuracy", ascending=False).iloc[0].copy()
            row["label"] = label
            high_rows.append(row)
    high = pd.DataFrame(high_rows)
    bar_metrics(high, "label", "主要条件の最良モデル性能", FIG / "01_主要条件_性能比較.png", ylim=(0.45, 0.82))

    paddy_summary = paddy_best[paddy_best["mode"] == "GridSearch"].copy()
    paddy_summary["label"] = paddy_summary["scenario"]
    bar_metrics(paddy_summary, "label", "bbox内田んぼ限定: 画素単位と筆ポリゴン単位", FIG / "02_bbox田んぼ_画素vs筆.png", ylim=(0.45, 0.82))

    plot_tif_lines(per_tif_paddy, per_tif_all)
    plot_recall_specificity(per_tif_paddy, per_tif_all)
    plot_effect(effect)

    counts_tif_all_plot = counts_tif_all.copy()
    counts_tif_all_plot["label"] = counts_tif_all_plot["tif"].str.replace("_inun.tif", "", regex=False)
    plot_counts(counts_tif_all_plot, "TIF別 bbox内全画素の母数", FIG / "07_TIF別_全画素_母数.png")

    counts_tif_paddy_plot = counts_tif_paddy[counts_tif_paddy["unit"] == "polygon"].copy()
    counts_tif_paddy_plot["label"] = counts_tif_paddy_plot["tif"].str.replace("_inun.tif", "", regex=False) + "\n筆"
    plot_counts(counts_tif_paddy_plot, "TIF別 田んぼ筆ポリゴンの母数", FIG / "08_TIF別_田んぼ筆_母数.png")

    confusion_source = [
        ("bbox全画素", all_best[all_best["mode"] == "GridSearch"].sort_values("balanced_accuracy", ascending=False).iloc[0]),
        ("田んぼ画素", paddy_summary[paddy_summary["scenario"] == "田んぼ画素"].sort_values("balanced_accuracy", ascending=False).iloc[0]),
        ("田んぼ筆", paddy_summary[paddy_summary["scenario"] == "田んぼ筆ポリゴン"].sort_values("balanced_accuracy", ascending=False).iloc[0]),
    ]
    confusion_records = []
    for label, row in confusion_source:
        record = row.to_dict()
        record["label"] = label
        confusion_records.append(record)
    confusion_rows = pd.DataFrame(confusion_records)
    plot_combined_confusions(confusion_rows)

    high_out = high[["label", "model", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]].copy()
    high_out.to_csv(OUT / "主要条件_最良性能.csv", index=False, encoding="utf-8-sig")

    tif_compare = per_tif_all[["tif", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]].rename(
        columns={
            "balanced_accuracy": "全画素_BA",
            "precision": "全画素_precision",
            "recall": "全画素_recall",
            "specificity": "全画素_specificity",
            "ROC_AUC": "全画素_AUC",
        }
    )
    ppx = per_tif_paddy[per_tif_paddy["unit"] == "pixel"][
        ["tif", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]
    ].rename(
        columns={
            "balanced_accuracy": "田んぼ画素_BA",
            "precision": "田んぼ画素_precision",
            "recall": "田んぼ画素_recall",
            "specificity": "田んぼ画素_specificity",
            "ROC_AUC": "田んぼ画素_AUC",
        }
    )
    ppoly = per_tif_paddy[per_tif_paddy["unit"] == "polygon"][
        ["tif", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]
    ].rename(
        columns={
            "balanced_accuracy": "田んぼ筆_BA",
            "precision": "田んぼ筆_precision",
            "recall": "田んぼ筆_recall",
            "specificity": "田んぼ筆_specificity",
            "ROC_AUC": "田んぼ筆_AUC",
        }
    )
    tif_compare = tif_compare.merge(ppx, on="tif", how="left").merge(ppoly, on="tif", how="left")
    tif_compare.to_csv(OUT / "TIF別_判定性能比較.csv", index=False, encoding="utf-8-sig")

    report = [
        "# map7 浸水域判別 最新overview",
        "",
        "## 目的",
        "",
        "後方散乱強度差分の時系列特徴を使い、浸水域と非浸水域をどこまで判別できるかを整理しました。今回のoverviewでは、従来の全体評価に加えて、Kurume TIFごとのbbox内評価、田んぼ画素単位、田んぼ筆ポリゴン単位、田んぼ以外を含む全画素評価を含めています。",
        "",
        "## 使用した主なデータ整理",
        "",
        "- Sentinel-1 GRDのBefore/Target差分を、降雨後経過時間帯 `0-3h`, `3-6h`, `6-12h`, `12-24h` に整理しました。",
        "- 正解浸水域は `D:/sotsuron/kurume/*.tif` の `0.5 <= 値 <= 1.7` を用いました。",
        "- 各TIFの有効画素bboxを作成し、bbox内だけの評価も行いました。",
        "- 田んぼについては、画素単位と筆ポリゴン単位の2通りで評価しました。",
        "- 学習・検証では、positive / negative を同数に揃え、7:3分割とGridSearchCVを用いました。",
        "",
        "## 主要条件の最良性能",
        "",
        md_table(high_out),
        "",
        "## bbox内・田んぼ限定の整理",
        "",
        "田んぼ画素単位ではBalanced Accuracyが約0.57に留まりました。一方、筆ポリゴン単位に集約すると約0.69まで改善しました。これは、画素単位のノイズや局所的な後方散乱変動を、筆単位の平均・分散などで緩和できたためと考えられます。",
        "",
        md_table(paddy_summary[["scenario", "mode", "model", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]]),
        "",
        "## TIF別評価",
        "",
        "TIFごとに分けると、場所によって精度のばらつきが大きくなりました。田んぼ筆ポリゴン単位は高い精度を示すTIFもありますが、positive筆数が少ない場合は過大評価の可能性があります。",
        "",
        md_table(tif_compare),
        "",
        "## 誤判定の要因",
        "",
        "- bbox内全画素では、建物・道路・畦畔・粗い地表なども含まれるため、浸水域と似た後方散乱強度差分の時系列を示す非浸水画素が多くなります。",
        "- 特徴量効果量を見ると、浸水域と非浸水域の差は大きくありません。最大でも `diff_3_6h` の Cohen's d は約0.116で、分布の重なりが大きいです。",
        "- そのため、後方散乱強度差分だけで全画素を分類するとFPが増えやすく、田んぼ筆ポリゴンのような地理条件・集約単位を使うほうが安定します。",
        "",
        "## 日本語図",
        "",
        "- `figures/01_主要条件_性能比較.png`",
        "- `figures/02_bbox田んぼ_画素vs筆.png`",
        "- `figures/03_TIF別_BA比較.png`",
        "- `figures/04_TIF別_Recall_Specificity.png`",
        "- `figures/05_特徴量効果量.png`",
        "- `figures/06_代表ケース_混同行列.png`",
        "- `figures/07_TIF別_全画素_母数.png`",
        "- `figures/08_TIF別_田んぼ筆_母数.png`",
        "",
        "## 結論",
        "",
        "後方散乱強度差分のみでも浸水域らしい変化は一定程度検出できますが、画素単位では非浸水域との分布重なりが大きく、誤検出が多くなります。現時点では、田んぼ筆ポリゴン単位に集約した分析が最も安定しており、浸水域抽出の実用的な単位として有望です。ただし、TIFごとにpositive筆数が少ない場合は評価が不安定になるため、複数イベント・複数地域での検証が必要です。",
        "",
    ]
    (OUT / "map7_detection_latest_overview.md").write_text("\n".join(report), encoding="utf-8-sig")

    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
