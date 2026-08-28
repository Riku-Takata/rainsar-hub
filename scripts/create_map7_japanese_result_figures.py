from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "japanese_figures"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")


def setup_font() -> font_manager.FontProperties:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    else:
        prop = font_manager.FontProperties()
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.dpi"] = 120
    return prop


FONT = setup_font()


def jp(text: str) -> dict:
    return {"fontproperties": FONT}


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")


def savefig(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=200, bbox_inches="tight")
    plt.close()


def best_row(df: pd.DataFrame) -> pd.Series:
    return df.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0]


def plot_confusion_matrix(row: dict | pd.Series, title: str, path: Path) -> None:
    cm = np.array([[int(row["TP"]), int(row["FN"])], [int(row["FP"]), int(row["TN"])]])
    fig, ax = plt.subplots(figsize=(5.2, 4.4))
    im = ax.imshow(cm, cmap="Blues")
    labels = [["TP\n正解浸水を検出", "FN\n正解浸水を未検出"], ["FP\n非浸水を誤検出", "TN\n非浸水を非検出"]]
    max_v = cm.max() if cm.size else 1
    for i in range(2):
        for j in range(2):
            color = "white" if cm[i, j] > max_v * 0.55 else "black"
            ax.text(j, i, f"{labels[i][j]}\n{cm[i, j]:,}", ha="center", va="center", color=color, fontsize=11, fontproperties=FONT)
    ax.set_xticks([0, 1])
    ax.set_xticklabels(["浸水と予測", "非浸水と予測"], fontproperties=FONT)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["実際に浸水", "実際に非浸水"], fontproperties=FONT)
    ax.set_title(title, fontproperties=FONT, fontsize=13)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    savefig(path)


def plot_initial_detection() -> tuple[Path, dict]:
    summary = read_csv(BASE / "map7_detection_summary.csv").iloc[0]
    total = int(summary["評価範囲_有効画素数"])
    truth = int(summary["正解浸水域画素数"])
    tp = int(summary["検出された正解浸水域画素数"])
    detected = int(summary["検出画素総数"])
    fp = detected - tp
    fn = truth - tp
    tn = total - truth - fp
    row = {"TP": tp, "FP": fp, "FN": fn, "TN": tn}
    path = OUT / "01_初期判定_混同行列.png"
    plot_confusion_matrix(row, "初期判定の混同行列（画素単位）", path)

    fig, ax = plt.subplots(figsize=(7, 4))
    vals = [
        float(summary["浸水域検出率_percent"]),
        float(summary["検出画素中の正解割合_percent"]),
        float(summary["検出画素面積率_percent"]),
    ]
    names = ["正解浸水域の検出率", "検出画素中の正解割合", "検出画素の面積率"]
    bars = ax.bar(names, vals, color=["#4c78a8", "#f58518", "#54a24b"])
    ax.set_ylabel("割合（%）", fontproperties=FONT)
    ax.set_title("初期判定の概要", fontproperties=FONT)
    ax.set_ylim(0, max(vals) * 1.35)
    ax.grid(axis="y", alpha=0.3)
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{val:.1f}%", ha="center", va="bottom", fontproperties=FONT)
    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_fontproperties(FONT)
    savefig(OUT / "02_初期判定_指標.png")
    return path, row


def collect_model_summary() -> pd.DataFrame:
    rows = []

    grid10 = best_row(read_csv(BASE / "paddy_model_gridsearch_report/gridsearch_test_metrics.csv"))
    rows.append({"分析": "画素単位\nランダム1万/1万", **grid10.to_dict()})

    random5 = best_row(read_csv(BASE / "paddy_random_5000_model_report/kurume_paddy_random_5000_test_metrics.csv"))
    rows.append({"分析": "画素単位\nランダム5千/5千", **random5.to_dict()})

    char5 = best_row(read_csv(BASE / "paddy_characteristic_5000_model_report/kurume_paddy_characteristic_5000_test_metrics.csv"))
    rows.append({"分析": "画素単位\n特徴的5千/5千", **char5.to_dict()})

    applied = best_row(read_csv(BASE / "paddy_characteristic_5000_model_report/applied_to_all_paddy_pixels/balanced_paddy_application_metrics.csv"))
    rows.append({"分析": "特徴的モデル\n全田んぼ適用", **applied.to_dict()})

    poly = best_row(read_csv(BASE / "paddy_polygon_1000_classification/map7_paddy_polygon_1000_model_metrics.csv"))
    rows.append({"分析": "筆単位\n1千/1千", **poly.to_dict()})

    tif = best_row(read_csv(BASE / "paddy_all_tif_value_truth/truth_definition_random5000_model_metrics.csv"))
    rows.append({"分析": "TIF>0ラベル\n画素5千/5千", **tif.to_dict()})

    out = pd.DataFrame(rows)
    for col in ["balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def plot_model_summary(summary: pd.DataFrame) -> None:
    metrics = ["balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]
    labels = ["Balanced Acc.", "Precision", "Recall", "Specificity", "AUC"]
    fig, ax = plt.subplots(figsize=(11, 5.5))
    x = np.arange(len(summary))
    width = 0.15
    colors = ["#4c78a8", "#f58518", "#54a24b", "#e45756", "#72b7b2"]
    for i, (metric, label, color) in enumerate(zip(metrics, labels, colors)):
        ax.bar(x + (i - 2) * width, summary[metric], width, label=label, color=color)
    ax.set_xticks(x)
    ax.set_xticklabels(summary["分析"], fontproperties=FONT)
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("スコア", fontproperties=FONT)
    ax.set_title("分析単位・抽出方法別のモデル性能", fontproperties=FONT, fontsize=14)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(prop=FONT, ncol=5, loc="upper center", bbox_to_anchor=(0.5, -0.16))
    for tick in ax.get_yticklabels():
        tick.set_fontproperties(FONT)
    savefig(OUT / "03_分析別_モデル性能比較.png")


def plot_selected_confusions(summary: pd.DataFrame) -> None:
    targets = [
        ("画素単位\nランダム5千/5千", "04_混同行列_画素ランダム5000.png"),
        ("画素単位\n特徴的5千/5千", "05_混同行列_特徴的5000.png"),
        ("特徴的モデル\n全田んぼ適用", "06_混同行列_特徴的モデル全田んぼ適用.png"),
        ("筆単位\n1千/1千", "07_混同行列_筆単位1000.png"),
    ]
    for name, filename in targets:
        row = summary[summary["分析"] == name].iloc[0]
        plot_confusion_matrix(row, name.replace("\n", " ") + " の混同行列", OUT / filename)


def plot_sampling_comparison() -> None:
    effects = read_csv(BASE / "paddy_5000_sampling_comparison/feature_separation_effects.csv")
    pick = effects[effects["feature"].isin(["profile_std", "profile_range", "early_minus_late", "drop_3_6_to_6_12", "diff_6_12h", "late_mean_6_24h"])].copy()
    jp_map = {
        "profile_std": "時系列標準偏差",
        "profile_range": "時系列レンジ",
        "early_minus_late": "早期-後期",
        "drop_3_6_to_6_12": "3-6h→6-12h低下",
        "diff_6_12h": "6-12h差分",
        "late_mean_6_24h": "後期平均",
    }
    pick["特徴量"] = pick["feature"].map(jp_map)
    pick["抽出方法"] = pick["sample"].map({"characteristic_5000": "特徴的5000", "random_5000": "ランダム5000"})
    pivot = pick.pivot(index="特徴量", columns="抽出方法", values="abs_cohen_d").loc[list(jp_map.values())]
    ax = pivot.plot(kind="barh", figsize=(8, 5), color=["#e45756", "#4c78a8"])
    ax.invert_yaxis()
    ax.set_xlabel("効果量 |Cohen's d|", fontproperties=FONT)
    ax.set_ylabel("")
    ax.set_title("特徴的5000とランダム5000の分離度比較", fontproperties=FONT)
    ax.grid(axis="x", alpha=0.3)
    ax.legend(prop=FONT)
    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_fontproperties(FONT)
    savefig(OUT / "08_特徴的5000とランダム5000_特徴量分離.png")


def plot_core_coverage() -> None:
    cov = read_csv(BASE / "paddy_characteristic_core_coverage/characteristic_core_coverage_summary.csv")
    cov = cov[cov["candidate_rule"] == "distance_and_closer_than_non_inundated_core"].copy()
    cov["基準"] = cov["threshold_name"].str.extract(r"(p\d+|max)").iloc[:, 0].replace({"max": "最大"})
    fig, ax1 = plt.subplots(figsize=(8, 4.8))
    x = np.arange(len(cov))
    bars = ax1.bar(x, cov["candidate_pixels"], color="#4c78a8", alpha=0.75, label="候補画素数")
    ax1.set_ylabel("候補画素数", fontproperties=FONT)
    ax1.set_xticks(x)
    ax1.set_xticklabels(cov["基準"], fontproperties=FONT)
    ax1.grid(axis="y", alpha=0.25)
    ax2 = ax1.twinx()
    ax2.plot(x, cov["truth_recall_percent"], marker="o", color="#e45756", label="正解浸水域のカバー率")
    ax2.plot(x, cov["candidate_precision_percent"], marker="s", color="#54a24b", label="候補中の正解率")
    ax2.set_ylabel("割合（%）", fontproperties=FONT)
    ax1.set_title("特徴的浸水パターンに近い画素の存在量", fontproperties=FONT)
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, prop=FONT, loc="upper left")
    for bar in bars:
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{int(bar.get_height()):,}", ha="center", va="bottom", fontsize=8, fontproperties=FONT)
    for tick in ax1.get_yticklabels() + ax2.get_yticklabels():
        tick.set_fontproperties(FONT)
    savefig(OUT / "09_特徴的パターン近傍画素_存在量.png")


def plot_label_definition() -> None:
    counts = read_csv(BASE / "paddy_all_tif_value_truth/truth_definition_paddy_pixel_counts.csv")
    metrics = read_csv(BASE / "paddy_all_tif_value_truth/truth_definition_random5000_model_metrics.csv")
    best = metrics.sort_values(["label_definition", "balanced_accuracy"], ascending=[True, False]).groupby("label_definition").head(1)
    label_map = {"truth_0p5_1p7": "TIF 0.5-1.7", "truth_all_tif_positive": "TIF値 > 0"}
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    c = counts[counts["label_definition"].isin(label_map)].copy()
    c["label"] = c["label_definition"].map(label_map)
    axes[0].bar(c["label"], c["inundated_paddy_pixels"], color="#4c78a8")
    axes[0].set_title("正解浸水域の田んぼ画素数", fontproperties=FONT)
    axes[0].set_ylabel("画素数", fontproperties=FONT)
    axes[0].grid(axis="y", alpha=0.3)
    for i, v in enumerate(c["inundated_paddy_pixels"]):
        axes[0].text(i, v, f"{int(v):,}", ha="center", va="bottom", fontproperties=FONT)
    b = best.copy()
    b["label"] = b["label_definition"].map(label_map)
    x = np.arange(len(b))
    axes[1].bar(x - 0.18, b["balanced_accuracy"], 0.36, label="Balanced Acc.", color="#f58518")
    axes[1].bar(x + 0.18, b["ROC_AUC"], 0.36, label="AUC", color="#54a24b")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(b["label"], fontproperties=FONT)
    axes[1].set_ylim(0.5, 0.7)
    axes[1].set_title("ラベル定義別の簡易モデル性能", fontproperties=FONT)
    axes[1].grid(axis="y", alpha=0.3)
    axes[1].legend(prop=FONT)
    for ax in axes:
        for tick in ax.get_xticklabels() + ax.get_yticklabels():
            tick.set_fontproperties(FONT)
    savefig(OUT / "10_TIFラベル定義比較.png")


def plot_polygon_thresholds() -> None:
    metrics = read_csv(BASE / "paddy_polygon_inundation_ratio_thresholds/model_metrics_by_inundation_ratio_threshold.csv")
    best = metrics.sort_values(["inundation_ratio_threshold", "balanced_accuracy"], ascending=[True, False]).groupby("inundation_ratio_threshold").head(1)
    fig, ax = plt.subplots(figsize=(8, 4.8))
    x = best["inundation_ratio_threshold"] * 100
    ax.plot(x, best["balanced_accuracy"], marker="o", label="Balanced Acc.", color="#4c78a8")
    ax.plot(x, best["precision"], marker="s", label="Precision", color="#f58518")
    ax.plot(x, best["recall"], marker="^", label="Recall", color="#54a24b")
    ax.plot(x, best["ROC_AUC"], marker="D", label="AUC", color="#e45756")
    ax.set_xlabel("positive判定の浸水率閾値（%）", fontproperties=FONT)
    ax.set_ylabel("スコア", fontproperties=FONT)
    ax.set_title("筆単位: 浸水率閾値別の分類性能", fontproperties=FONT)
    ax.set_ylim(0.5, 0.85)
    ax.grid(alpha=0.3)
    ax.legend(prop=FONT, ncol=2)
    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_fontproperties(FONT)
    savefig(OUT / "11_筆単位_浸水率閾値別性能.png")

    # Best threshold confusion matrix, prefer 30% because it has high AUC and BA.
    selected = best.loc[(best["inundation_ratio_threshold"] - 0.30).abs().idxmin()]
    plot_confusion_matrix(selected, "筆単位 30%以上浸水筆の混同行列", OUT / "12_混同行列_筆単位_浸水率30percent.png")


def plot_polygon_features() -> None:
    sep = read_csv(BASE / "paddy_polygon_inundation_ratio_thresholds/feature_separation_by_inundation_ratio_threshold.csv")
    sep = sep[sep["inundation_ratio_threshold"].isin([0.0, 0.1, 0.3])].copy()
    features = ["early_minus_late", "drop_3_6_to_6_12", "late_mean_6_24h", "diff_6_12h_mean", "diff_3_6h_mean"]
    jp_map = {
        "early_minus_late": "早期-後期",
        "drop_3_6_to_6_12": "3-6h→6-12h低下",
        "late_mean_6_24h": "後期平均",
        "diff_6_12h_mean": "6-12h平均差分",
        "diff_3_6h_mean": "3-6h平均差分",
    }
    sub = sep[sep["feature"].isin(features)].copy()
    sub["特徴量"] = sub["feature"].map(jp_map)
    sub["閾値"] = (sub["inundation_ratio_threshold"] * 100).map(lambda v: f"{v:.0f}%")
    pivot = sub.pivot(index="特徴量", columns="閾値", values="abs_cohen_d").loc[list(jp_map.values())]
    ax = pivot.plot(kind="barh", figsize=(8, 5))
    ax.invert_yaxis()
    ax.set_xlabel("効果量 |Cohen's d|", fontproperties=FONT)
    ax.set_ylabel("")
    ax.set_title("筆単位: 浸水率閾値別の特徴量分離", fontproperties=FONT)
    ax.grid(axis="x", alpha=0.3)
    ax.legend(title="浸水率閾値", prop=FONT)
    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_fontproperties(FONT)
    savefig(OUT / "13_筆単位_特徴量分離.png")


def write_index(summary: pd.DataFrame) -> None:
    files = sorted(p.name for p in OUT.glob("*.png"))
    lines = [
        "# 日本語図表まとめ",
        "",
        "map7 / Kurume 浸水域判別分析の主要結果を日本語図として整理したものです。",
        "",
        "## 作成図",
        "",
    ]
    for f in files:
        lines.append(f"- `{f}`")
    lines += [
        "",
        "## 分析別の代表性能",
        "",
        "| 分析 | Balanced Accuracy | Precision | Recall | Specificity | AUC |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for _, r in summary.iterrows():
        lines.append(
            f"| {str(r['分析']).replace(chr(10), ' ')} | {r['balanced_accuracy']:.3f} | {r['precision']:.3f} | {r['recall']:.3f} | {r['specificity']:.3f} | {r['ROC_AUC']:.3f} |"
        )
    (OUT / "README.md").write_text("\n".join(lines), encoding="utf-8")
    summary.to_csv(OUT / "figure_model_summary.csv", index=False, encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    plot_initial_detection()
    summary = collect_model_summary()
    plot_model_summary(summary)
    plot_selected_confusions(summary)
    plot_sampling_comparison()
    plot_core_coverage()
    plot_label_definition()
    plot_polygon_thresholds()
    plot_polygon_features()
    write_index(summary)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
