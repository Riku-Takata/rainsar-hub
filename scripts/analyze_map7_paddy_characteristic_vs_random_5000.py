from __future__ import annotations

import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
CHAR_DIR = BASE / "paddy_characteristic_5000_model_report"
RAND_DIR = BASE / "paddy_random_5000_model_report"
OUT_DIR = BASE / "paddy_5000_sampling_comparison"

CHAR_PIXELS = CHAR_DIR / "kurume_paddy_characteristic_5000_pixels.csv"
RAND_PIXELS = RAND_DIR / "kurume_paddy_random_5000_pixels.csv"

LABEL_COL = "正解浸水域"
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

JA_FEATURE = {
    "diff_0_3h": "0-3h差分",
    "diff_3_6h": "3-6h差分",
    "diff_6_12h": "6-12h差分",
    "diff_12_24h": "12-24h差分",
    "early_mean_0_6h": "早期平均(0-6h)",
    "late_mean_6_24h": "後期平均(6-24h)",
    "early_minus_late": "早期-後期",
    "drop_0_3_to_3_6": "0-3hから3-6hの低下",
    "drop_3_6_to_6_12": "3-6hから6-12hの低下",
    "recovery_6_12_to_12_24": "6-12hから12-24hの回復",
    "drop_0_3_to_6_12": "0-3hから6-12hの低下",
    "change_0_3_to_6_12": "0-3hから6-12hの変化",
    "profile_mean": "時系列平均",
    "profile_std": "時系列標準偏差",
    "profile_range": "時系列レンジ",
    "negative_bin_count": "負差分の時間帯数",
    "monotonic_drop_score": "単調低下スコア",
}


def cohen_d(pos: pd.Series, neg: pd.Series) -> float:
    pos = pos.dropna().to_numpy(dtype=float)
    neg = neg.dropna().to_numpy(dtype=float)
    if len(pos) < 2 or len(neg) < 2:
        return math.nan
    pooled = math.sqrt(((len(pos) - 1) * pos.var(ddof=1) + (len(neg) - 1) * neg.var(ddof=1)) / (len(pos) + len(neg) - 2))
    if pooled == 0:
        return math.nan
    return float((pos.mean() - neg.mean()) / pooled)


def overlap_coef(pos: pd.Series, neg: pd.Series, bins: int = 120) -> float:
    values = pd.concat([pos, neg]).dropna().to_numpy(dtype=float)
    if len(values) == 0:
        return math.nan
    lo, hi = np.nanpercentile(values, [1, 99])
    if not np.isfinite(lo) or not np.isfinite(hi) or lo == hi:
        return math.nan
    hist_pos, edges = np.histogram(pos.dropna(), bins=bins, range=(lo, hi), density=True)
    hist_neg, _ = np.histogram(neg.dropna(), bins=edges, density=True)
    width = edges[1] - edges[0]
    return float(np.minimum(hist_pos, hist_neg).sum() * width)


def summarize(df: pd.DataFrame, source: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    effect_rows = []
    for feature in FEATURES:
        if feature not in df.columns:
            continue
        for label, zone in [(1, "inundated_truth"), (0, "non_inundated")]:
            s = df.loc[df[LABEL_COL] == label, feature].dropna()
            rows.append(
                {
                    "sample": source,
                    "zone": zone,
                    "feature": feature,
                    "feature_jp": JA_FEATURE.get(feature, feature),
                    "count": len(s),
                    "mean": s.mean(),
                    "std": s.std(),
                    "p05": s.quantile(0.05),
                    "p25": s.quantile(0.25),
                    "median": s.median(),
                    "p75": s.quantile(0.75),
                    "p95": s.quantile(0.95),
                }
            )
        pos = df.loc[df[LABEL_COL] == 1, feature]
        neg = df.loc[df[LABEL_COL] == 0, feature]
        effect_rows.append(
            {
                "sample": source,
                "feature": feature,
                "feature_jp": JA_FEATURE.get(feature, feature),
                "inundated_mean": pos.mean(),
                "non_inundated_mean": neg.mean(),
                "mean_difference": pos.mean() - neg.mean(),
                "cohen_d": cohen_d(pos, neg),
                "abs_cohen_d": abs(cohen_d(pos, neg)),
                "distribution_overlap": overlap_coef(pos, neg),
            }
        )
    return pd.DataFrame(rows), pd.DataFrame(effect_rows)


def plot_profile(char: pd.DataFrame, rand: pd.DataFrame) -> None:
    time_features = ["diff_0_3h", "diff_3_6h", "diff_6_12h", "diff_12_24h"]
    x = np.arange(len(time_features))
    labels = ["0-3h", "3-6h", "6-12h", "12-24h"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4), sharey=True)
    for ax, df, title in zip(axes, [char, rand], ["Characteristic 5000", "Random 5000"]):
        for label, name, color in [(1, "truth inundated", "#d62728"), (0, "non-inundated", "#1f77b4")]:
            means = [df.loc[df[LABEL_COL] == label, f].mean() for f in time_features]
            q25 = [df.loc[df[LABEL_COL] == label, f].quantile(0.25) for f in time_features]
            q75 = [df.loc[df[LABEL_COL] == label, f].quantile(0.75) for f in time_features]
            ax.plot(x, means, marker="o", label=name, color=color)
            ax.fill_between(x, q25, q75, alpha=0.18, color=color)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_title(title)
        ax.set_ylabel("backscatter difference")
        ax.grid(alpha=0.3)
    axes[1].legend(loc="best")
    fig.tight_layout()
    fig.savefig(OUT_DIR / "profile_mean_iqr_comparison.png", dpi=180)
    plt.close(fig)


def plot_effects(effect: pd.DataFrame) -> None:
    top_features = (
        effect.pivot(index="feature", columns="sample", values="abs_cohen_d")
        .fillna(0)
        .assign(max_d=lambda d: d.max(axis=1))
        .sort_values("max_d", ascending=False)
        .head(10)
        .drop(columns="max_d")
    )
    ax = top_features.plot(kind="barh", figsize=(9, 6))
    ax.invert_yaxis()
    ax.set_xlabel("|Cohen's d| between truth and non-truth")
    ax.set_ylabel("")
    ax.grid(axis="x", alpha=0.3)
    fig = ax.get_figure()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "effect_size_top10_comparison.png", dpi=180)
    plt.close(fig)


def write_report(summary: pd.DataFrame, effect: pd.DataFrame) -> None:
    top_char = effect[effect["sample"] == "characteristic_5000"].sort_values("abs_cohen_d", ascending=False).head(8)
    top_rand = effect[effect["sample"] == "random_5000"].sort_values("abs_cohen_d", ascending=False).head(8)

    def md_table(df: pd.DataFrame, cols: list[str]) -> str:
        out = df[cols].copy()
        for c in out.select_dtypes(include=[float]).columns:
            out[c] = out[c].map(lambda v: f"{v:.3f}")
        headers = list(out.columns)
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        for _, row in out.iterrows():
            lines.append("| " + " | ".join(str(row[h]) for h in headers) + " |")
        return "\n".join(lines)

    lines = [
        "# 田んぼ内 5000画素抽出方法の比較",
        "",
        "## 目的",
        "",
        "特徴的5000画素とランダム5000画素で精度差が出た原因を、特徴量分布とクラス間分離の観点から比較した。",
        "",
        "## クラス間分離が大きい特徴量",
        "",
        "### 特徴的5000画素",
        "",
        md_table(
            top_char,
            ["feature_jp", "inundated_mean", "non_inundated_mean", "mean_difference", "cohen_d", "distribution_overlap"],
        ),
        "",
        "### ランダム5000画素",
        "",
        md_table(
            top_rand,
            ["feature_jp", "inundated_mean", "non_inundated_mean", "mean_difference", "cohen_d", "distribution_overlap"],
        ),
        "",
        "## 明らかな特徴",
        "",
        "- 特徴的5000画素では、正解浸水域の `3-6h差分` が非浸水域より高く、`6-12h差分` が低い。そのため `3-6hから6-12hの低下` と `0-3hから6-12hの低下` が大きくなる。",
        "- 特徴的5000画素では、正解浸水域と非浸水域の時系列プロファイルが人工的に見えやすい形で分かれている。",
        "- ランダム5000画素では、同じ特徴量でも分布の重なりが大きく、クラス間の平均差が小さい。そのためモデル精度は約60%に落ちる。",
        "- 精度差の主因は、モデルの種類ではなく、抽出された画素集合の分布差である。",
        "",
        "## 出力図",
        "",
        "- `profile_mean_iqr_comparison.png`: 時間帯別の平均差分と四分位範囲",
        "- `effect_size_top10_comparison.png`: クラス間効果量の比較",
        "",
    ]
    (OUT_DIR / "sampling_comparison_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    char = pd.read_csv(CHAR_PIXELS)
    rand = pd.read_csv(RAND_PIXELS)
    char_summary, char_effect = summarize(char, "characteristic_5000")
    rand_summary, rand_effect = summarize(rand, "random_5000")
    summary = pd.concat([char_summary, rand_summary], ignore_index=True)
    effect = pd.concat([char_effect, rand_effect], ignore_index=True)
    summary.to_csv(OUT_DIR / "feature_distribution_summary.csv", index=False, encoding="utf-8-sig")
    effect.to_csv(OUT_DIR / "feature_separation_effects.csv", index=False, encoding="utf-8-sig")
    plot_profile(char, rand)
    plot_effects(effect)
    write_report(summary, effect)
    print(f"Wrote {OUT_DIR}")


if __name__ == "__main__":
    main()
