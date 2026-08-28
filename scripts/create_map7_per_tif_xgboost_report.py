from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
ALL_PIXEL_DIR = BASE / "bbox_per_tif_all_pixels"
PADDY_DIR = BASE / "bbox_per_tif_paddy_pixel_polygon"
OUT = BASE / "bbox_per_tif_xgboost_report"
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")


METRIC_COLS = ["balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]
CM_COLS = ["TP", "FP", "FN", "TN"]


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


def find_csv(folder: Path, stem_contains: str) -> Path:
    matches = sorted(p for p in folder.glob("*.csv") if stem_contains in p.stem)
    if not matches:
        raise FileNotFoundError(f"{folder} に {stem_contains} を含むCSVがありません")
    return matches[0]


def read_xgb_rows() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    all_csv = find_csv(ALL_PIXEL_DIR, "全モデル評価")
    paddy_csv = find_csv(PADDY_DIR, "全モデル評価")

    all_df = pd.read_csv(all_csv, encoding="utf-8-sig")
    paddy_df = pd.read_csv(paddy_csv, encoding="utf-8-sig")

    all_xgb = all_df[all_df["model"].astype(str).str.lower().eq("xgboost")].copy()
    all_xgb.insert(0, "scope", "bbox内全画素")
    if "unit" not in all_xgb.columns:
        all_xgb.insert(1, "unit", "pixel")

    paddy_xgb = paddy_df[paddy_df["model"].astype(str).str.lower().eq("xgboost")].copy()
    paddy_pixel = paddy_xgb[paddy_xgb["unit"].astype(str).eq("pixel")].copy()
    paddy_polygon = paddy_xgb[paddy_xgb["unit"].astype(str).eq("polygon")].copy()
    paddy_pixel.insert(0, "scope", "bbox内田んぼ画素")
    paddy_polygon.insert(0, "scope", "bbox内田んぼ筆ポリゴン")
    return all_xgb, paddy_pixel, paddy_polygon


def format_float_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    return out


def md_table(df: pd.DataFrame) -> str:
    shown = format_float_df(df)
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[col]) for col in shown.columns) + " |")
    return "\n".join(lines)


def plot_metric_lines(datasets: dict[str, pd.DataFrame]) -> None:
    for metric in ["balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]:
        fig, ax = plt.subplots(figsize=(11.5, 5.2))
        for label, df in datasets.items():
            if df.empty:
                continue
            x = np.arange(len(df))
            ax.plot(x, df[metric].astype(float), marker="o", label=label)
            ax.set_xticks(x)
            ax.set_xticklabels(df["tif"], rotation=45, ha="right")
        ax.set_ylim(0, 1.02)
        ax.set_ylabel(metric)
        ax.set_title(f"TIF別 XGBoost {metric}")
        ax.grid(axis="y", alpha=0.3)
        ax.legend()
        plt.tight_layout()
        fig.savefig(OUT / f"図_TIF別_XGBoost_{metric}.png", dpi=220, bbox_inches="tight")
        plt.close(fig)


def plot_confusion_bars(df: pd.DataFrame, label: str, filename: str) -> None:
    if df.empty:
        return
    plot_df = df[["tif", *CM_COLS]].copy()
    for col in CM_COLS:
        plot_df[col] = plot_df[col].astype(float)
    fig, ax = plt.subplots(figsize=(11.5, 5.2))
    bottom = np.zeros(len(plot_df))
    colors = {"TP": "#2ca25f", "FP": "#de2d26", "FN": "#fdae6b", "TN": "#3182bd"}
    for col in CM_COLS:
        vals = plot_df[col].to_numpy()
        ax.bar(plot_df["tif"], vals, bottom=bottom, label=col, color=colors[col])
        bottom += vals
    ax.set_title(f"{label}: TIF別 混同行列内訳")
    ax.set_ylabel("検証画素数 / 検証筆数")
    ax.tick_params(axis="x", rotation=45)
    ax.legend(ncol=4)
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    fig.savefig(OUT / filename, dpi=220, bbox_inches="tight")
    plt.close(fig)


def summary_stats(name: str, df: pd.DataFrame) -> dict:
    row: dict[str, object] = {
        "対象": name,
        "TIF数": int(df["tif"].nunique()) if not df.empty else 0,
    }
    for metric in METRIC_COLS:
        row[f"{metric}_mean"] = float(df[metric].astype(float).mean()) if not df.empty else np.nan
        row[f"{metric}_min"] = float(df[metric].astype(float).min()) if not df.empty else np.nan
        row[f"{metric}_max"] = float(df[metric].astype(float).max()) if not df.empty else np.nan
    return row


def write_report(all_xgb: pd.DataFrame, paddy_pixel: pd.DataFrame, paddy_polygon: pd.DataFrame, summary: pd.DataFrame) -> None:
    cols = ["tif", "threshold", *METRIC_COLS, *CM_COLS, "best_cv_balanced_accuracy", "best_params"]
    paddy_cols = ["tif", "unit", "threshold", *METRIC_COLS, *CM_COLS, "best_cv_balanced_accuracy", "best_params"]

    lines = [
        "# TIF別 XGBoost 精度算出レポート",
        "",
        "## 条件",
        "",
        "- 既存のTIF別評価で作成済みの `TIF別_全モデル評価.csv` から、XGBoostの行のみを抽出した。",
        "- 各TIFで positive / negative は同数に揃えた評価結果を使用した。",
        "- 閾値は検証データ上でBalanced Accuracyが最大になる値を採用した。",
        "- 標高データは使用せず、衛星データ由来の特徴量のみを対象にした。",
        "",
        "## 平均・最小・最大",
        "",
        md_table(summary),
        "",
        "## bbox内全画素",
        "",
        md_table(all_xgb[[c for c in cols if c in all_xgb.columns]]),
        "",
        "## bbox内田んぼ画素",
        "",
        md_table(paddy_pixel[[c for c in paddy_cols if c in paddy_pixel.columns]]),
        "",
        "## bbox内田んぼ筆ポリゴン",
        "",
        md_table(paddy_polygon[[c for c in paddy_cols if c in paddy_polygon.columns]]),
        "",
        "## 図",
        "",
        "- `図_TIF別_XGBoost_balanced_accuracy.png`",
        "- `図_TIF別_XGBoost_precision.png`",
        "- `図_TIF別_XGBoost_recall.png`",
        "- `図_TIF別_XGBoost_specificity.png`",
        "- `図_TIF別_XGBoost_ROC_AUC.png`",
        "- `図_全画素_XGBoost_混同行列内訳.png`",
        "- `図_田んぼ画素_XGBoost_混同行列内訳.png`",
        "- `図_田んぼ筆_XGBoost_混同行列内訳.png`",
        "",
        "## 読み取り",
        "",
        "- 全画素ではTIFごとの差が大きく、Kurume03・Kurume06・Kurume12は比較的Balanced Accuracyが高い。",
        "- 田んぼ画素に限定すると、Kurume01・Kurume03・Kurume06などで全画素より分離が改善している。",
        "- 田んぼ筆ポリゴン単位では、画素単位よりBalanced Accuracyが高いTIFがあり、筆内の集約により局所ノイズが平均化された可能性がある。",
    ]
    (OUT / "TIF別_XGBoost_精度算出レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    all_xgb, paddy_pixel, paddy_polygon = read_xgb_rows()

    all_xgb.to_csv(OUT / "TIF別_XGBoost_全画素評価.csv", index=False, encoding="utf-8-sig")
    paddy_pixel.to_csv(OUT / "TIF別_XGBoost_田んぼ画素評価.csv", index=False, encoding="utf-8-sig")
    paddy_polygon.to_csv(OUT / "TIF別_XGBoost_田んぼ筆ポリゴン評価.csv", index=False, encoding="utf-8-sig")

    summary = pd.DataFrame(
        [
            summary_stats("bbox内全画素", all_xgb),
            summary_stats("bbox内田んぼ画素", paddy_pixel),
            summary_stats("bbox内田んぼ筆ポリゴン", paddy_polygon),
        ]
    )
    summary.to_csv(OUT / "TIF別_XGBoost_要約統計.csv", index=False, encoding="utf-8-sig")

    plot_metric_lines(
        {
            "全画素": all_xgb.sort_values("tif"),
            "田んぼ画素": paddy_pixel.sort_values("tif"),
            "田んぼ筆": paddy_polygon.sort_values("tif"),
        }
    )
    plot_confusion_bars(all_xgb.sort_values("tif"), "全画素", "図_全画素_XGBoost_混同行列内訳.png")
    plot_confusion_bars(paddy_pixel.sort_values("tif"), "田んぼ画素", "図_田んぼ画素_XGBoost_混同行列内訳.png")
    plot_confusion_bars(paddy_polygon.sort_values("tif"), "田んぼ筆", "図_田んぼ筆_XGBoost_混同行列内訳.png")
    write_report(all_xgb.sort_values("tif"), paddy_pixel.sort_values("tif"), paddy_polygon.sort_values("tif"), summary)
    print(summary.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
