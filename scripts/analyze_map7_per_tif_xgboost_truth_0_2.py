from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
OUT = BASE / "bbox_per_tif_xgboost_truth_0_2"
TRUTH_MIN = 0.0
TRUTH_MAX = 2.0
SEED = 42
FONT_PATH = Path(r"C:\Windows\Fonts\NotoSansJP-VF.ttf")


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ALL_MOD = load_module(ROOT / "scripts/analyze_map7_per_kurume_tif_all_pixels.py", "per_tif_all_pixels")
PADDY_MOD = load_module(ROOT / "scripts/analyze_map7_per_kurume_tif_paddy_pixel_polygon.py", "per_tif_paddy")


def setup_font() -> None:
    if FONT_PATH.exists():
        font_manager.fontManager.addfont(str(FONT_PATH))
        prop = font_manager.FontProperties(fname=str(FONT_PATH))
        plt.rcParams["font.family"] = prop.get_name()
    plt.rcParams["axes.unicode_minus"] = False


def xgb_spec() -> dict:
    return {
        "XGBoost": (
            XGBClassifier(objective="binary:logistic", eval_metric="logloss", random_state=SEED, n_jobs=1),
            {
                "n_estimators": [120],
                "max_depth": [2, 3],
                "learning_rate": [0.03, 0.05],
                "min_child_weight": [1, 3],
                "subsample": [0.85],
                "colsample_bytree": [0.85],
            },
        )
    }


ALL_MOD.specs = xgb_spec
PADDY_MOD.specs_grid = xgb_spec


def truth_mask_0_2(tif_path: Path) -> np.ndarray:
    profile = ALL_MOD.template_profile()
    dst = np.zeros((profile["height"], profile["width"]), dtype=np.uint8)
    with rasterio.open(tif_path) as src:
        arr = src.read(1).astype(np.float32)
        valid = np.isfinite(arr)
        if src.nodata is not None:
            valid &= arr != src.nodata
        truth = ((arr >= TRUTH_MIN) & (arr <= TRUTH_MAX) & valid).astype(np.uint8)
        reproject(
            source=truth,
            destination=dst,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=profile["transform"],
            dst_crs=profile["crs"],
            resampling=Resampling.nearest,
            src_nodata=0,
            dst_nodata=0,
        )
    return dst > 0


def evaluate_all_pixels(diffs: dict[str, np.ndarray]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    counts = []
    for feature in ALL_MOD.bbox_features():
        tif_name = feature["properties"]["tif"]
        tif_path = ALL_MOD.KURUME_DIR / tif_name
        mask = ALL_MOD.bbox_mask(feature)
        truth = truth_mask_0_2(tif_path)
        df = ALL_MOD.build_frame(tif_name, mask, truth, diffs)
        valid = df.dropna(subset=ALL_MOD.FEATURES + [ALL_MOD.LABEL_COL])
        sample = ALL_MOD.balanced_sample(valid)
        counts.append(
            {
                "tif": tif_name,
                "scope": "bbox内全画素",
                "available_positive": int((valid[ALL_MOD.LABEL_COL] == 1).sum()),
                "available_negative": int((valid[ALL_MOD.LABEL_COL] == 0).sum()),
                "sampled_positive": int((sample[ALL_MOD.LABEL_COL] == 1).sum()) if not sample.empty else 0,
                "sampled_negative": int((sample[ALL_MOD.LABEL_COL] == 0).sum()) if not sample.empty else 0,
                "feature_count": len(ALL_MOD.FEATURES),
            }
        )
        metrics, _, _, _ = ALL_MOD.evaluate(valid, tif_name)
        if not metrics.empty:
            row = metrics.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
            row["scope"] = "bbox内全画素"
            row["unit"] = "pixel"
            rows.append(row)
        print(f"all pixels: {tif_name}")
    return pd.DataFrame(rows), pd.DataFrame(counts)


def evaluate_paddy(diffs: dict[str, np.ndarray]) -> tuple[pd.DataFrame, pd.DataFrame]:
    profile = PADDY_MOD.template_profile()
    shape = (profile["height"], profile["width"])
    paddy_features = PADDY_MOD.load_features(PADDY_MOD.PADDY_GEOJSON)
    polygon_ids = PADDY_MOD.rasterize_polygon_ids(paddy_features, shape)

    rows = []
    counts = []
    for feature in PADDY_MOD.load_bbox_features():
        tif_name = feature["properties"]["tif"]
        tif_path = PADDY_MOD.KURUME_DIR / tif_name
        bbox = PADDY_MOD.bbox_mask_for_feature(feature)
        truth = truth_mask_0_2(tif_path)
        pixel_df = PADDY_MOD.build_pixel_frame(tif_name, bbox, truth, diffs)
        polygon_df = PADDY_MOD.build_polygon_frame(tif_name, bbox, truth, diffs, polygon_ids, paddy_features)
        for unit, scope, df, features, n_limit in [
            ("pixel", "bbox内田んぼ画素", pixel_df, PADDY_MOD.PIXEL_FEATURES, PADDY_MOD.N_PER_CLASS_PIXEL),
            ("polygon", "bbox内田んぼ筆ポリゴン", polygon_df, PADDY_MOD.POLYGON_FEATURES, PADDY_MOD.N_PER_CLASS_POLYGON),
        ]:
            valid = df.dropna(subset=features + [PADDY_MOD.LABEL_COL]).copy()
            sample = PADDY_MOD.balanced_sample(valid, features, n_limit)
            counts.append(
                {
                    "tif": tif_name,
                    "scope": scope,
                    "available_positive": int((valid[PADDY_MOD.LABEL_COL] == 1).sum()),
                    "available_negative": int((valid[PADDY_MOD.LABEL_COL] == 0).sum()),
                    "sampled_positive": int((sample[PADDY_MOD.LABEL_COL] == 1).sum()) if not sample.empty else 0,
                    "sampled_negative": int((sample[PADDY_MOD.LABEL_COL] == 0).sum()) if not sample.empty else 0,
                    "feature_count": len(features),
                }
            )
            metrics, _, _, _ = PADDY_MOD.evaluate(valid, features, tif_name, unit)
            if not metrics.empty:
                row = metrics.sort_values(["balanced_accuracy", "F1"], ascending=False).iloc[0].to_dict()
                row["scope"] = scope
                rows.append(row)
        print(f"paddy: {tif_name}")
    return pd.DataFrame(rows), pd.DataFrame(counts)


def fmt_table(df: pd.DataFrame) -> str:
    out = df.copy()
    for col in out.select_dtypes(include=[float]).columns:
        out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    out = out.fillna("")
    lines = ["| " + " | ".join(map(str, out.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(out.columns)) + " |")
    for _, row in out.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in out.columns) + " |")
    return "\n".join(lines)


def plot_metrics(metrics: pd.DataFrame) -> None:
    for metric in ["balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC"]:
        fig, ax = plt.subplots(figsize=(12, 5.5))
        for scope, sub in metrics.sort_values("tif").groupby("scope"):
            ax.plot(sub["tif"], sub[metric], marker="o", label=scope)
        ax.set_ylim(0, 1.02)
        ax.set_title(f"TIF別 XGBoost {metric}（正解TIF値 0-2）")
        ax.set_ylabel(metric)
        ax.tick_params(axis="x", rotation=45)
        ax.grid(axis="y", alpha=0.3)
        ax.legend()
        plt.tight_layout()
        fig.savefig(OUT / f"図_TIF別_XGBoost_{metric}_truth0_2.png", dpi=220, bbox_inches="tight")
        plt.close(fig)


def plot_confusion(metrics: pd.DataFrame) -> None:
    colors = {"TP": "#2ca25f", "FP": "#de2d26", "FN": "#fdae6b", "TN": "#3182bd"}
    for scope, sub in metrics.sort_values("tif").groupby("scope"):
        fig, ax = plt.subplots(figsize=(12, 5.5))
        bottom = np.zeros(len(sub))
        for col in ["TP", "FP", "FN", "TN"]:
            vals = sub[col].astype(float).to_numpy()
            ax.bar(sub["tif"], vals, bottom=bottom, label=col, color=colors[col])
            bottom += vals
        ax.set_title(f"{scope}: TIF別 XGBoost 混同行列（正解TIF値 0-2）")
        ax.set_ylabel("検証データ数")
        ax.tick_params(axis="x", rotation=45)
        ax.grid(axis="y", alpha=0.25)
        ax.legend(ncol=4)
        plt.tight_layout()
        safe_scope = scope.replace("bbox内", "").replace("画素", "pixel").replace("田んぼ", "paddy").replace("筆ポリゴン", "polygon").replace("全", "all")
        fig.savefig(OUT / f"図_{safe_scope}_XGBoost_混同行列_truth0_2.png", dpi=220, bbox_inches="tight")
        plt.close(fig)


def write_report(metrics: pd.DataFrame, counts: pd.DataFrame, summary: pd.DataFrame) -> None:
    metric_cols = [
        "scope",
        "tif",
        "unit",
        "threshold",
        "balanced_accuracy",
        "precision",
        "recall",
        "specificity",
        "F1",
        "ROC_AUC",
        "TP",
        "FP",
        "FN",
        "TN",
        "best_cv_balanced_accuracy",
        "best_params",
    ]
    lines = [
        "# TIF別 XGBoost 精度算出レポート（正解TIF値 0-2）",
        "",
        "## 条件",
        "",
        "- 正解浸水域の定義を `0 <= TIF値 <= 2` に変更した。",
        "- bbox内全画素、bbox内田んぼ画素、bbox内田んぼ筆ポリゴンの3条件で評価した。",
        "- モデルはXGBoostのみを使用した。",
        "- positive / negative は各TIF・各条件内で同数に揃えた。",
        "- 閾値は検証データ上でBalanced Accuracyが最大になる値を採用した。",
        "- 標高データは使用していない。",
        "",
        "## 要約統計",
        "",
        fmt_table(summary),
        "",
        "## 母数と抽出数",
        "",
        fmt_table(counts),
        "",
        "## TIF別評価",
        "",
        fmt_table(metrics[[c for c in metric_cols if c in metrics.columns]]),
        "",
        "## 図",
        "",
        "- `図_TIF別_XGBoost_balanced_accuracy_truth0_2.png`",
        "- `図_TIF別_XGBoost_precision_truth0_2.png`",
        "- `図_TIF別_XGBoost_recall_truth0_2.png`",
        "- `図_TIF別_XGBoost_specificity_truth0_2.png`",
        "- `図_TIF別_XGBoost_ROC_AUC_truth0_2.png`",
        "- `図_*_XGBoost_混同行列_truth0_2.png`",
    ]
    (OUT / "TIF別_XGBoost_精度算出レポート_truth0_2.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    setup_font()
    OUT.mkdir(parents=True, exist_ok=True)
    diffs = {name: ALL_MOD.read_float(path) for name, path in ALL_MOD.DIFF_RASTERS.items()}
    all_metrics, all_counts = evaluate_all_pixels(diffs)
    paddy_metrics, paddy_counts = evaluate_paddy(diffs)
    metrics = pd.concat([all_metrics, paddy_metrics], ignore_index=True)
    counts = pd.concat([all_counts, paddy_counts], ignore_index=True)
    metrics = metrics.sort_values(["scope", "tif", "unit"]).reset_index(drop=True)
    counts = counts.sort_values(["scope", "tif"]).reset_index(drop=True)

    summary_rows = []
    for scope, sub in metrics.groupby("scope"):
        row = {"scope": scope, "evaluated_tif_count": int(sub["tif"].nunique())}
        for col in ["balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC"]:
            row[f"{col}_mean"] = float(sub[col].mean())
            row[f"{col}_min"] = float(sub[col].min())
            row[f"{col}_max"] = float(sub[col].max())
        summary_rows.append(row)
    summary = pd.DataFrame(summary_rows)

    metrics.to_csv(OUT / "TIF別_XGBoost_評価_truth0_2.csv", index=False, encoding="utf-8-sig")
    counts.to_csv(OUT / "TIF別_XGBoost_母数と抽出数_truth0_2.csv", index=False, encoding="utf-8-sig")
    summary.to_csv(OUT / "TIF別_XGBoost_要約統計_truth0_2.csv", index=False, encoding="utf-8-sig")
    plot_metrics(metrics)
    plot_confusion(metrics)
    write_report(metrics, counts, summary)
    print(summary.to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
