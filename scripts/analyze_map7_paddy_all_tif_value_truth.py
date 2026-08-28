from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, confusion_matrix, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
KURUME_DIR = Path(r"D:\sotsuron\kurume")
OUT_DIR = DETECTION_DIR / "paddy_all_tif_value_truth"

LABEL_ORIGINAL = "truth_0p5_1p7"
LABEL_ALL_TIF = "truth_all_tif_positive"
SEED = 42
N_PER_CLASS = 5000

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


def reproject_all_tif_positive(template_path: Path, shape: tuple[int, int]) -> tuple[np.ndarray, pd.DataFrame]:
    union = np.zeros(shape, dtype=bool)
    rows = []
    with rasterio.open(template_path) as template:
        for path in sorted(KURUME_DIR.glob("*.tif")):
            with rasterio.open(path) as src:
                with WarpedVRT(
                    src,
                    crs=template.crs,
                    transform=template.transform,
                    width=template.width,
                    height=template.height,
                    resampling=Resampling.nearest,
                    nodata=src.nodata,
                ) as vrt:
                    arr = vrt.read(1).astype(np.float32)
                valid = np.isfinite(arr)
                if src.nodata is not None:
                    valid &= arr != src.nodata
                positive = valid & (arr > 0)
                in_range = valid & (arr >= 0.5) & (arr <= 1.7)
                union |= positive
                rows.append(
                    {
                        "tif": path.name,
                        "positive_pixels_on_sentinel_grid": int(positive.sum()),
                        "range_0p5_1p7_pixels_on_sentinel_grid": int(in_range.sum()),
                    }
                )
    rows.append(
        {
            "tif": "union",
            "positive_pixels_on_sentinel_grid": int(union.sum()),
            "range_0p5_1p7_pixels_on_sentinel_grid": np.nan,
        }
    )
    return union, pd.DataFrame(rows)


def build_features() -> tuple[pd.DataFrame, tuple[int, int]]:
    d0_path = DETECTION_DIR / "map7_mean_diff_0_3h.tif"
    d0 = read_float(d0_path)
    d3 = read_float(DETECTION_DIR / "map7_mean_diff_3_6h.tif")
    d6 = read_float(DETECTION_DIR / "map7_mean_diff_6_12h.tif")
    d12 = read_float(DETECTION_DIR / "map7_mean_diff_12_24h.tif")
    paddy = read_bool(DETECTION_DIR / "landmask_filter" / "map7_paddy_mask.tif")
    original_truth = read_bool(DETECTION_DIR / "map7_inundation_truth_mask.tif")
    all_tif_truth, tif_counts = reproject_all_tif_positive(d0_path, d0.shape)
    valid = np.isfinite(d0) & np.isfinite(d3) & np.isfinite(d6) & np.isfinite(d12) & paddy
    row, col = np.where(valid)
    df = pd.DataFrame(
        {
            "row": row,
            "col": col,
            LABEL_ORIGINAL: original_truth[valid].astype(np.uint8),
            LABEL_ALL_TIF: all_tif_truth[valid].astype(np.uint8),
            "diff_0_3h": d0[valid],
            "diff_3_6h": d3[valid],
            "diff_6_12h": d6[valid],
            "diff_12_24h": d12[valid],
        }
    )
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
    tif_counts.to_csv(OUT_DIR / "kurume_tif_positive_pixel_counts_on_sentinel_grid.csv", index=False, encoding="utf-8-sig")
    return df, d0.shape


def cohen_d(pos: pd.Series, neg: pd.Series) -> float:
    pos = pos.dropna().to_numpy(float)
    neg = neg.dropna().to_numpy(float)
    pooled = np.sqrt(((len(pos) - 1) * pos.var(ddof=1) + (len(neg) - 1) * neg.var(ddof=1)) / (len(pos) + len(neg) - 2))
    return float((pos.mean() - neg.mean()) / pooled) if pooled > 0 else np.nan


def summarize_features(df: pd.DataFrame, label_col: str) -> pd.DataFrame:
    rows = []
    for feature in FEATURES:
        pos = df.loc[df[label_col] == 1, feature]
        neg = df.loc[df[label_col] == 0, feature]
        rows.append(
            {
                "label_definition": label_col,
                "feature": feature,
                "inundated_mean": pos.mean(),
                "non_inundated_mean": neg.mean(),
                "mean_difference": pos.mean() - neg.mean(),
                "cohen_d": cohen_d(pos, neg),
                "abs_cohen_d": abs(cohen_d(pos, neg)),
            }
        )
    return pd.DataFrame(rows).sort_values("abs_cohen_d", ascending=False)


def balanced_sample(df: pd.DataFrame, label_col: str) -> pd.DataFrame:
    pos = df[df[label_col] == 1]
    neg = df[df[label_col] == 0]
    n = min(N_PER_CLASS, len(pos), len(neg))
    return pd.concat(
        [
            pos.sample(n=n, random_state=SEED),
            neg.sample(n=n, random_state=SEED),
        ],
        ignore_index=True,
    ).sample(frac=1, random_state=SEED)


def model_metrics(df: pd.DataFrame, label_col: str) -> pd.DataFrame:
    sampled = balanced_sample(df, label_col)
    x = sampled[FEATURES].to_numpy(np.float32)
    y = sampled[label_col].to_numpy(np.int32)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, stratify=y, random_state=SEED)
    models = {
        "logistic_regression": Pipeline([("scaler", StandardScaler()), ("model", LogisticRegression(max_iter=3000, random_state=SEED))]),
        "random_forest": RandomForestClassifier(n_estimators=300, min_samples_leaf=50, max_features="sqrt", random_state=SEED, n_jobs=1),
        "xgboost": XGBClassifier(
            objective="binary:logistic",
            eval_metric="logloss",
            n_estimators=300,
            max_depth=3,
            learning_rate=0.05,
            min_child_weight=3,
            subsample=0.85,
            colsample_bytree=0.85,
            random_state=SEED,
            n_jobs=1,
        ),
    }
    rows = []
    for name, model in models.items():
        model.fit(x_train, y_train)
        prob = model.predict_proba(x_test)[:, 1]
        best = None
        for th in np.linspace(0.05, 0.95, 181):
            pred = (prob >= th).astype(np.int32)
            tn, fp, fn, tp = confusion_matrix(y_test, pred, labels=[0, 1]).ravel()
            row = {
                "label_definition": label_col,
                "model": name,
                "threshold": float(th),
                "balanced_accuracy": balanced_accuracy_score(y_test, pred),
                "precision": precision_score(y_test, pred, zero_division=0),
                "recall": recall_score(y_test, pred, zero_division=0),
                "specificity": tn / (tn + fp) if tn + fp else 0.0,
                "F1": f1_score(y_test, pred, zero_division=0),
                "ROC_AUC": roc_auc_score(y_test, prob),
                "TP": int(tp),
                "FP": int(fp),
                "FN": int(fn),
                "TN": int(tn),
                "sampled_per_class": int(min((sampled[label_col] == 1).sum(), (sampled[label_col] == 0).sum())),
            }
            if best is None or row["balanced_accuracy"] > best["balanced_accuracy"]:
                best = row
        rows.append(best)
    return pd.DataFrame(rows).sort_values("balanced_accuracy", ascending=False)


def write_mask(path: Path, data: np.ndarray) -> None:
    with rasterio.open(DETECTION_DIR / "map7_mean_diff_0_3h.tif") as src:
        profile = src.profile.copy()
    profile.update(count=1, dtype="uint8", nodata=0, compress="deflate")
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype("uint8"), 1)


def md_table(df: pd.DataFrame, digits: int = 3) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.{digits}f}")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def plot_top_effects(feature_summary: pd.DataFrame) -> None:
    top = feature_summary.groupby("feature")["abs_cohen_d"].max().sort_values(ascending=False).head(10).index
    pivot = feature_summary[feature_summary["feature"].isin(top)].pivot(index="feature", columns="label_definition", values="abs_cohen_d")
    ax = pivot.loc[top].plot(kind="barh", figsize=(9, 5))
    ax.invert_yaxis()
    ax.set_xlabel("|Cohen's d|")
    ax.set_ylabel("")
    ax.grid(axis="x", alpha=0.3)
    fig = ax.get_figure()
    fig.tight_layout()
    fig.savefig(OUT_DIR / "truth_definition_effect_size_comparison.png", dpi=180)
    plt.close(fig)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df, shape = build_features()

    all_tif_mask = np.zeros(shape, dtype=np.uint8)
    all_tif_mask[df["row"].to_numpy(), df["col"].to_numpy()] = df[LABEL_ALL_TIF].to_numpy(np.uint8)
    write_mask(OUT_DIR / "map7_paddy_truth_all_tif_positive_mask.tif", all_tif_mask)

    counts = pd.DataFrame(
        [
            {
                "label_definition": LABEL_ORIGINAL,
                "inundated_paddy_pixels": int((df[LABEL_ORIGINAL] == 1).sum()),
                "non_inundated_paddy_pixels": int((df[LABEL_ORIGINAL] == 0).sum()),
                "all_paddy_pixels": len(df),
            },
            {
                "label_definition": LABEL_ALL_TIF,
                "inundated_paddy_pixels": int((df[LABEL_ALL_TIF] == 1).sum()),
                "non_inundated_paddy_pixels": int((df[LABEL_ALL_TIF] == 0).sum()),
                "all_paddy_pixels": len(df),
            },
            {
                "label_definition": "added_by_all_tif_positive",
                "inundated_paddy_pixels": int(((df[LABEL_ALL_TIF] == 1) & (df[LABEL_ORIGINAL] == 0)).sum()),
                "non_inundated_paddy_pixels": np.nan,
                "all_paddy_pixels": len(df),
            },
        ]
    )
    counts["inundated_paddy_percent"] = counts["inundated_paddy_pixels"] / counts["all_paddy_pixels"] * 100
    counts.to_csv(OUT_DIR / "truth_definition_paddy_pixel_counts.csv", index=False, encoding="utf-8-sig")

    feature_summary = pd.concat(
        [summarize_features(df, LABEL_ORIGINAL), summarize_features(df, LABEL_ALL_TIF)],
        ignore_index=True,
    )
    feature_summary.to_csv(OUT_DIR / "truth_definition_feature_separation.csv", index=False, encoding="utf-8-sig")
    plot_top_effects(feature_summary)

    metrics = pd.concat(
        [model_metrics(df, LABEL_ORIGINAL), model_metrics(df, LABEL_ALL_TIF)],
        ignore_index=True,
    )
    metrics.to_csv(OUT_DIR / "truth_definition_random5000_model_metrics.csv", index=False, encoding="utf-8-sig")

    report = [
        "# 田んぼ内における浸水域ラベル定義の比較",
        "",
        "## 比較したラベル",
        "",
        "- `truth_0p5_1p7`: 従来どおり `0.5 <= TIF値 <= 1.7`",
        "- `truth_all_tif_positive`: TIF値で限定せず、Kurume TIFの有効値かつ `TIF値 > 0` を浸水域",
        "",
        "## 田んぼ内画素数",
        "",
        md_table(counts),
        "",
        "## ランダム5000/5000画素での簡易モデル比較",
        "",
        md_table(
            metrics[
                [
                    "label_definition",
                    "model",
                    "threshold",
                    "balanced_accuracy",
                    "precision",
                    "recall",
                    "specificity",
                    "F1",
                    "ROC_AUC",
                    "sampled_per_class",
                ]
            ]
        ),
        "",
        "## クラス間分離が大きい特徴量",
        "",
        md_table(
            feature_summary.sort_values(["label_definition", "abs_cohen_d"], ascending=[True, False])
            .groupby("label_definition")
            .head(8)[["label_definition", "feature", "inundated_mean", "non_inundated_mean", "mean_difference", "cohen_d"]]
        ),
        "",
        "## 解釈",
        "",
        "- TIF値範囲を外すと、浅い浸水や深い浸水も正解浸水域に含まれるため、正解画素数は増える。",
        "- ただし、ラベルが広がることで後方散乱差分の時系列特徴が均質になるとは限らない。",
        "- モデル精度と効果量が上がる場合は、従来の `0.5-1.7` 条件が正解域を狭く切りすぎていた可能性がある。",
        "- 逆に下がる場合は、浅い値や外れ値を含めたことで、浸水域ラベル内のばらつきが増えた可能性がある。",
        "",
    ]
    (OUT_DIR / "truth_definition_comparison_report.md").write_text("\n".join(report), encoding="utf-8")
    print(f"Wrote {OUT_DIR}")


if __name__ == "__main__":
    main()
