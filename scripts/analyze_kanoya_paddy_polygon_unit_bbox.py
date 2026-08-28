from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize
from shapely.geometry import box, mapping

import analyze_kanoya_paddy_polygon_unit as base


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output/kanoya_rain_s1/kanoya_paddy_polygon_unit_bbox_report"
KAN0YA_TIF = Path(r"D:\sotsuron\kanoya\Inun_shinkawacho.tif")


def valid_bbox_geometry() -> dict:
    with rasterio.open(KAN0YA_TIF) as src:
        arr = src.read(1)
        valid = np.isfinite(arr)
        if src.nodata is not None:
            valid &= arr != src.nodata
        rows, cols = np.where(valid)
        if len(rows) == 0:
            raise ValueError("Inun_shinkawacho.tif に有効画素がありません。")
        left, top = src.xy(rows.min(), cols.min(), offset="ul")
        right, bottom = src.xy(rows.max(), cols.max(), offset="lr")
        geom = box(left, bottom, right, top)
    return mapping(geom)


def bbox_mask_from_tif() -> np.ndarray:
    profile = base.template_profile()
    geom = valid_bbox_geometry()
    mask = rasterize(
        [(geom, 1)],
        out_shape=(profile["height"], profile["width"]),
        transform=profile["transform"],
        fill=0,
        dtype="uint8",
        all_touched=True,
    ).astype(bool)
    base.write_raster(OUT / "GIS_鹿屋浸水TIF_bbox_mask.tif", mask.astype(np.uint8), "uint8", 0)
    (OUT / "鹿屋浸水TIF_bbox.geojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": geom, "properties": {"source": str(KAN0YA_TIF)}}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    return mask


def build_bbox_polygon_frame() -> tuple[pd.DataFrame, np.ndarray, gpd.GeoDataFrame]:
    features = base.load_features(base.PADDY_GEOJSON)
    profile = base.template_profile()
    shape = (profile["height"], profile["width"])
    ids = base.rasterize_polygon_ids(features, shape)
    bbox_mask = bbox_mask_from_tif()
    truth = base.read_bool(base.TRUTH_MASK)
    diffs = {name: base.read_float(path) for name, path in base.DIFF_RASTERS.items()}
    valid = bbox_mask & (ids > 0)
    for arr in diffs.values():
        valid &= np.isfinite(arr)

    base_ids, pixel_counts = np.unique(ids[valid], return_counts=True)
    df = pd.DataFrame({"feature_seq_id": base_ids.astype(np.int32), "sentinel_pixel_count": pixel_counts.astype(np.int32)})
    truth_ids, truth_counts = np.unique(ids[valid & truth], return_counts=True)
    truth_df = pd.DataFrame({"feature_seq_id": truth_ids.astype(np.int32), "truth_pixel_count": truth_counts.astype(np.int32)})
    df = df.merge(truth_df, on="feature_seq_id", how="left")
    df["truth_pixel_count"] = df["truth_pixel_count"].fillna(0).astype(np.int32)
    df["truth_pixel_ratio"] = df["truth_pixel_count"] / df["sentinel_pixel_count"]
    df[base.LABEL_COL] = (df["truth_pixel_count"] > 0).astype(np.uint8)

    props = []
    for uid in df["feature_seq_id"].to_numpy():
        prop = features[int(uid) - 1].get("properties", {})
        props.append(
            {
                "feature_seq_id": int(uid),
                "polygon_uuid": prop.get("polygon_uuid"),
                "local_government_cd": prop.get("local_government_cd"),
                "pref_id": prop.get("pref_id"),
                "land_type": prop.get("land_type"),
            }
        )
    df = pd.DataFrame(props).merge(df, on="feature_seq_id", how="right")

    for name, arr in diffs.items():
        df = df.merge(base.grouped_stats(ids, arr, valid, name), on="feature_seq_id", how="left")

    df["early_mean_0_6h"] = (df["diff_0_3h_mean"] + df["diff_3_6h_mean"]) / 2
    df["late_mean_6_24h"] = (df["diff_6_12h_mean"] + df["diff_12_24h_mean"]) / 2
    df["early_minus_late"] = df["early_mean_0_6h"] - df["late_mean_6_24h"]
    df["drop_0_3_to_3_6"] = df["diff_0_3h_mean"] - df["diff_3_6h_mean"]
    df["drop_3_6_to_6_12"] = df["diff_3_6h_mean"] - df["diff_6_12h_mean"]
    df["recovery_6_12_to_12_24"] = df["diff_12_24h_mean"] - df["diff_6_12h_mean"]
    df["drop_0_3_to_6_12"] = df["diff_0_3h_mean"] - df["diff_6_12h_mean"]
    df["change_0_3_to_6_12"] = df["diff_6_12h_mean"] - df["diff_0_3h_mean"]
    profile_values = df[["diff_0_3h_mean", "diff_3_6h_mean", "diff_6_12h_mean", "diff_12_24h_mean"]].to_numpy(np.float32)
    df["profile_mean"] = np.nanmean(profile_values, axis=1)
    df["profile_std"] = np.nanstd(profile_values, axis=1)
    df["profile_range"] = np.nanmax(profile_values, axis=1) - np.nanmin(profile_values, axis=1)
    df["negative_bin_count"] = (profile_values < 0).sum(axis=1)
    df["monotonic_drop_score"] = (
        (df["diff_0_3h_mean"] >= df["diff_3_6h_mean"]).astype(int)
        + (df["diff_3_6h_mean"] >= df["diff_6_12h_mean"]).astype(int)
        + (df["diff_6_12h_mean"] <= df["diff_12_24h_mean"]).astype(int)
    )

    gdf = gpd.read_file(base.PADDY_GEOJSON).reset_index().rename(columns={"index": "feature_index"})
    gdf["feature_seq_id"] = gdf["feature_index"] + 1
    return df, ids, gdf


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


def write_report(counts: pd.DataFrame, metrics: pd.DataFrame, pred_df: pd.DataFrame) -> None:
    best = metrics.sort_values("balanced_accuracy", ascending=False).head(1)
    pred_counts = pd.DataFrame(
        [
            {
                "all_bbox_polygons": len(pred_df),
                "truth_inundated_polygons": int(pred_df[base.LABEL_COL].sum()),
                "predicted_inundated_polygons": int(pred_df["predicted_inundated"].sum()),
            }
        ]
    )
    lines = [
        "# 鹿屋 bbox内 田んぼ筆ポリゴン単位 浸水判定レポート",
        "",
        "## 条件",
        "",
        "- `D:/sotsuron/kanoya/Inun_shinkawacho.tif` の有効画素bbox内にSentinel画素を持つ田んぼ筆ポリゴンだけを対象にしました。",
        "- 正解浸水域は従来通り `0.5 <= TIF値 <= 1.7` です。",
        "- 特徴量はbbox内に含まれる各筆のSentinel画素だけから再集計しました。",
        "- positive / negative を同数抽出し、train/test = 7:3、GridSearchCVで評価しました。",
        "",
        "## 母数と抽出数",
        "",
        md_table(counts),
        "",
        "## 最良モデル",
        "",
        md_table(best[["model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "TP", "FP", "FN", "TN", "best_cv_balanced_accuracy", "best_params"]]),
        "",
        "## 全モデル結果",
        "",
        md_table(metrics[["model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "F1", "ROC_AUC", "best_cv_balanced_accuracy", "best_params"]]),
        "",
        "## bbox内全筆へ適用した判定数",
        "",
        md_table(pred_counts),
        "",
        "## 注意",
        "",
        "bbox内に限定しても正解浸水筆が少ないため、精度は不安定です。この結果はモデル性能の確定値ではなく、鹿屋の正解データでは学習サンプルが不足していることを示す結果として扱うべきです。",
        "",
    ]
    (OUT / "鹿屋_bbox内_田んぼ筆ポリゴン単位_浸水判定レポート.md").write_text("\n".join(lines), encoding="utf-8-sig")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    base.OUT = OUT
    polygon_df, ids, gdf = build_bbox_polygon_frame()
    valid_df = polygon_df.dropna(subset=base.FEATURES + [base.LABEL_COL]).copy()
    sampled = base.balanced_sample(valid_df)
    counts = pd.DataFrame(
        [
            {
                "available_positive": int((valid_df[base.LABEL_COL] == 1).sum()),
                "available_negative": int((valid_df[base.LABEL_COL] == 0).sum()),
                "sampled_positive": int((sampled[base.LABEL_COL] == 1).sum()) if not sampled.empty else 0,
                "sampled_negative": int((sampled[base.LABEL_COL] == 0).sum()) if not sampled.empty else 0,
                "feature_count": len(base.FEATURES),
            }
        ]
    )
    metrics, scans, best_est = base.evaluate(sampled)
    pred_df = base.predict(valid_df, best_est)

    polygon_df.to_csv(OUT / "鹿屋_bbox内_田んぼ筆ポリゴン特徴量.csv", index=False, encoding="utf-8-sig")
    sampled.to_csv(OUT / "抽出データ.csv", index=False, encoding="utf-8-sig")
    counts.to_csv(OUT / "母数と抽出数.csv", index=False, encoding="utf-8-sig")
    metrics.to_csv(OUT / "全モデル評価指標.csv", index=False, encoding="utf-8-sig")
    scans.to_csv(OUT / "閾値スキャン.csv", index=False, encoding="utf-8-sig")
    pred_df.to_csv(OUT / "鹿屋_bbox内_田んぼ筆ポリゴン_全判定一覧.csv", index=False, encoding="utf-8-sig")

    base.export_gis(pred_df, ids, gdf)
    best = metrics.sort_values("balanced_accuracy", ascending=False).iloc[0]
    base.plot_confusion(best)
    base.plot_metrics(metrics)
    write_report(counts, metrics, pred_df)
    print(counts.to_string(index=False))
    print(metrics.sort_values("balanced_accuracy", ascending=False)[["model", "threshold", "balanced_accuracy", "precision", "recall", "specificity", "ROC_AUC", "TP", "FP", "FN", "TN"]].to_string(index=False))
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
