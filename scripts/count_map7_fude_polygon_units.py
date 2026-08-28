from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.features import rasterize


ROOT = Path(__file__).resolve().parents[1]
DETECTION_DIR = ROOT / "output/gsi_h30_geojson_s1/map7_rain_s1/kurume_inundation_analysis/map7_detection_test"
LAND_DIR = ROOT / "output/gsi_h30_geojson_s1/map7_land_polygons"
OUT_DIR = DETECTION_DIR / "fude_polygon_unit_counts"

FUDE_GEOJSON = LAND_DIR / "map7_fude_polygons_from_db.geojson"
PADDY_GEOJSON = LAND_DIR / "map7_fude_paddy_polygons_from_db.geojson"
TEMPLATE = DETECTION_DIR / "map7_mean_diff_0_3h.tif"
TRUTH_MASK = DETECTION_DIR / "map7_inundation_truth_mask.tif"
PADDY_MASK = DETECTION_DIR / "landmask_filter/map7_paddy_mask.tif"


def load_features(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("features", [])


def read_bool(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1) > 0


def read_valid_mask(path: Path) -> tuple[np.ndarray, dict]:
    with rasterio.open(path) as src:
        arr = src.read(1).astype(np.float32)
        profile = src.profile.copy()
        nodata = src.nodata
    valid = np.isfinite(arr)
    if nodata is not None:
        valid &= arr != nodata
    return valid, profile


def rasterize_feature_ids(features: list[dict], profile: dict) -> np.ndarray:
    shapes = (
        (feature["geometry"], idx)
        for idx, feature in enumerate(features, start=1)
        if feature.get("geometry")
    )
    return rasterize(
        shapes,
        out_shape=(profile["height"], profile["width"]),
        transform=profile["transform"],
        fill=0,
        dtype="int32",
        all_touched=False,
    )


def count_unique_positive(arr: np.ndarray, mask: np.ndarray) -> int:
    ids = np.unique(arr[mask & (arr > 0)])
    return int(ids.size)


def id_pixel_table(arr: np.ndarray, truth: np.ndarray, valid: np.ndarray, n_features: int, features: list[dict]) -> pd.DataFrame:
    ids, pix = np.unique(arr[valid & (arr > 0)], return_counts=True)
    truth_ids, truth_pix = np.unique(arr[valid & truth & (arr > 0)], return_counts=True)
    df = pd.DataFrame({"feature_seq_id": ids.astype(np.int32), "sentinel_pixel_count": pix.astype(np.int32)})
    truth_df = pd.DataFrame({"feature_seq_id": truth_ids.astype(np.int32), "truth_pixel_count": truth_pix.astype(np.int32)})
    df = df.merge(truth_df, on="feature_seq_id", how="left")
    df["truth_pixel_count"] = df["truth_pixel_count"].fillna(0).astype(np.int32)
    df["is_truth_polygon"] = df["truth_pixel_count"] > 0

    props = []
    for idx in df["feature_seq_id"].to_numpy():
        prop = features[int(idx) - 1].get("properties", {})
        props.append(
            {
                "feature_seq_id": int(idx),
                "polygon_uuid": prop.get("polygon_uuid"),
                "land_type": prop.get("land_type"),
                "local_government_cd": prop.get("local_government_cd"),
                "pref_id": prop.get("pref_id"),
            }
        )
    return pd.DataFrame(props).merge(df, on="feature_seq_id", how="right")


def summarize(name: str, features: list[dict], arr: np.ndarray, truth: np.ndarray, valid: np.ndarray) -> dict:
    on_grid = arr > 0
    truth_poly = valid & truth & on_grid
    nontruth_poly = valid & (~truth) & on_grid
    polygon_ids_on_grid = np.unique(arr[valid & on_grid])
    polygon_ids_truth = np.unique(arr[truth_poly])
    polygon_ids_nontruth_only = np.setdiff1d(polygon_ids_on_grid, polygon_ids_truth, assume_unique=False)
    return {
        "scope": name,
        "source_polygon_count": len(features),
        "sentinel_grid_polygon_count": int(polygon_ids_on_grid.size),
        "truth_overlap_polygon_count": int(polygon_ids_truth.size),
        "nontruth_only_polygon_count": int(polygon_ids_nontruth_only.size),
        "truth_overlap_polygon_percent_of_grid_polygons": float(polygon_ids_truth.size / polygon_ids_on_grid.size * 100) if polygon_ids_on_grid.size else 0.0,
        "sentinel_pixels_in_scope": int((valid & on_grid).sum()),
        "truth_pixels_in_scope": int(truth_poly.sum()),
        "nontruth_pixels_in_scope": int(nontruth_poly.sum()),
    }


def md_table(df: pd.DataFrame) -> str:
    shown = df.copy()
    for col in shown.select_dtypes(include=[float]).columns:
        shown[col] = shown[col].map(lambda x: "" if pd.isna(x) else f"{x:.3f}")
    shown = shown.fillna("")
    lines = ["| " + " | ".join(map(str, shown.columns)) + " |"]
    lines.append("| " + " | ".join(["---"] * len(shown.columns)) + " |")
    for _, row in shown.iterrows():
        lines.append("| " + " | ".join(str(row[c]) for c in shown.columns) + " |")
    return "\n".join(lines)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    valid, profile = read_valid_mask(TEMPLATE)
    truth = read_bool(TRUTH_MASK)
    paddy_mask = read_bool(PADDY_MASK)

    fude_features = load_features(FUDE_GEOJSON)
    paddy_features = load_features(PADDY_GEOJSON)

    fude_arr = rasterize_feature_ids(fude_features, profile)
    paddy_arr = rasterize_feature_ids(paddy_features, profile)

    summaries = [
        summarize("all_fude", fude_features, fude_arr, truth, valid),
        summarize("paddy_fude", paddy_features, paddy_arr, truth, valid),
        {
            "scope": "paddy_mask_raster_reference",
            "source_polygon_count": np.nan,
            "sentinel_grid_polygon_count": np.nan,
            "truth_overlap_polygon_count": np.nan,
            "nontruth_only_polygon_count": np.nan,
            "truth_overlap_polygon_percent_of_grid_polygons": np.nan,
            "sentinel_pixels_in_scope": int((valid & paddy_mask).sum()),
            "truth_pixels_in_scope": int((valid & paddy_mask & truth).sum()),
            "nontruth_pixels_in_scope": int((valid & paddy_mask & ~truth).sum()),
        },
    ]
    summary_df = pd.DataFrame(summaries)
    summary_df.to_csv(OUT_DIR / "map7_fude_polygon_unit_count_summary.csv", index=False, encoding="utf-8-sig")

    paddy_table = id_pixel_table(paddy_arr, truth, valid, len(paddy_features), paddy_features)
    paddy_table.to_csv(OUT_DIR / "map7_paddy_fude_polygon_pixel_counts.csv", index=False, encoding="utf-8-sig")

    all_table = id_pixel_table(fude_arr, truth, valid, len(fude_features), fude_features)
    all_table.to_csv(OUT_DIR / "map7_all_fude_polygon_pixel_counts.csv", index=False, encoding="utf-8-sig")

    report = [
        "# map7 筆ポリゴン単位の母数確認",
        "",
        "## 定義",
        "",
        "- `source_polygon_count`: DBからmap7 bbox内として抽出済みの筆ポリゴン数",
        "- `sentinel_grid_polygon_count`: Sentinel評価グリッド上で1画素以上を持つ筆ポリゴン数",
        "- `truth_overlap_polygon_count`: 正解浸水域マスクと1画素以上重なる筆ポリゴン数",
        "- `nontruth_only_polygon_count`: Sentinel評価グリッド上にあるが、正解浸水域とは重ならない筆ポリゴン数",
        "",
        "## 結果",
        "",
        md_table(summary_df),
        "",
        "## 解釈",
        "",
        "- 筆ポリゴン単位で分類する場合、正解浸水域と1画素以上重なる筆を positive とみなすのが最初の基準になる。",
        "- ただし、1画素だけ重なる筆も positive になるため、次段階では `truth_pixel_count / sentinel_pixel_count` による浸水率閾値を検討する必要がある。",
        "- 田んぼ筆に限定した場合の positive / negative の筆数が、機械学習で扱える母数になる。",
        "",
    ]
    (OUT_DIR / "map7_fude_polygon_unit_count_report.md").write_text("\n".join(report), encoding="utf-8")
    print(summary_df.to_string(index=False))
    print(f"Wrote {OUT_DIR}")


if __name__ == "__main__":
    main()
