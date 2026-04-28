#!/usr/bin/env python3
# rf_train_predict.py
import argparse
import numpy as np
import rasterio
import geopandas as gpd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix

def sample_stack_at_points(stack_path: str, points_gdf: gpd.GeoDataFrame):
    with rasterio.open(stack_path) as src:
        # 点をラスタCRSへ
        if points_gdf.crs != src.crs:
            points_gdf = points_gdf.to_crs(src.crs)

        coords = [(geom.x, geom.y) for geom in points_gdf.geometry]
        # shape: (n_points, n_bands)
        samples = np.array([v for v in src.sample(coords)], dtype=np.float32)

        # nodata/NaN除去
        nodata = src.nodata
        valid = np.all(np.isfinite(samples), axis=1)
        if nodata is not None:
            valid &= np.all(samples != nodata, axis=1)

        return samples[valid], points_gdf.loc[valid].copy(), src.profile

def predict_full_raster(model, stack_path: str, out_path: str):
    with rasterio.open(stack_path) as src:
        profile = src.profile.copy()
        profile.update(count=1, dtype="uint8", compress="deflate")

        # 全画素を一括は重いのでタイル（window）で処理
        with rasterio.open(out_path, "w", **profile) as dst:
            for ji, window in src.block_windows(1):
                block = src.read(window=window)  # (bands, h, w)
                bands, h, w = block.shape
                X = block.reshape(bands, -1).T.astype(np.float32)

                valid = np.all(np.isfinite(X), axis=1)
                if src.nodata is not None:
                    valid &= np.all(X != src.nodata, axis=1)

                y_pred = np.zeros((h * w,), dtype=np.uint8)
                if np.any(valid):
                    y_pred[valid] = model.predict(X[valid]).astype(np.uint8)

                dst.write(y_pred.reshape(h, w), 1, window=window)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stack", required=True, help="multi-band GeoTIFF feature stack")
    ap.add_argument("--train", required=True, help="training points (gpkg/shp/geojson)")
    ap.add_argument("--label_col", default="label", help="label column name in training data")
    ap.add_argument("--out", required=True, help="output prediction GeoTIFF")
    ap.add_argument("--n_estimators", type=int, default=500)
    ap.add_argument("--max_depth", type=int, default=None)
    ap.add_argument("--test_size", type=float, default=0.2)
    args = ap.parse_args()

    pts = gpd.read_file(args.train)
    if args.label_col not in pts.columns:
        raise ValueError(f"label column '{args.label_col}' not found in {args.train}")

    X, pts_valid, _ = sample_stack_at_points(args.stack, pts)
    y = pts_valid[args.label_col].to_numpy()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=42, stratify=y
    )

    clf = RandomForestClassifier(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        n_jobs=-1,
        random_state=42,
        class_weight="balanced_subsample",
    )
    clf.fit(X_train, y_train)

    y_hat = clf.predict(X_test)
    print("Confusion matrix:\n", confusion_matrix(y_test, y_hat))
    print(classification_report(y_test, y_hat, digits=4))

    predict_full_raster(clf, args.stack, args.out)

if __name__ == "__main__":
    main()
