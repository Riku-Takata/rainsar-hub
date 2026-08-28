# 輪島 土地利用特徴量 分析

## データ形式

- 入力データは JAXA High-Resolution Land-Use and Land-Cover Map of Japan 2024 v25.04。
- 1度タイルの GeoTIFF 形式で、輪島周辺では `LC_N37E136.tif` を使用した。
- CRS は EPSG:4326、値は 1〜15 の土地利用カテゴリ。

## 分析方法

- 既存の DEM サンプル点に対して、該当ピクセルの土地利用コードを抽出した。
- 浸水ラベル別に土地利用カテゴリの構成比を比較した。

## 浸水域で多い土地利用 上位5件

- 2: Built-up 0.911 (597点)
- 4: Cropland 0.043 (28点)
- 10: Bare 0.032 (21点)
- 5: Grassland 0.014 (9点)

## 非浸水域で多い土地利用 上位5件

- 2: Built-up 0.576 (51675点)
- 4: Cropland 0.088 (7892点)
- 9: ENF 0.084 (7498点)
- 5: Grassland 0.071 (6394点)
- 11: Bamboo forest 0.051 (4606点)

## 出力

- `samples_with_landcover.csv`: DEM・河川距離・土地利用を付与したサンプル
- `landcover_counts_by_label.csv`: ラベル別土地利用カテゴリ集計
- `landcover_ratio_comparison.csv`: 浸水/非浸水の構成比差
- `landcover_ratio_by_label.png`: 構成比分布図