# 輪島 河川距離特徴量 分析

## データ形式

- 河川データは `W05-07_17-g_Stream.shp` の LineString データ。
- `.prj` は無いが、座標値は経緯度のため EPSG:4326 として読み込み、距離計算時に EPSG:6675 へ変換した。
- 各 DEM サンプル点から最寄り河川ラインまでの距離をメートル単位で計算した。

## 結果

- 使用河川ライン数: 10
- 浸水域の河川距離中央値: 57.43 m
- 非浸水域の河川距離中央値: 166.83 m
- 中央値差（浸水 - 非浸水）: -109.40 m
- 浸水域の平均河川距離: 81.02 m
- 非浸水域の平均河川距離: 190.75 m

## 出力

- `dem_samples_with_river_distance.csv`: DEM点ごとの標高・ラベル・河川距離
- `river_distance_stats_by_label.csv`: 浸水/非浸水別の河川距離統計
- `river_distance_distribution.png`: 河川距離分布
- `rivers_used.geojson`: 計算に使った河川ライン