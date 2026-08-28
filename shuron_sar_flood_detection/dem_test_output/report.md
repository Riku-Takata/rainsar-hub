# 輪島 確認済み浸水域 標高分析

## データ形式

- 入力標高データは国土地理院 基盤地図情報の `FG-GML-*-DEM5A-*.zip`。
- ZIP 内に 5m メッシュ標高の XML/GML が格納されている。
- 座標は緯度経度系で、XML 内の `gml:Envelope` と `gml:tupleList` から標高値を読み取った。

## 分析条件

- 正解ラベル: `D:\shuron\GT-data\sinsuiiki\shinsui.shp`
- 非浸水比較領域: 浸水ポリゴンから 500 m バッファ内のうち、浸水ポリゴン外
- 使用DEMタイル数: 9
- 有効サンプル数: 90322

## 結果

- 浸水域の標高中央値: 5.970 m
- 非浸水域の標高中央値: 7.690 m
- 中央値差（浸水 - 非浸水）: -1.720 m
- 浸水域の平均標高: 6.066 m
- 非浸水域の平均標高: 12.878 m
- 平均差（浸水 - 非浸水）: -6.812 m

## 出力

- `elevation_stats_by_label.csv`: 浸水/非浸水別の標高統計
- `dem_samples_labeled.csv`: DEM点ごとの標高とラベル
- `elevation_distribution.png`: 標高分布ヒストグラム
- `overlapping_dem_tiles.csv`: 使用したDEMタイル一覧