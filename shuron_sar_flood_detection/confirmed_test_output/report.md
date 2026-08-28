# 確認済み浸水域 SAR 差分分析

## 入力

- 正解ラベル: `D:\shuron\GT-data\sinsuiiki\shinsui.shp`
- 評価範囲: intersecting wajima inputarea.shp
- SAR ペア: `IMG-HH-ALOS2551802860-240811-UBSR2.1GUD.tif` -> `IMG-HH-ALOS2558012860-240922-UBSR2.1GUD.tif`

## 方法

- DN を `20*log10(DN) - 83` で dB に変換した。
- 被災後画像を被災前画像のグリッドへ再投影し、`post_db - pre_db` を計算した。
- `shinsui.shp` は確認済み正例として扱い、周辺未確認領域は参考比較領域として集計した。
- SAR 低下候補は、確認済み正例との F1 が最大になる `delta_db <= threshold` で抽出した。

## 結果

- しきい値: -1.693 dB
- 確認済み浸水面積: 19688.9 m2
- SAR 低下候補面積: 50956.2 m2
- precision: 0.042
- recall: 0.263
- F1: 0.073
- 確認済み浸水域 delta 中央値: 1.447 dB
- 周辺比較領域 delta 中央値: 1.786 dB

## 出力

- `confirmed_area_delta_db.tif`: SAR 差分 dB
- `confirmed_flood_label.tif`: 確認済み浸水域ラベル
- `sar_decrease_candidate.tif`: SAR 低下候補マスク
- `sar_decrease_candidate.geojson`: SAR 低下候補ポリゴン
- `summary.csv`: 全体集計
- `per_polygon_stats.csv`: ポリゴン別集計