# 確認済み浸水域 SAR 差分分析

`D:\shuron\GT-data\sinsuiiki\shinsui.shp` を確認済み浸水域として、被災前後の ALOS-2 SAR 差分を集計します。

## 実行

```powershell
cd D:\shuron\confirmed_flood_sar_analysis
.\run_confirmed_flood_analysis.ps1
```

## 目的

この分析は、目視確認済みの浸水ポリゴンで SAR の後方散乱強度が本当に低下しているかを検証するためのものです。

未確認領域は厳密な非浸水ラベルではなく、周辺比較領域として扱います。

## 出力

- `output/summary.csv`: 全体集計
- `output/per_polygon_stats.csv`: ポリゴン別の SAR 差分統計
- `output/report.md`: 解析レポート
- `output/confirmed_area_delta_db.tif`: SAR 差分 dB
- `output/confirmed_flood_label.tif`: 確認済み浸水域ラベル
- `output/sar_decrease_candidate.tif`: SAR 低下候補マスク
- `output/sar_decrease_candidate.geojson`: SAR 低下候補ポリゴン
