from pathlib import Path

content = """# 2025-12-05 解析結果まとめ

本ドキュメントは、2025年12月5日に実施された後方散乱強度解析の統合結果です。
以下の4つの観点から解析を行いました。

1.  **単一Grid解析 (Single Grid)**: 特定のGrid (N03295E12995) における詳細な時系列変化。
2.  **全体解析 (Global)**: 全Gridを用いた全体的なトレンド把握。
3.  **有効データ解析 (Filtered)**: ノイズ除去後のデータを用いたトレンド解析。
4.  **負の差分解析 (Negative)**: 「降雨により後方散乱が低下する」という仮説に基づく解析。

---

## 1. 単一Grid解析 (N03295E12995)
Grid `N03295E12995` を例に、イベントごとの変化を可視化しました。

### 後方散乱強度 (Intensity)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Single Intensity Mean](single_grid/single_intensity_mean.png) | ![Single Intensity Median](single_grid/single_intensity_median.png) | ![Single Intensity Variance](single_grid/single_intensity_variance.png) |

### 強度差分 (Difference)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Single Diff Mean](single_grid/single_diff_mean.png) | ![Single Diff Median](single_grid/single_diff_median.png) | ![Single Diff Variance](single_grid/single_diff_variance.png) |

**考察**:
*   このGridでは、田んぼ（青）のばらつき（Variance）が道路（オレンジ）よりも常に高いことが確認できます。
*   差分の中央値（Median）を見ると、田んぼの方が正の方向にシフトしている傾向が見られます。

---

## 2. 全体解析 (Global Analysis)
全Gridのデータを集約し、遅延時間（Delay）に対するトレンドを見ました。

### 強度差分 (Difference)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Global Diff Mean](global/global_diff_mean.png) | ![Global Diff Median](global/global_diff_median.png) | ![Global Diff Variance](global/global_diff_variance.png) |

### 後方散乱強度 (Intensity)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Global Intensity Mean](global/global_intensity_mean.png) | ![Global Intensity Median](global/global_intensity_median.png) | ![Global Intensity Variance](global/global_intensity_variance.png) |

**考察**:
*   **分散のトレンド**: 田んぼの分散は遅延時間とともに増大する傾向がありますが、道路は比較的安定しています。
*   **ノイズの影響**: 全体解析にはノイズGridが含まれているため、道路のトレンドにも一部ばらつきが見られます。

---

## 3. 有効データ解析 (Filtered Analysis)
ノイズGridを除去した「有効データ」のみを用いた解析結果です。

### 強度差分 (Difference)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Filtered Diff Mean](filtered/filtered_diff_mean.png) | ![Filtered Diff Median](filtered/filtered_diff_median.png) | ![Filtered Diff Variance](filtered/filtered_diff_variance.png) |

### 後方散乱強度 (Intensity)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Filtered Intensity Mean](filtered/filtered_intensity_mean.png) | ![Filtered Intensity Median](filtered/filtered_intensity_median.png) | ![Filtered Intensity Variance](filtered/filtered_intensity_variance.png) |

**考察**:
*   ノイズ除去により、道路（オレンジ）のトレンドが非常に安定しました（特にVarianceとMedian）。
*   **透水シグナル**: 短時間（Delay < 1.5h）において、田んぼの中央値（Median）が道路よりも明確に高く、透水による変化を捉えています。

---

## 4. 負の差分解析 (Negative Pixel Analysis)
「降雨により後方散乱強度が低下した（差分 < 0）」ピクセルのみを抽出して解析しました。
**※強度比較グラフは、Before（降雨前）とAfter（降雨後）を同一グラフ上にプロットしています。**

### 負の差分 (Negative Difference)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Negative Diff Mean](negative/negative_diff_mean.png) | ![Negative Diff Median](negative/negative_diff_median.png) | ![Negative Diff Variance](negative/negative_diff_variance.png) |

### 強度比較 (Intensity Comparison: Before vs After)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Negative Int Combined Mean](negative/negative_intensity_combined_mean.png) | ![Negative Int Combined Median](negative/negative_intensity_combined_median.png) | ![Negative Int Combined Variance](negative/negative_intensity_combined_variance.png) |

**考察**:
*   **田んぼの特性**: 負の差分ピクセルにおいて、田んぼは道路よりも**圧倒的に低い値（強い減少）**を示しています。
*   **Before vs After**: 強度比較グラフを見ると、田んぼ（青）のAfter（●）はBefore（×）よりも明確に低い位置に分布しており、冠水による鏡面反射の影響を視覚的に確認できます。
*   **トレンド**: 遅延時間が長くなると、田んぼの負の差分はやや緩和（ゼロに近づく）傾向があり、排水や風の影響を示唆しています。

---

## 総合結論
1.  **指標の選択**: 外れ値の影響を受けにくい**「中央値 (Median)」**が最も適しています。
2.  **ノイズ除去**: 道路の分散が高いGridを除外することで、解析精度が大幅に向上しました。
3.  **透水性推定**:
    *   **正の変化**: 短時間の「有効データ・中央値」の差分（田んぼ - 道路）を見ることで、土壌水分増加（透水）を推定可能。
    *   **負の変化**: 「負の差分ピクセル」の強度低下を見ることで、冠水リスクを推定可能。
"""

output_path = Path(r"d:\sotsuron\rainsar-hub\result\20251205\summary.md")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"Successfully wrote to {output_path}")
