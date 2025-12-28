from pathlib import Path

file_path = Path(r"d:\sotsuron\rainsar-hub\result\result.md")

append_content = """
### 負の差分ピクセルの詳細分析 (ヒストグラムと長時間変動)

さらに深く分析するために、**「短時間で負の差分を示したピクセル」**がどのような特性を持っているのか、そして**「長時間ではどうなるか」**を調査しました。

#### 1. 元の強度分布 (短時間, 負のピクセル)
「差分が負になったピクセル」の、降雨前（Before）と降雨後（After）の強度分布を比較しました。

![Histogram Paddy Before/After](C:/Users/riku_/.gemini/antigravity/brain/d68da1ff-1681-42c6-8eb1-3c8ad2d58cb5/hist_short_paddy_before_after.png)

*   **田んぼ（Paddy）**: 緑色（Before）に比べて、青色（After）の分布が**全体的に左側（低強度側）にシフト**しています。これは、特定のピクセルがランダムに下がったのではなく、**明確な物理現象（鏡面反射による減衰）**によって強度が低下したことを示唆しています。

![Histogram Road Before/After](C:/Users/riku_/.gemini/antigravity/brain/d68da1ff-1681-42c6-8eb1-3c8ad2d58cb5/hist_short_road_before_after.png)

*   **道路（Road）**: Before（オレンジ）とAfter（赤）の分布は**ほぼ重なっています**。これは、道路における「負の差分」が、単なる強度のゆらぎ（ノイズ）であることを裏付けています。

#### 2. 長時間（>6h）における負のピクセルの変化
「負の差分」を示すピクセルの割合と深さが、長時間経過後にどう変化するかを比較しました。

![Boxplot Neg Diff Short vs Long](C:/Users/riku_/.gemini/antigravity/brain/d68da1ff-1681-42c6-8eb1-3c8ad2d58cb5/boxplot_neg_diff_comparison.png)

| 期間 | 田んぼ: 負ピクセル率 | 田んぼ: 平均減少量 (Diff) | 道路: 平均減少量 (Diff) |
| :--- | :--- | :--- | :--- |
| **短時間 (≦ 1.5h)** | **26.6%** | **-0.161** (深い減少) | -0.036 (ノイズ) |
| **長時間 (> 6h)** | **34.1%** | **-0.122** (やや浅くなる) | -0.037 (一定) |

*   **田んぼの変化**:
    1.  **範囲の拡大**: 負の差分を示すピクセルの割合が増加しています（26% -> 34%）。これは、時間の経過とともに降雨の影響を受ける領域が広がった、あるいは風などの影響でランダムに変動するピクセルが増えた可能性があります。
    2.  **深度の緩和**: 平均的な減少量はやや浅くなりました（-0.16 -> -0.12）。これは、降雨直後の「激しい冠水（強い鏡面反射）」から、徐々に排水が進んだり、風で水面が波立ったりすることで、極端な低強度が緩和された可能性があります。

### 結論 (Negative Analysis)
*   **短時間**: 田んぼの負のピクセルは「強い冠水シグナル」を含んでおり、道路ノイズとは明確に区別できます。
*   **長時間**: 負の領域は広がるものの、減少の程度は弱まります。これは「冠水のピーク」が過ぎ、排水や環境変化への移行期であることを示唆しています。
"""

if file_path.exists():
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(append_content)
    print(f"Appended detailed analysis to {file_path}")
else:
    print(f"File not found: {file_path}")
