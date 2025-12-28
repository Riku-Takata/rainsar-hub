from pathlib import Path

# Settings
BASE_DIR = Path(r"d:\sotsuron\rainsar-hub")
MD_PATH = BASE_DIR / "result" / "20251205" / "summary.md"

content_to_add = """
### 負の差分解析 (Negative Analysis - Single Grid)
Grid `N03295E12995` における、負の差分ピクセルのみを対象とした解析結果です。

#### 負の差分 (Negative Difference)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Single Neg Diff Mean](single_grid/single_negative_diff_mean.png) | ![Single Neg Diff Median](single_grid/single_negative_diff_median.png) | ![Single Neg Diff Variance](single_grid/single_negative_diff_variance.png) |

#### 強度比較 (Intensity Comparison: Before vs After)
| 平均値 (Mean) | 中央値 (Median) | 分散 (Variance) |
| :---: | :---: | :---: |
| ![Single Neg Int Mean](single_grid/single_negative_intensity_mean.png) | ![Single Neg Int Median](single_grid/single_negative_intensity_median.png) | ![Single Neg Int Variance](single_grid/single_negative_intensity_variance.png) |
"""

def main():
    if not MD_PATH.exists():
        print(f"MD file not found: {MD_PATH}")
        return

    with open(MD_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    # Insert after "### 強度差分 (Difference)" table in Single Grid section
    # Or just append to the end of Single Grid section.
    # Let's look for the next section header "## 2. 全体解析"
    
    marker = "## 2. 全体解析"
    if marker in content:
        parts = content.split(marker)
        # Insert before the marker
        new_content = parts[0] + content_to_add + "\n" + marker + parts[1]
    else:
        new_content = content + content_to_add

    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write(new_content)
        
    print(f"Added single grid negative analysis to {MD_PATH}")

if __name__ == "__main__":
    main()
