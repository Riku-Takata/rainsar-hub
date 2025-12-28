from pathlib import Path

file_path = Path(r"d:\sotsuron\rainsar-hub\result\result.md")

if not file_path.exists():
    print(f"File not found: {file_path}")
    exit(1)

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# Replacements
replacements = {
    "C:/Users/riku_/.gemini/antigravity/brain/d68da1ff-1681-42c6-8eb1-3c8ad2d58cb5/hist_short_paddy_before_after.png": "distributions/negative_analysis_v2/hist_short_paddy_before_after.png",
    "C:/Users/riku_/.gemini/antigravity/brain/d68da1ff-1681-42c6-8eb1-3c8ad2d58cb5/hist_short_road_before_after.png": "distributions/negative_analysis_v2/hist_short_road_before_after.png",
    "C:/Users/riku_/.gemini/antigravity/brain/d68da1ff-1681-42c6-8eb1-3c8ad2d58cb5/boxplot_neg_diff_comparison.png": "distributions/negative_analysis_v2/boxplot_neg_diff_comparison.png",
    "C:/Users/riku_/.gemini/antigravity/brain/d68da1ff-1681-42c6-8eb1-3c8ad2d58cb5/boxplot_mean_vs_median.png": "distributions/analysis_v2/boxplot_mean_vs_median.png"
}

for old, new in replacements.items():
    content = content.replace(old, new)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"Updated paths in {file_path}")
