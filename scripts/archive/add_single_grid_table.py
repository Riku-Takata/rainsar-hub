import pandas as pd
from pathlib import Path

# Settings
BASE_DIR = Path(r"d:\sotsuron\rainsar-hub")
CSV_PATH = BASE_DIR / "result" / "20251205" / "single_grid" / "data_intensity.csv"
MD_PATH = BASE_DIR / "result" / "20251205" / "summary.md"

def main():
    if not CSV_PATH.exists():
        print(f"CSV not found: {CSV_PATH}")
        return

    df = pd.read_csv(CSV_PATH)
    
    # Select and rename columns for display
    # Columns in CSV: Delay, Type, Time, Mean, Median, Variance, Std, etc.
    # We want: Delay, Type, Time, Mean, Median, Variance
    
    cols_to_show = ['Delay', 'Type', 'Time', 'Mean', 'Median', 'Variance']
    # Ensure columns exist
    for c in cols_to_show:
        if c not in df.columns:
            print(f"Column {c} missing in CSV")
            return

    # Sort
    df.sort_values(by=['Delay', 'Type', 'Time'], inplace=True)
    
    # Format values
    df['Mean'] = df['Mean'].map('{:.5f}'.format)
    df['Median'] = df['Median'].map('{:.5f}'.format)
    df['Variance'] = df['Variance'].map('{:.5f}'.format)
    
    # Create Markdown Table
    table_md = "\n### 数値データ (Statistics Table)\n\n"
    table_md += "| Delay (h) | Type | Time | Mean | Median | Variance |\n"
    table_md += "| :--- | :--- | :--- | :--- | :--- | :--- |\n"
    
    for _, row in df.iterrows():
        table_md += f"| {row['Delay']} | {row['Type']} | {row['Time']} | {row['Mean']} | {row['Median']} | {row['Variance']} |\n"
    
    # Append to summary.md
    # We want to insert this after the "Single Grid Analysis" section, specifically after the graphs.
    # Let's just append it to the end of the Single Grid section or finding a marker.
    # Or just append it to the file? The user might want it in the Single Grid section.
    
    if not MD_PATH.exists():
        print(f"MD file not found: {MD_PATH}")
        return
        
    with open(MD_PATH, "r", encoding="utf-8") as f:
        content = f.read()
        
    # Find insertion point: After "### 強度差分 (Difference)" section?
    # Or maybe just after the Single Grid graphs.
    # Let's look for "## 2. 全体解析" and insert before that.
    
    marker = "## 2. 全体解析"
    if marker in content:
        parts = content.split(marker)
        new_content = parts[0] + table_md + "\n" + marker + parts[1]
    else:
        # Fallback: Append to end (though unlikely if file structure is as expected)
        new_content = content + table_md

    with open(MD_PATH, "w", encoding="utf-8") as f:
        f.write(new_content)
        
    print(f"Added table to {MD_PATH}")

if __name__ == "__main__":
    main()
