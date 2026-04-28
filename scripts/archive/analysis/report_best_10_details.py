import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
GRID_LIST_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_10_diverse_grids.csv"
PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"
OUTPUT_MD = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_10_event_details.md"

def main():
    if not GRID_LIST_CSV.exists() or not PAIRS_CSV.exists():
        print("Input files missing.")
        return

    print("Loading data...")
    df_grids = pd.read_csv(GRID_LIST_CSV)
    df_pairs = pd.read_csv(PAIRS_CSV)
    
    # FILTER: Heavy Rain (>= 10mm/h) AND Short Delay (<= 12h)
    df_pairs = df_pairs[
        (df_pairs['max_gauge_mm_h'] >= 10.0) &
        (df_pairs['delay_h'] <= 12.0)
    ].copy()
    
    target_grids = df_grids['grid_id'].tolist()
    
    # Generate Report
    report_lines = []
    report_lines.append("# Diverse Top 10 Grids - Event Details\n")
    report_lines.append(f"**Total Grids**: {len(target_grids)}\n")
    report_lines.append("This report details the rainfall events associated with the selected diverse grids. Events are sorted by Delay (ascending).\n")
    
    for i, gid in enumerate(target_grids):
        events = df_pairs[df_pairs['grid_id'] == gid].sort_values('delay_h')
        count = len(events)
        
        report_lines.append(f"## {i+1}. {gid} ({count} Events)")
        report_lines.append(f"- **Scenes Required**: {events['after_scene_id'].nunique() + events['before_scene_id'].nunique()} unique images")
        
        # Table Header
        report_lines.append("| Date (UTC) | Delay (h) | Rain (mm/h) | Duration (h) | Scene Pair (After / Before) |")
        report_lines.append("|---|---|---|---|---|")
        
        for _, row in events.iterrows():
            date_str = str(row['event_end_ts_utc'])[:16]
            delay = f"{row['delay_h']:.2f}"
            rain_max = f"{row['max_gauge_mm_h']:.1f}"
            duration = int(row['hit_hours']) if pd.notnull(row['hit_hours']) else "?"
            
            # Shorten scene IDs for readability
            v_after = row['after_scene_id'].split('_')[4] if isinstance(row['after_scene_id'], str) else "UNK"
            v_before = row['before_scene_id'].split('_')[4] if isinstance(row['before_scene_id'], str) else "UNK" 
            
            # Full IDs are too long for table, maybe just date part?
            # 20250716T210006 -> 2025-07-16
            
            report_lines.append(f"| {date_str} | {delay} | {rain_max} | {duration} | {v_after} / {v_before} |")
            
        report_lines.append("\n") # Spacer
        
    # Write to file
    with open(OUTPUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
        
    print(f"Report generated: {OUTPUT_MD}")

if __name__ == "__main__":
    main()
