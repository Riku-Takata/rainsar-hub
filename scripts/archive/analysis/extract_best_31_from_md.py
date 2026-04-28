import re
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
MD_FILE = BASE_DIR / "data_vv" / "analysis" / "grid_event_details_best_31.md"
OUTPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "best_31_grids_extracted.csv"

def parse_md_to_csv():
    if not MD_FILE.exists():
        print(f"Error: {MD_FILE} not found.")
        return

    content = MD_FILE.read_text(encoding='utf-8')
    lines = content.split('\n')
    
    data = []
    current_grid = None
    
    # Regex to find grid header: ## Grid: N02435E12385 (Count: 8)
    grid_pattern = re.compile(r"## Grid: ([N\d]+E[\d]+)")
    
    # Regex to extract table row data
    # | S1 After... | ... | After ID | Before ID |
    # We want Delay (h) (col 5), After ID (col 7), Before ID (col 8)
    # Indices in split('|'): 0="", 1=AfterTime, 2=BeforeTime, 3=Plat, 4=RainEnd, 5=Delay, 6=MaxRain, 7=AfterID, 8=BeforeID
    
    for line in lines:
        line = line.strip()
        
        # Check for grid header
        m_grid = grid_pattern.search(line)
        if m_grid:
            current_grid = m_grid.group(1)
            continue
            
        # Check for table row (must start with | and not be header/separator)
        if line.startswith("|") and "After ID" not in line and "---" not in line:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 9: continue
            
            try:
                # Part 0 is empty string before first |
                delay_str = parts[5]
                after_id = parts[7]
                before_id = parts[8]
                
                # Clean up IDs (remove _COG if present, or keep as is? User's file has _COG)
                # The zip files on disk usually don't have _COG.SAFE.zip, just .zip or _COG.zip?
                # Usually CDSE downloads are .zip. The stored filename is just the product ID.
                # Let's keep the ID as is, and let the processor fuzzy match.
                # BUT, notice the ID in MD: S1B_..._COG
                # Real SAFE names don't usually end in _COG unless specifically processed.
                # But let's export what's there.
                
                if current_grid:
                    data.append({
                        'grid_id': current_grid,
                        'after_scene_id': after_id,
                        'before_scene_id': before_id,
                        'delay_h': float(delay_str)
                    })
            except Exception as e:
                # print(f"Skipping line: {line} -> {e}")
                pass

    df = pd.DataFrame(data)
    print(f"Extracted {len(df)} total pairs.")
    
    # Resolving Logical Conflict:
    # Multiple 'Rain End' times for same 'Satellite Pair' -> Ambiguous correlation.
    # Logic: Keep only the event with Minimum Delay (closest to image acquisition).
    
    # Sort by Delay ascending
    df = df.sort_values(by='delay_h', ascending=True)
    
    # Drop duplicates, keeping first (min delay)
    before_count = len(df)
    df = df.drop_duplicates(subset=['grid_id', 'after_scene_id', 'before_scene_id'], keep='first')
    after_count = len(df)
    
    print(f"Resolved {before_count} -> {after_count} unique Image-Event pairs (removed {before_count-after_count} duplicates/conflicts).")
    
    # Save
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved to {OUTPUT_CSV}")
    print(df.head())

if __name__ == "__main__":
    parse_md_to_csv()
