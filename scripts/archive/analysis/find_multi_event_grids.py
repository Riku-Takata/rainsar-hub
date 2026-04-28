import os
import re
from pathlib import Path
import logging

# Setup paths
BASE_DIR = Path(__file__).resolve().parents[2]
SAMPLES_DIR = BASE_DIR / "data_vv" / "samples"

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("find_grids")

def main():
    if not SAMPLES_DIR.exists():
        logger.error(f"Samples dir not found: {SAMPLES_DIR}")
        return

    logger.info("Scanning for grids with >= 3 events...")
    
    found_grids = []
    
    # Iterate all grid directories
    for grid_dir in SAMPLES_DIR.iterdir():
        if not grid_dir.is_dir():
            continue
            
        grid_id = grid_dir.name
        
        # Count event subdirectories
        event_count = 0
        events = []
        
        for item in grid_dir.iterdir():
            if item.is_dir():
                # Check if it looks like an event folder (e.g. "05.2h")
                # pattern: float + h
                if re.match(r"^\d+\.\d+h$", item.name):
                    # verify it has a tif
                    tifs = list(item.glob("*.tif"))
                    if tifs:
                        event_count += 1
                        events.append(item.name)
        
        if event_count >= 3:
            found_grids.append((grid_id, event_count, sorted(events)))

    # Sort by count desc
    found_grids.sort(key=lambda x: x[1], reverse=True)
    
    # Save to CSV
    output_csv = BASE_DIR / "data_vv" / "analysis" / "multi_event_grids.csv"
    with open(output_csv, "w") as f:
        f.write("grid_id,event_count,events\n")
        for g in found_grids:
            events_str = "|".join(g[2])
            f.write(f"{g[0]},{g[1]},{events_str}\n")
            
    logger.info(f"Saved list to {output_csv}")

    if not found_grids:
        logger.info("No grids found with >= 3 events.")
    else:
        logger.info(f"Found {len(found_grids)} grids. Top 10:")
        for g in found_grids[:10]:
            print(f"{g[0]}: {g[1]} events")

if __name__ == "__main__":
    main()
