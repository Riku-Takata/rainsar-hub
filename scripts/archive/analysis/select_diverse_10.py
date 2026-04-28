import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
BEST_50_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_50_grids_v3.csv"
PAIRS_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"
OUTPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "best_10_diverse_grids.csv"

def main():
    if not BEST_50_CSV.exists() or not PAIRS_CSV.exists():
        print("Input files missing.")
        return

    print("Loading data...")
    df_grids = pd.read_csv(BEST_50_CSV)
    df_pairs = pd.read_csv(PAIRS_CSV)
    
    # Filter pairs to only those relevant to the top 50
    # (Actually we want to check overlap against *selected* so far)
    candidate_grids = df_grids['grid_id'].tolist()
    
    # FILTER: Heavy Rain (>= 10mm/h) AND Short Delay (<= 12h)
    print(f"Total events: {len(df_pairs)}")
    df_pairs = df_pairs[
        (df_pairs['max_gauge_mm_h'] >= 10.0) & 
        (df_pairs['delay_h'] <= 12.0)
    ].copy()
    print(f"Filtered Events (Rain >= 10, Delay <= 12): {len(df_pairs)}")
    
    # Build a map of Grid -> Set(Scenes)
    grid_scenes = {}
    for gid in candidate_grids:
        rows = df_pairs[df_pairs['grid_id'] == gid]
        scenes = set()
        scenes.update(rows['after_scene_id'].dropna())
        scenes.update(rows['before_scene_id'].dropna())
        grid_scenes[gid] = scenes
        
    print(f"Loaded feature maps for {len(grid_scenes)} grids.")
    
    # Selection Loop
    # Strategy: Greedy selection. 
    # 1. Pick the grid with the highest event count (already sorted in input).
    # 2. Add its scenes to `used_scenes`.
    # 3. Re-score remaining grids based on count of *new* scenes they would add.
    #    (Or just ensure they don't have too much overlap? User said "satellite images are not covered" -> distinct)
    #    Let's prioritize: Count of (Scenes NOT in used_scenes).
    
    selected_grids = []
    used_scenes = set()
    
    candidates = df_grids.to_dict('records') # List of dicts, sorted by event count desc
    
    while len(selected_grids) < 10 and candidates:
        best_candidate = None
        best_new_scenes_count = -1
        
        # Evaluate all remaining candidates
        for cand in candidates:
            gid = cand['grid_id']
            my_scenes = grid_scenes.get(gid, set())
            
            # Calculate new contribution
            new_scenes = my_scenes - used_scenes
            score = len(new_scenes)
            
            # Optimization: If score is 0, this grid adds NOTHING new (all scenes reused).
            # We probably want to skip it unless we run out of unique options.
            
            if score > best_new_scenes_count:
                best_new_scenes_count = score
                best_candidate = cand
            elif score == best_new_scenes_count:
                # Tie-breaker: original event count (higher is better)
                # Since candidates are sorted by event count, first seen is better.
                pass
        
        if best_candidate:
            gid = best_candidate['grid_id']
            # Commit
            selected_grids.append(best_candidate)
            candidates.remove(best_candidate) # Remove from pool
            
            # Update used
            used_scenes.update(grid_scenes[gid])
            
            print(f"Selected #{len(selected_grids)}: {gid} (Events: {best_candidate['event_count']}, New Scenes: {best_new_scenes_count})")
        else:
            print("No more candidates with new scenes.")
            break
            
    # Save
    out_df = pd.DataFrame(selected_grids)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(out_df)} diverse grids to {OUTPUT_CSV}")
    print(out_df[['grid_id', 'event_count']])

if __name__ == "__main__":
    main()
