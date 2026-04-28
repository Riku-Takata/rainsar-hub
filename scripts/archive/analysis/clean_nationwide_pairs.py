import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs.csv"
OUTPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_pairs_cleaned.csv"

def clean_pairs():
    if not INPUT_CSV.exists():
        print(f"Error: {INPUT_CSV} not found.")
        return

    print(f"Loading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV)
    initial_count = len(df)
    
    # 1. Sort by delay ascending (so first occurrence is the smallest delay)
    df = df.sort_values(by='delay_h', ascending=True)
    
    # 2. Drop duplicates
    # A "duplicate" is defined as the same Satellite Pair (After+Before) for the same Grid.
    # We keep the one with the smallest Delay (most recent rain event).
    # Note: 'id' might be different, 'threshold' might be different. We prioritize the physical image acquisition context.
    
    deduped = df.drop_duplicates(subset=['grid_id', 'after_scene_id', 'before_scene_id'], keep='first')
    
    final_count = len(deduped)
    removed = initial_count - final_count
    
    print(f"Initial rows: {initial_count}")
    print(f"Final rows:   {final_count}")
    print(f"Removed:      {removed} duplicates/conflicts.")
    
    # Save
    deduped.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved cleaned list to {OUTPUT_CSV}")
    
    # Verification stats
    print("\nStats:")
    print(f"Unique Grids: {deduped['grid_id'].nunique()}")
    print("Delay distribution:")
    print(deduped['delay_h'].describe())

if __name__ == "__main__":
    clean_pairs()
