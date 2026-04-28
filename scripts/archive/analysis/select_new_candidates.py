import pandas as pd
from pathlib import Path
import logging

# Setup paths
BASE_DIR = Path(__file__).resolve().parents[2]
NATIONWIDE_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "nationwide_grid_summary.csv"
OLD_CANDIDATES_CSV = BASE_DIR / "data_vv" / "analysis" / "top20_candidates.csv"
OUTPUT_CSV = BASE_DIR / "data_vv" / "analysis" / "nationwide" / "new_candidates_50.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("select_new")

def main():
    if not NATIONWIDE_CSV.exists():
        logger.error(f"Nationwide summary not found: {NATIONWIDE_CSV}")
        return
    
    # 1. Load All Grids
    df_all = pd.read_csv(NATIONWIDE_CSV)
    logger.info(f"Loaded {len(df_all)} total grids.")
    
    # 2. Load Exclude List
    exclude_ids = set()
    if OLD_CANDIDATES_CSV.exists():
        df_old = pd.read_csv(OLD_CANDIDATES_CSV)
        exclude_ids = set(df_old['GridID'].unique())
        logger.info(f"Loaded {len(exclude_ids)} grids to exclude.")
    else:
        logger.warning("Old candidates file not found, no exclusions applied.")

    # 3. Filter
    df_filtered = df_all[~df_all['grid_id'].isin(exclude_ids)].copy()
    logger.info(f"Remaining grids after exclusion: {len(df_filtered)}")
    
    # 4. Rank / Sort
    # Priority: High Pair Count -> Low Min Delay
    df_sorted = df_filtered.sort_values(
        by=['pair_count', 'min_delay'], 
        ascending=[False, True]
    )
    
    # 5. Select Top 50
    df_top50 = df_sorted.head(50)
    
    # 6. Save
    df_top50.to_csv(OUTPUT_CSV, index=False)
    logger.info(f"Saved top 50 new candidates to {OUTPUT_CSV}")
    
    # 7. Print for user
    print("\n--- Top 10 New Candidates ---")
    print(df_top50[['grid_id', 'pair_count', 'min_delay', 'mean_delay']].head(10).to_string(index=False))
    print(f"\n... and {len(df_top50)-10} more. See file for details.")

if __name__ == "__main__":
    main()
