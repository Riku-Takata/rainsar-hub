"""
Download Sentinel-1 SAFE files for Expansion Grids.
Strictly downloads .SAFE (zip) files to data/final/SAFE using s1_cdse_client.
"""
import os
import sys
import logging
import concurrent.futures
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
sys.path.append(str(BASE_DIR / "backend"))

# Load Env
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

from app.services.s1_cdse_client import S1CDSEClient

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "data/download_safe_expansion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SAFEDownloader")

# Config
SAFE_DIR = BASE_DIR / "data/final/SAFE"
CANDIDATES_CSV = BASE_DIR / "data/analysis/suggested_grids_quality.csv"
MAX_WORKERS = 2  # Limit concurrency for large files (bandwidth/disk I/O)

def main():
    if not CANDIDATES_CSV.exists():
        logger.error("Candidates CSV not found.")
        return
        
    # 1. Load Candidates
    candidates = pd.read_csv(CANDIDATES_CSV)['grid_id'].tolist()
    logger.info(f"Loaded {len(candidates)} candidate grids.")
    
    # 2. Query target scenes
    engine = create_engine(DATABASE_URL)
    query = text("""
        SELECT DISTINCT after_scene_id
        FROM s1_pairs s
        WHERE s.grid_id IN :grids
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
        UNION
        SELECT DISTINCT before_scene_id
        FROM s1_pairs s
        WHERE s.grid_id IN :grids
        AND MONTH(s.event_end_ts_utc) IN (4, 8, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
    """)
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"grids": tuple(candidates)})
        
        # Merge columns (UNION results in one column name usually 'after_scene_id')
        scenes = df.iloc[:, 0].dropna().unique().tolist()
        logger.info(f"Total Unique Scenes to Download: {len(scenes)}")
        
    except Exception as e:
        logger.error(f"DB Query Failed: {e}")
        return

    # 3. Download
    client = S1CDSEClient()
    
    # Ensure directory
    SAFE_DIR.mkdir(parents=True, exist_ok=True)
    
    def download_task(scene_id):
        try:
            # S1CDSEClient.download_product checks existence internally, but requires full name often?
            # Or just ID? The implementation uses _get_odata_id_by_name which takes ID/Name.
            # Let's pass the ID.
            
            # Check existence first to avoid API call overhead if possible
            # But the client handles normalization (.zip extension etc).
            # Let the client handle it.
            
            result = client.download_product(scene_id, SAFE_DIR)
            if result:
                return "SUCCESS"
            else:
                return "FAILED"
        except Exception as e:
            logger.error(f"Error downloading {scene_id}: {e}")
            return "ERROR"

    # Execute
    succeeded = 0
    skipped = 0 # client might log skip, but returns path if exists
    failed = 0
    
    # Check existing files manually to count simplified 'skipped'
    # This is rough check
    to_download = []
    for s in scenes:
        # DB has _COG suffix, file does not.
        normalized_s = s.replace("_COG", "")
        potential_path = SAFE_DIR / f"{normalized_s}.zip"
        
        # Also check exact match just in case
        exact_path = SAFE_DIR / f"{s}.zip"
        
        if (potential_path.exists() and potential_path.stat().st_size > 0) or \
           (exact_path.exists() and exact_path.stat().st_size > 0):
            skipped += 1
        else:
            to_download.append(s)
            
    logger.info(f"Already exists: {skipped}, To download: {len(to_download)}")
    
    if not to_download:
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_task, sid): sid for sid in to_download}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            sid = futures[future]
            res = future.result()
            if res == "SUCCESS":
                succeeded += 1
                logger.info(f"[{i+1}/{len(to_download)}] Downloaded: {sid}")
            else:
                failed += 1
                logger.warning(f"[{i+1}/{len(to_download)}] Failed: {sid}")

    logger.info(f"All Done. Downloaded: {succeeded}, Failed: {failed}")

if __name__ == "__main__":
    main()
