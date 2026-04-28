"""
Batch execute preprocessing for expansion grids using preprocess_s1_cog.py (SNAP).
Orchestrates the conversion of SAFE files to cropped GeoTIFFs (VV/VH, Before/After).
"""
import os
import sys
import subprocess
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import shutil

# Path setup
BASE_DIR = Path(r"D:\sotsuron\rainsar-hub")
sys.path.append(str(BASE_DIR / "backend"))

# Load Env
load_dotenv(BASE_DIR / "backend/.env")
DB_USER = os.getenv('DB_USER', 'rainsar')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'rainsar_pw')
DB_PORT = os.getenv('DB_PORT_HOST', '3307')
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/rainsar_hub"

# Config
PREPROCESS_SCRIPT = BASE_DIR / "scripts/preprocessing/preprocess_s1_core.py"
SAFE_ROOT = BASE_DIR / "data/final/SAFE"
OUTPUT_ROOT = BASE_DIR / "data/expanded/samples"
CANDIDATES_CSV = BASE_DIR / "data/analysis/suggested_grids_quality.csv"
PYTHON_EXE = sys.executable 

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(BASE_DIR / "data/preprocess_expansion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PreprocessOrchestrator")

def run_preprocess(grid_id, safe_zip, pol, out_dir, rename_to):
    """
    Run preprocess_s1_core.py for a single output.
    """
    if not (SAFE_ROOT / safe_zip).exists():
        logger.warning(f"SAFE file missing: {safe_zip}")
        return False

    final_path = out_dir / rename_to
    if final_path.exists():
        return True

    temp_out_root = out_dir / "temp_proc"
    temp_out_root.mkdir(parents=True, exist_ok=True)
    
    stem = safe_zip.replace(".zip", "").replace(".SAFE", "")
    # Core script takes absolute output path
    temp_tif = temp_out_root / f"{stem}_proc.tif"
    
    cmd = [
        PYTHON_EXE, str(PREPROCESS_SCRIPT),
        "--in-file", str(SAFE_ROOT / safe_zip),
        "--out-file", str(temp_tif),
        "--grid-id", grid_id,
        "--pol", pol
    ]
    
    try:
        logging.info(f"Running SNAP for {grid_id} {pol} {safe_zip}...")
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        
        if temp_tif.exists():
            if final_path.exists(): final_path.unlink()
            temp_tif.rename(final_path)
            shutil.rmtree(temp_out_root)
            return True
        else:
            logger.error(f"Output not generated: {generated}")
            return False
            
    except subprocess.CalledProcessError as e:
        logger.error(f"SNAP Execution failed: {e.stderr.decode('utf-8')}")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        return False

def main():
    if not CANDIDATES_CSV.exists():
        logger.error("Candidates CSV not found.")
        return
        
    candidates = pd.read_csv(CANDIDATES_CSV)['grid_id'].tolist()
    logger.info(f"Loaded {len(candidates)} candidate grids.")
    
    engine = create_engine(DATABASE_URL)
    
    # Query details
    query = text("""
        SELECT *
        FROM s1_pairs s
        WHERE s.grid_id IN :grids
        AND MONTH(s.event_end_ts_utc) IN (4, 9, 10)
        AND s.delay_h BETWEEN 0 AND 12
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"grids": tuple(candidates)})
        
    # Filter > 10mm events if needed (assuming verify_unused_grids_quality filtered candidates)
    # But filtering events helps reducing work.
    
    logger.info(f"Total Events to Process: {len(df)}")
    
    processed_count = 0
    
    for idx, row in df.iterrows():
        grid_id = row['grid_id']
        delay_float = row['delay_h']
        date_str = pd.to_datetime(row['event_end_ts_utc']).strftime("%Y%m%d")
        event_dir_name = f"delay_{delay_float:.1f}h_{date_str}"
        
        out_dir = OUTPUT_ROOT / grid_id / event_dir_name
        out_dir.mkdir(parents=True, exist_ok=True)
        
        if not row['before_scene_id']:
            continue
            
        # Strip _COG for file search
        after_id = row['after_scene_id'].replace("_COG", "")
        before_id = row['before_scene_id'].replace("_COG", "")

        def find_safe_file(scene_id):
            # Check exact match first (normalized)
            exact = SAFE_ROOT / f"{scene_id}.zip"
            if exact.exists(): return exact.name
            
            # Check glob just in case
            matches = list(SAFE_ROOT.glob(f"{scene_id}*.zip"))
            if matches:
                return matches[0].name
            return None

        after_zip = find_safe_file(after_id)
        before_zip = find_safe_file(before_id)
        
        if not after_zip:
            logger.warning(f"After Scene missing: {row['after_scene_id']}")
            continue
        if not before_zip:
            logger.warning(f"Before Scene missing: {row['before_scene_id']}")
            continue
        
        # 1. After VV
        run_preprocess(grid_id, after_zip, "VV", out_dir, "after_vv.tif")
        # 2. After VH
        run_preprocess(grid_id, after_zip, "VH", out_dir, "after_vh.tif")
        # 3. Before VV
        run_preprocess(grid_id, before_zip, "VV", out_dir, "before_vv.tif")
        # 4. Before VH
        run_preprocess(grid_id, before_zip, "VH", out_dir, "before_vh.tif")
        
        processed_count += 1
        if processed_count % 10 == 0:
            logger.info(f"Processed {processed_count} / {len(df)} events.")

if __name__ == "__main__":
    main()
