"""
Corrected AOI Coverage Verification Script (Footprint-based)

s1_pairsテーブルに登録されているSentinel-1シーンが、
実際にグリッドのAOI範囲（±0.05°）と重なっているかを、
シーンのフットプリント（Geometry）を取得して正確に検証する。

修正点:
- STAC検索範囲: ±0.1° → シーンフットプリント取得のみに使用
- 交差判定: グリッド実AOI（±0.05°）とフットプリントの正確な交差判定
"""

import sys
import logging
import mysql.connector
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import re

# Shapely for geometry operations
try:
    from shapely.geometry import box, shape
    from shapely import wkt
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    print("ERROR: shapely not available. Install with: pip install shapely")
    sys.exit(1)

# Setup
BASE_DIR = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = BASE_DIR / "scripts"
DATA_DIR = BASE_DIR / "data"
VALIDATION_DIR = DATA_DIR / "validation"
VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
SAFE_DIR = DATA_DIR / "final" / "SAFE"

# Backend modules
BACKEND_DIR = BASE_DIR / "backend"
sys.path.append(str(BACKEND_DIR))

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

try:
    from app.services.s1_cdse_client import S1CDSEClient
    CDSE_AVAILABLE = True
except ImportError as e:
    print(f"Failed to import S1CDSEClient: {e}")
    CDSE_AVAILABLE = False

OUTPUT_CSV = VALIDATION_DIR / "invalid_pairs_corrected.csv"
LOG_FILE = VALIDATION_DIR / "verify_aoi_coverage_corrected.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AOI_Validator_Corrected")

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 3307,
    'user': 'rainsar',
    'password': 'rainsar_pw',
    'database': 'rainsar_hub'
}

def get_db_connection():
    """Connect to MySQL database"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logger.info("Database connection established")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Database connection failed: {err}")
        logger.info("Note: Ensure Docker container 'rainsarhub-db' is running")
        sys.exit(1)

def decode_grid_id(grid_id):
    """グリッドIDから緯度経度を取得"""
    pattern = r"([NS])(\d{5})([EW])(\d{5})"
    m = re.match(pattern, grid_id)
    if not m:
        raise ValueError(f"Invalid Grid ID: {grid_id}")
    
    ns, lat_str, ew, lon_str = m.groups()
    lat = float(lat_str) / 100.0
    if ns == 'S': lat = -lat
    lon = float(lon_str) / 100.0
    if ew == 'W': lon = -lon
    
    return lat, lon

def get_grid_aoi(grid_id, size=0.1):
    """
    グリッドの実際のAOI（Shapely Polygon）を取得
    size: グリッドサイズ（デフォルト 0.1° = 10km）
    """
    lat, lon = decode_grid_id(grid_id)
    half = size / 2.0  # ±0.05°
    return box(lon - half, lat - half, lon + half, lat + half)

def check_scene_in_safe_dir(scene_id, safe_dir):
    """SAFEディレクトリにシーンが存在するかチェック"""
    scene_id_clean = scene_id.replace('_COG', '')
    
    p1 = safe_dir / f"{scene_id_clean}.zip"
    p2 = safe_dir / f"{scene_id_clean}.SAFE.zip"
    
    return p1.exists() or p2.exists()

def extract_scene_timestamp(scene_id):
    """シーンIDから取得日時を抽出"""
    pattern = r'\d{8}T\d{6}'
    m = re.search(pattern, scene_id)
    if m:
        return datetime.strptime(m.group(), "%Y%m%dT%H%M%S")
    return None

def validate_scene_footprint(client, scene_id, grid_aoi_polygon, lat, lon):
    """
    CDSE STACからシーンフットプリントを取得し、グリッドAOIとの交差を検証
    
    Args:
        client: S1CDSEClient instance
        scene_id: Scene ID
        grid_aoi_polygon: Shapely Polygon (grid AOI, ±0.05°)
        lat, lon: Grid center coordinates
    
    Returns:
        (bool, str): (is_valid, reason)
    """
    scene_ts = extract_scene_timestamp(scene_id)
    if not scene_ts:
        return False, "timestamp_parse_error"
    
    # Search window: ±1 hour
    start = scene_ts - timedelta(hours=1)
    end = scene_ts + timedelta(hours=1)
    
    try:
        # STAC search (wide range to ensure we find the scene)
        scenes = client.search_grd_point_time(lat, lon, start, end, limit=50)
        
        scene_id_clean = scene_id.replace('_COG', '')
        
        for s in scenes:
            s_id = s.product_identifier or s.stac_id or ""
            if scene_id_clean in s_id or s_id in scene_id_clean:
                # Found the scene, now check its footprint
                geom = s.geometry
                
                if not geom or not geom.get('coordinates'):
                    logger.warning(f"Scene {scene_id} has no geometry data")
                    return False, "no_geometry"
                
                try:
                    # Convert GeoJSON geometry to Shapely polygon
                    scene_footprint = shape(geom)
                    
                    # Check intersection with grid AOI
                    if scene_footprint.intersects(grid_aoi_polygon):
                        # Calculate intersection area ratio
                        intersection = scene_footprint.intersection(grid_aoi_polygon)
                        coverage_ratio = intersection.area / grid_aoi_polygon.area
                        
                        if coverage_ratio > 0.01:  # At least 1% coverage
                            return True, f"valid_coverage_{coverage_ratio:.2%}"
                        else:
                            return False, f"insufficient_coverage_{coverage_ratio:.2%}"
                    else:
                        return False, "aoi_no_intersection"
                        
                except Exception as e:
                    logger.warning(f"Geometry processing error for {scene_id}: {e}")
                    return False, f"geometry_error:{str(e)[:50]}"
        
        # Scene not found in STAC search
        return False, "scene_not_found_in_stac"
        
    except Exception as e:
        logger.warning(f"STAC search error for {scene_id}: {e}")
        return False, f"stac_error:{str(e)[:50]}"

def validate_pairs():
    """
    s1_pairsテーブルのペアを検証（300グリッドのみ、フットプリントベース）
    """
    logger.info("="*60)
    logger.info("Corrected AOI Coverage Validation Started (Footprint-based)")
    logger.info("="*60)
    
    # Load thesis grids list
    GRID_LIST = DATA_DIR / "thesis_grids_with_masks.txt"
    if not GRID_LIST.exists():
        logger.error(f"Grid list not found: {GRID_LIST}")
        sys.exit(1)
    
    with open(GRID_LIST) as f:
        thesis_grids = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Target thesis grids: {len(thesis_grids)}")
    
    # Initialize CDSE Client
    if not CDSE_AVAILABLE:
        logger.error("CDSE Client not available.")
        sys.exit(1)
    
    client = S1CDSEClient()
    logger.info("CDSE Client initialized")
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Load pairs for thesis grids only
    logger.info("Loading s1_pairs from database (thesis grids only)...")
    
    placeholders = ','.join(['%s'] * len(thesis_grids))
    query = f"""
    SELECT 
        id,
        grid_id,
        event_start_ts_utc,
        after_scene_id,
        before_scene_id,
        delay_h
    FROM s1_pairs
    WHERE grid_id IN ({placeholders})
      AND after_scene_id IS NOT NULL
      AND before_scene_id IS NOT NULL
    ORDER BY grid_id, event_start_ts_utc
    """
    
    cursor.execute(query, tuple(thesis_grids))
    pairs = cursor.fetchall()
    logger.info(f"Total pairs to validate: {len(pairs)}")
    
    cursor.close()
    conn.close()
    
    # Validation results
    invalid_pairs = []
    valid_count = 0
    
    logger.info("\nStarting footprint-based validation...")
    logger.info("-" * 60)
    
    for i, pair in enumerate(pairs, 1):
        if i % 50 == 0:
            logger.info(f"Progress: {i}/{len(pairs)} ({i/len(pairs)*100:.1f}%) - Valid: {valid_count}, Invalid: {len(invalid_pairs)}")
        
        grid_id = pair['grid_id']
        after_id = pair['after_scene_id']
        before_id = pair['before_scene_id']
        
        lat, lon = decode_grid_id(grid_id)
        grid_aoi = get_grid_aoi(grid_id, size=0.1)
        
        reasons = []
        
        # Check 1: AFTER scene file exists?
        if not check_scene_in_safe_dir(after_id, SAFE_DIR):
            reasons.append("after_missing")
        else:
            # Check 2: AFTER scene footprint validation
            is_valid, reason = validate_scene_footprint(client, after_id, grid_aoi, lat, lon)
            if not is_valid:
                reasons.append(f"after_{reason}")
        
        # Check 3: BEFORE scene file exists?
        if not check_scene_in_safe_dir(before_id, SAFE_DIR):
            reasons.append("before_missing")
        else:
            # Check 4: BEFORE scene footprint validation
            is_valid, reason = validate_scene_footprint(client, before_id, grid_aoi, lat, lon)
            if not is_valid:
                reasons.append(f"before_{reason}")
        
        if reasons:
            invalid_pairs.append({
                'id': pair['id'],
                'grid_id': grid_id,
                'event_start_ts_utc': pair['event_start_ts_utc'],
                'after_scene_id': after_id,
                'before_scene_id': before_id,
                'delay_h': pair['delay_h'],
                'reason': ','.join(reasons)
            })
        else:
            valid_count += 1
    
    # Save results
    logger.info(f"\n{'='*60}")
    logger.info(f"Validation Complete")
    logger.info(f"{'='*60}")
    logger.info(f"Total pairs checked: {len(pairs)}")
    logger.info(f"Valid pairs: {valid_count} ({valid_count/len(pairs)*100:.2f}%)")
    logger.info(f"Invalid pairs: {len(invalid_pairs)} ({len(invalid_pairs)/len(pairs)*100:.2f}%)")
    
    if invalid_pairs:
        df = pd.DataFrame(invalid_pairs)
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"\nInvalid pairs saved to: {OUTPUT_CSV}")
        
        # Breakdown by reason
        logger.info("\nBreakdown by reason:")
        all_reasons = []
        for reason_str in df['reason']:
            all_reasons.extend(reason_str.split(','))
        
        reason_counts = pd.Series(all_reasons).value_counts()
        for reason, count in reason_counts.items():
            logger.info(f"  {reason}: {count}")
    else:
        logger.info("\nAll pairs are valid!")

def main():
    try:
        validate_pairs()
    except KeyboardInterrupt:
        logger.warning("\nValidation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
