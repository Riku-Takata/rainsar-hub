"""
Sentinel-1 Data Downloader
ASF (Alaska Satellite Facility) を使用してSentinel-1データをダウンロード

使用前にASFアカウント作成とログインが必要:
https://urs.earthdata.nasa.gov/users/new
"""
import requests
from pathlib import Path
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Setup
BASE_DIR = Path("d:/sotsuron/rainsar-hub")
DATA_DIR = BASE_DIR / "data"
SAFE_DIR = BASE_DIR / "data" / "final" / "SAFE"
SCENE_LIST = DATA_DIR / "scenes_to_download.txt"

# ASF credentials (環境変数から取得)
ASF_USERNAME = os.getenv('ASF_USERNAME', '')
ASF_PASSWORD = os.getenv('ASF_PASSWORD', '')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("S1Downloader")

def search_asf_scene(scene_id):
    """Search for scene on ASF"""
    # ASF Search API
    search_url = "https://api.daac.asf.alaska.edu/services/search/param"
    
    params = {
        'granule_list': scene_id,
        'output': 'json'
    }
    
    try:
        response = requests.get(search_url, params=params, timeout=30)
        response.raise_for_status()
        
        results = response.json()
        if results and len(results) > 0:
            return results[0]
        return None
    except Exception as e:
        logger.error(f"Search error for {scene_id}: {e}")
        return None

def download_scene(scene_id, output_dir, session):
    """Download a single scene"""
    output_path = output_dir / f"{scene_id}.zip"
    
    # Skip if already exists
    if output_path.exists():
        logger.info(f"Already exists: {scene_id}")
        return True
    
    logger.info(f"Searching: {scene_id}")
    scene_info = search_asf_scene(scene_id)
    
    if not scene_info:
        logger.error(f"Not found: {scene_id}")
        return False
    
    download_url = scene_info.get('url')
    if not download_url:
        logger.error(f"No download URL: {scene_id}")
        return False
    
    logger.info(f"Downloading: {scene_id}")
    logger.info(f"  URL: {download_url}")
    
    try:
        response = session.get(download_url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Get file size
        total_size = int(response.headers.get('content-length', 0))
        
        # Download with progress
        downloaded = 0
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Log progress every 100MB
                    if downloaded % (100 * 1024 * 1024) == 0:
                        progress = downloaded / total_size * 100 if total_size > 0 else 0
                        logger.info(f"  Progress: {progress:.1f}% ({downloaded / 1024 / 1024:.1f} MB)")
        
        logger.info(f"✓ Downloaded: {scene_id} ({downloaded / 1024 / 1024:.1f} MB)")
        return True
        
    except Exception as e:
        logger.error(f"Download error for {scene_id}: {e}")
        # Remove partial file
        if output_path.exists():
            output_path.unlink()
        return False

def main():
    logger.info("="*60)
    logger.info("Sentinel-1 Data Downloader")
    logger.info("="*60)
    
    # Check credentials
    if not ASF_USERNAME or not ASF_PASSWORD:
        logger.error("ASF credentials not set!")
        logger.info("\nSet credentials using:")
        logger.info("  Windows: ")
        logger.info("    set ASF_USERNAME=your_username")
        logger.info("    set ASF_PASSWORD=your_password")
        logger.info("\n  Or edit this script to set ASF_USERNAME and ASF_PASSWORD")
        logger.info("\nCreate account at: https://urs.earthdata.nasa.gov/users/new")
        return
    
    # Load scene list
    if not SCENE_LIST.exists():
        logger.error(f"Scene list not found: {SCENE_LIST}")
        return
    
    with open(SCENE_LIST, 'r') as f:
        scenes = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Scenes to download: {len(scenes)}")
    logger.info(f"Output directory: {SAFE_DIR}")
    
    # Create output directory
    SAFE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create authenticated session
    session = requests.Session()
    session.auth = (ASF_USERNAME, ASF_PASSWORD)
    
    # Download scenes
    success_count = 0
    failed_scenes = []
    
    logger.info("\nStarting downloads...")
    logger.info("="*60)
    
    for i, scene_id in enumerate(scenes, 1):
        logger.info(f"\n[{i}/{len(scenes)}] {scene_id}")
        
        success = download_scene(scene_id, SAFE_DIR, session)
        
        if success:
            success_count += 1
        else:
            failed_scenes.append(scene_id)
        
        # Sleep between downloads to avoid rate limiting
        if i < len(scenes):
            time.sleep(2)
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("Download Summary")
    logger.info("="*60)
    logger.info(f"Total scenes: {len(scenes)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {len(failed_scenes)}")
    
    if failed_scenes:
        logger.info(f"\nFailed scenes:")
        for scene in failed_scenes:
            logger.info(f"  - {scene}")
        
        # Save failed scenes
        failed_file = DATA_DIR / "download_failed.txt"
        with open(failed_file, 'w') as f:
            for scene in failed_scenes:
                f.write(f"{scene}\n")
        logger.info(f"\nFailed scenes saved to: {failed_file}")

if __name__ == "__main__":
    main()
