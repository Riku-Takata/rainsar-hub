#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/download_s1_by_area.py

- 指定した緯度・経度と期間に基づいて、Sentinel-1 (SAR) の GRD画像をCDSEから網羅的に検索＆ダウンロードする。
- 昇交・降交軌道 (Ascending/Descending) で件数を集計し、最もデータ数が多い軌道方向の画像を自動で選択してダウンロードする機能を含む。
"""

import argparse
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict
import time
import requests
from dotenv import load_dotenv

from app.services.s1_cdse_client import S1CDSEClient
from app.core.config import settings

logger = logging.getLogger(__name__)

def patch_s1cdse_client_for_password_grant():
    """
    s1_cdse_client.py を書き換えることなく、
    S1CDSEClientのトークン取得メソッド( _get_token )を
    「CDSEのユーザー名とパスワードを使う方式(Password Grant)」にすり替える。
    """
    original_get_token = S1CDSEClient._get_token
    
    def custom_get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._access_token and now < self._token_expire_at:
            return self._access_token

        # .env から直接 username / password を取得
        env_path = Path(__file__).parent.parent / ".env"
        load_dotenv(env_path)
        
        username = os.getenv("CDSE_USERNAME")
        password = os.getenv("CDSE_PASSWORD")
        
        # もし username/password が無ければ元のclient_credentials方式にフォールバック
        if not username or not password:
            logger.warning("No CDSE_USERNAME or CDSE_PASSWORD found in .env. Falling back to default client_credentials.")
            return original_get_token(self)

        data = {
            "grant_type": "password",
            "client_id": "cdse-public", # CDSEの汎用パスワード用パブリッククライアントID
            "username": username,
            "password": password,
        }

        try:
            resp = self._session.post(self._token_url, data=data, timeout=30)
            resp.raise_for_status()
            j = resp.json()
            access_token = j["access_token"]
            expires_in = int(j.get("expires_in", 3600))
            self._access_token = access_token
            # CDSEのパブリッククライアントの場合もあるので少し余裕を持たせる
            self._token_expire_at = now + timedelta(seconds=expires_in - 60)
            return access_token
        except Exception as e:
            logger.error(f"Failed to get CDSE token via Password Grant: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return ""

    # メソッドを差し替え
    S1CDSEClient._get_token = custom_get_token

def main():
    patch_s1cdse_client_for_password_grant()
    parser = argparse.ArgumentParser(description="Download S1 GRD images by area, period and dominant orbit direction.")
    parser.add_argument("--lat", type=float, required=True, help="Target Latitude")
    parser.add_argument("--lon", type=float, required=True, help="Target Longitude")
    parser.add_argument("--start", type=str, required=True, help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, required=True, help="End Date (YYYY-MM-DD)")
    parser.add_argument("--out-dir", type=str, default=r"E:\s1_data", help="Output directory to save zip files")
    parser.add_argument("--auto-orbit", action="store_true", default=True, help="Automatically select the orbit direction with the most scenes")
    
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}. Please use YYYY-MM-DD.")
        return

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    client = S1CDSEClient()

    logger.info(f"Searching for S1 IW GRD scenes around (lat={args.lat}, lon={args.lon}) from {args.start} to {args.end}")
    
    # 検索範囲内のすべてのシーンを取得（VV, VH, IWモードはS1CDSEClientのデフォルトになっている）
    scenes = client.search_grd_point_time(
        lat=args.lat,
        lon=args.lon,
        start=start_date,
        end=end_date,
        limit=500  # 長期間の場合は多めに設定
    )

    if not scenes:
        logger.warning("No scenes found for the specified criteria.")
        return

    logger.info(f"Total raw scenes found: {len(scenes)}")

    # 軌道方向ごとに集計
    orbit_counts = defaultdict(list)
    for s in scenes:
        direction = str(s.orbit_direction).upper() if s.orbit_direction else "UNKNOWN"
        # 軽く正規化
        if "ASC" in direction:
            direction = "ASCENDING"
        elif "DES" in direction or "DSC" in direction:
            direction = "DESCENDING"
            
        orbit_counts[direction].append(s)

    for direction, direction_scenes in orbit_counts.items():
        logger.info(f"  - {direction}: {len(direction_scenes)} scenes")

    # 最もシーン数が多い軌道方向を選択する
    if args.auto_orbit and len(orbit_counts) > 1:
        dominant_orbit = max(orbit_counts, key=lambda k: len(orbit_counts[k]))
        logger.info(f"Auto-selected dominant orbit direction: {dominant_orbit}")
        target_scenes = orbit_counts[dominant_orbit]
    else:
        # 万が一1つの軌道しか見つからなかった場合はそれをそのまま使う
        dominant_orbit = list(orbit_counts.keys())[0]
        target_scenes = orbit_counts[dominant_orbit]

    logger.info(f"Preparing to download {len(target_scenes)} scenes (Orbit: {dominant_orbit}).")

    successful_downloads = 0
    failed_downloads = 0

    for i, scene in enumerate(target_scenes, 1):
        prod_id = scene.product_identifier or scene.stac_id
        logger.info(f"[{i}/{len(target_scenes)}] Downloading scene: {prod_id} (Date: {scene.acquisition_time.strftime('%Y-%m-%d %H:%M')})")
        
        saved_path = client.download_product(prod_id, out_dir)
        
        if saved_path:
            successful_downloads += 1
            logger.info(f"  -> Saved successfully.")
        else:
            failed_downloads += 1
            logger.error(f"  -> Failed to download.")
            
        time.sleep(1) # API制限への配慮

    logger.info("=== Download Summary ===")
    logger.info(f"Total Target Scenes: {len(target_scenes)}")
    logger.info(f"Successfully Downloaded: {successful_downloads}")
    logger.info(f"Failed: {failed_downloads}")
    logger.info(f"Output Directory: {out_dir}")

if __name__ == "__main__":
    main()
