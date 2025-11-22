# backend/app/services/s1_cdse_client.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Callable
from pathlib import Path
import shutil
import logging
import time
import random  # ★追加
import requests

from app.core.config import settings


logger = logging.getLogger(__name__)


@dataclass
class S1Scene:
    stac_id: str
    product_identifier: Optional[str]
    acquisition_time: datetime
    orbit_direction: Optional[str]
    relative_orbit: Optional[int]
    platform: Optional[str]
    product_type: Optional[str]
    geometry: Dict[str, Any]
    properties: Dict[str, Any]
    assets: Dict[str, Any]


class S1CDSEClient:
    """ Copernicus Data Space Ecosystem (CDSE) Client """

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self._session = session or requests.Session()
        self._token_url = settings.CDSE_TOKEN_URL
        self._client_id = settings.CDSE_CLIENT_ID
        self._client_secret = settings.CDSE_CLIENT_SECRET
        self._stac_search_url = settings.CDSE_STAC_URL.rstrip("/")
        
        self._access_token: Optional[str] = None
        self._token_expire_at: datetime = datetime.min.replace(tzinfo=timezone.utc)

    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._access_token and now < self._token_expire_at:
            return self._access_token

        if not self._client_id or not self._client_secret:
            return ""

        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        try:
            resp = self._session.post(self._token_url, data=data, timeout=30)
            resp.raise_for_status()
            j = resp.json()
            access_token = j["access_token"]
            expires_in = int(j.get("expires_in", 3600))
            self._access_token = access_token
            self._token_expire_at = now + timedelta(seconds=expires_in - 60)
            return access_token
        except Exception as e:
            logger.error(f"Failed to get CDSE token: {e}")
            return ""

    def _auth_headers(self) -> Dict[str, str]:
        token = self._get_token()
        if token:
            return {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
        return {"Accept": "application/json"}

    def _stac_search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # ★修正: リトライロジックの強化
        max_retries = 10  # 回数を増やす
        base_wait = 2.0

        for attempt in range(max_retries):
            try:
                resp = self._session.get(
                    self._stac_search_url,
                    headers=self._auth_headers(),
                    params=params,
                    timeout=60,
                )
                resp.raise_for_status()
                return resp.json()
            
            except requests.exceptions.HTTPError as e:
                # 429 Too Many Requests の場合
                if e.response is not None and e.response.status_code == 429:
                    # Retry-After ヘッダーがあれば従う
                    retry_after = e.response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_time = float(retry_after) + 1.0 # 念のため+1秒
                        except ValueError:
                            wait_time = base_wait * (2 ** attempt)
                    else:
                        # 指数バックオフ + Jitter (ゆらぎ)
                        # Jitterを入れることで、並列スレッドが一斉にリトライして再度刺さるのを防ぐ
                        wait_time = base_wait * (2 ** attempt) + (random.random() * 3.0)
                    
                    logger.warning(f"Rate limit hit (429). Retrying in {wait_time:.1f}s... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                
                # 5xx 系エラーも一時的ならリトライしてもよいが、今回はログを出して終了
                logger.error(f"STAC search error: {e}")
                return {}
            
            except Exception as e:
                logger.error(f"STAC search error: {e}")
                return {}
        
        logger.error("STAC search failed after max retries.")
        return {}

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime:
        if dt_str.endswith("Z"):
            dt_str = dt_str.replace("Z", "+00:00")
        if "." in dt_str:
             main, frac = dt_str.split(".", 1)
             if "+" in frac:
                 frac, tz = frac.split("+", 1)
                 tz = "+" + tz
             else:
                 tz = ""
             dt_str = f"{main}.{frac[:6]}{tz}"
        return datetime.fromisoformat(dt_str)

    def _features_to_scenes(self, data: Dict[str, Any]) -> List[S1Scene]:
        scenes: List[S1Scene] = []
        for feat in data.get("features", []):
            props = feat.get("properties", {}) or {}
            
            dt_str = (props.get("datetime") or props.get("start_datetime") or props.get("end_datetime"))
            if not dt_str: continue
            
            product_identifier = (
                props.get("s1:product_identifier") 
                or props.get("productIdentifier")
                or props.get("identifier")
            )

            scenes.append(S1Scene(
                stac_id=feat.get("id", ""),
                product_identifier=product_identifier,
                acquisition_time=self._parse_datetime(dt_str),
                orbit_direction=props.get("sat:orbit_state") or props.get("s1:orbitDirection") or props.get("orbitDirection"),
                relative_orbit=props.get("s1:relativeOrbitNumber") or props.get("sat:relative_orbit"),
                platform=props.get("platform") or props.get("platformSerialIdentifier"),
                product_type=props.get("s1:product_type") or props.get("productType"),
                geometry=feat.get("geometry") or {},
                properties=props,
                assets=feat.get("assets", {})
            ))
        scenes.sort(key=lambda s: s.acquisition_time)
        return scenes

    def search_grd_point_time(
        self, 
        lat: float, 
        lon: float, 
        start: datetime, 
        end: datetime, 
        limit: int = 100,
        platform: Optional[str] = None,
        orbit_direction: Optional[str] = None,
        relative_orbit: Optional[int] = None
    ) -> List[S1Scene]:
        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None: end = end.replace(tzinfo=timezone.utc)
        
        bbox = f"{lon-0.1},{lat-0.1},{lon+0.1},{lat+0.1}"
        dt_range = f"{start.isoformat().replace('+00:00', 'Z')}/{end.isoformat().replace('+00:00', 'Z')}"
        
        params = {
            "collections": "sentinel-1-grd",
            "bbox": bbox,
            "datetime": dt_range,
            "limit": limit,
            "sar:instrument_mode": "IW",
            "sar:polarizations": "VV,VH",
        }
        
        if platform:
            params["platform"] = platform
        if orbit_direction:
            params["sat:orbit_state"] = orbit_direction.lower()
        if relative_orbit is not None:
            params["sat:relative_orbit"] = relative_orbit

        data = self._stac_search(params)
        scenes = self._features_to_scenes(data)

        filtered_scenes = []
        for s in scenes:
            # Platform厳密チェック
            if platform:
                p_req = platform.lower().replace("-", "")
                p_act = (s.platform or "").lower().replace("-", "")
                if p_req not in p_act and p_act not in p_req:
                    continue
            
            # Orbit Direction厳密チェック
            if orbit_direction:
                o_req = orbit_direction.lower()
                o_act = (s.orbit_direction or "").lower()
                if o_req != o_act:
                    continue
                
            # Relative Orbit厳密チェック
            if relative_orbit is not None:
                if s.relative_orbit != relative_orbit:
                    continue
            
            filtered_scenes.append(s)
            
        return filtered_scenes

    def find_after_scene(self, lat: float, lon: float, event_end_utc: datetime, after_hours: float) -> Optional[S1Scene]:
        start = event_end_utc
        end = event_end_utc + timedelta(hours=after_hours)
        scenes = self.search_grd_point_time(lat, lon, start, end)
        for s in scenes:
            if s.acquisition_time >= event_end_utc:
                return s
        return None

    def find_before_scene_unbounded(
        self, 
        lat: float, 
        lon: float, 
        ref_time_utc: datetime,
        platform: Optional[str] = None,
        orbit_direction: Optional[str] = None,
        relative_orbit: Optional[int] = None
    ) -> Optional[S1Scene]:
        """
        指定された条件 (Platform, Orbit等) に合致する、ref_time_utc より前の「直近」のシーンを探す。
        APIのlimit制限を回避するため、期間を区切って段階的に過去へ遡る。
        """
        
        # 検索範囲のステップ (日): 1ヶ月 -> 3ヶ月 -> 1年 -> 5年 -> 全期間
        lookback_steps = [30, 90, 365, 365*5, 365*10]
        
        for days_back in lookback_steps:
            start = ref_time_utc - timedelta(days=days_back)
            
            # ref_time_utc までを検索 (limit=200あれば、30日分なら十分入るはず)
            scenes = self.search_grd_point_time(
                lat, lon, start, ref_time_utc, 
                limit=200,
                platform=platform,
                orbit_direction=orbit_direction,
                relative_orbit=relative_orbit
            )
            
            # 条件に合い、かつ ref_time より前のもの
            before_candidates = [s for s in scenes if s.acquisition_time < ref_time_utc]
            
            if before_candidates:
                # 見つかった中で最も新しいもの（直近）を返す
                return max(before_candidates, key=lambda s: s.acquisition_time)
            
            # 見つからなければループ継続（検索範囲を広げる）
            # logger.debug(f"No before scene found within {days_back} days. Extending search...")

        return None

    # ... (ダウンロード系メソッドは変更なし) ...
    def _normalize_product_name(self, name: str) -> str:
        if name.endswith("_COG"):
            name = name[:-4]
        if name.endswith(".SAFE"):
            name = name[:-5]
        return name

    def _get_odata_id_by_name(self, product_name: str) -> Optional[str]:
        name_stem = self._normalize_product_name(product_name)
        url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        
        filter_exact = f"Name eq '{name_stem}.SAFE' or Name eq '{name_stem}'"
        filter_fuzzy = f"contains(Name, '{name_stem}')"

        for filter_query in [filter_exact, filter_fuzzy]:
            params = {
                "$filter": filter_query,
                "$top": 1,
                "$orderby": "ContentDate/Start desc"
            }
            try:
                resp = self._session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                values = resp.json().get("value", [])
                if values:
                    return values[0].get("Id")
            except Exception:
                continue
        
        logger.error(f"OData product not found for: {product_name}")
        return None

    def download_product(self, product_identifier: str, output_dir: Path, progress_callback: Optional[Callable[[int, int], bool]] = None) -> Optional[Path]:
        if not product_identifier:
            return None
        
        if not self._get_token():
            logger.error("Cannot download: No CDSE credentials provided.")
            return None

        uuid = self._get_odata_id_by_name(product_identifier)
        if not uuid:
            return None

        download_url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value"
        
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_name = self._normalize_product_name(product_identifier) + ".zip"
        save_path = output_dir / safe_name

        if save_path.exists() and save_path.stat().st_size > 0:
            logger.info(f"Skipping existing file: {save_path}")
            if progress_callback:
                progress_callback(100, 100)
            return save_path

        logger.info(f"Downloading to: {save_path}")
        try:
            with self._session.get(download_url, headers=self._auth_headers(), stream=True, timeout=120) as r:
                r.raise_for_status()
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0
                
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if progress_callback and total_size > 0:
                                if progress_callback(downloaded, total_size) is False:
                                    raise InterruptedError("Cancelled")
                                
            return save_path
        except InterruptedError:
            logger.info(f"Download cancelled: {safe_name}")
            if save_path.exists(): save_path.unlink()
            return None
        except Exception as e:
            logger.error(f"Download failed: {e}")
            if save_path.exists(): save_path.unlink()
            return None