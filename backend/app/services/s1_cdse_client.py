# app/services/s1_cdse_client.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import logging
import requests

from app.core.config import settings


logger = logging.getLogger(__name__)


@dataclass
class S1Scene:
    """
    Sentinel-1 GRD シーンのメタデータ（最低限）

    - stac_id: STAC アイテムの ID（例: ..._COG）
    - product_identifier: 元の L1 GRD SAFE 製品名
        例: S1B_IW_GRDH_1SDV_20180706T084948_20180706T084959_011688_0157FB_C8CD.SAFE
    """

    # STAC アイテム ID (COG 名。UI での表示や STAC への参照用)
    stac_id: str

    # 元の SAFE プロダクト名 (s1:product_identifier)
    product_identifier: Optional[str]

    # 代表的なメタ情報
    acquisition_time: datetime
    orbit_direction: Optional[str]
    relative_orbit: Optional[int]
    platform: Optional[str]
    product_type: Optional[str]

    # ジオメトリ・プロパティ一式
    geometry: Dict[str, Any]
    properties: Dict[str, Any]


class S1CDSEClient:
    """
    Copernicus Data Space Ecosystem (CDSE) の STAC API を叩いて
    Sentinel-1 GRD シーンを検索するクライアント。

    - 認証: OAuth2 client_credentials
    - カタログ: https://stac.dataspace.copernicus.eu/v1/search
      (settings.CDSE_STAC_URL にこの URL を設定しておく)
    """

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self._session = session or requests.Session()

        self._token_url = settings.CDSE_TOKEN_URL
        self._client_id = settings.CDSE_CLIENT_ID
        self._client_secret = settings.CDSE_CLIENT_SECRET

        # settings.CDSE_STAC_URL は search のフルURLを期待
        # 例: https://stac.dataspace.copernicus.eu/v1/search
        self._stac_search_url = settings.CDSE_STAC_URL.rstrip("/")

        # アクセストークンキャッシュ
        self._access_token: Optional[str] = None
        self._token_expire_at: datetime = datetime.min.replace(tzinfo=timezone.utc)

    # ------------------------------------------------------------------
    # 認証関連
    # ------------------------------------------------------------------
    def _get_token(self) -> str:
        now = datetime.now(timezone.utc)
        if self._access_token and now < self._token_expire_at:
            return self._access_token

        if not self._client_id or not self._client_secret:
            raise RuntimeError("CDSE_CLIENT_ID / CDSE_CLIENT_SECRET が設定されていません。")

        data = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        resp = self._session.post(self._token_url, data=data, timeout=30)
        try:
            resp.raise_for_status()
        except Exception:
            logger.error("CDSE token取得に失敗: %s", resp.text[:500])
            raise

        j = resp.json()
        access_token = j["access_token"]
        expires_in = int(j.get("expires_in", 3600))

        # 有効期限の少し前に再取得するようにしておく
        self._access_token = access_token
        self._token_expire_at = now + timedelta(seconds=expires_in - 60)

        logger.debug("CDSE token を取得しました (expires_in=%s sec)", expires_in)
        return access_token

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

    # ------------------------------------------------------------------
    # STAC 検索（GET /v1/search?bbox=...&datetime=...）
    # ------------------------------------------------------------------
    def _stac_search(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        STAC /search を GET で叩く薄いラッパ。
        params はそのままクエリパラメータになる。
        """
        logger.debug("STAC search params: %s", params)

        resp = self._session.get(
            self._stac_search_url,
            headers=self._auth_headers(),
            params=params,
            timeout=60,
        )
        if resp.status_code >= 400:
            logger.error(
                "STAC search failed: %s %s",
                resp.status_code,
                resp.text[:800],
            )
            resp.raise_for_status()

        return resp.json()

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime:
        """
        STAC の datetime (例: '2020-02-18T19:44:09Z') を datetime に変換。
        """
        # Python の fromisoformat は 'Z' を直接読めないので +00:00 に置換
        if dt_str.endswith("Z"):
            dt_str = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(dt_str)

    def _features_to_scenes(self, data: Dict[str, Any]) -> List[S1Scene]:
        scenes: List[S1Scene] = []

        for feat in data.get("features", []):
            props = feat.get("properties", {}) or {}

            # 1) 観測日時を決定
            dt_str = (
                props.get("datetime")
                or props.get("start_datetime")
                or props.get("end_datetime")
            )
            if not dt_str:
                continue

            acq_time = self._parse_datetime(dt_str)

            # 2) SAFE のプロダクト名（本物の L1 GRD）を取得
            #    CDSE の STAC では `s1:product_identifier` に入っている想定
            product_identifier = (
                props.get("s1:product_identifier")
                or props.get("s1:productIdentifier")
            )

            # 3) 製品タイプ (GRD 等)
            product_type = (
                props.get("s1:product_type")
                or props.get("sar:product_type")
                or props.get("productType")
            )

            scenes.append(
                S1Scene(
                    stac_id=feat.get("id", ""),  # 例: ..._COG
                    product_identifier=product_identifier,  # 例: ...C8CD.SAFE
                    acquisition_time=acq_time,
                    orbit_direction=props.get("sat:orbit_state")
                    or props.get("s1:orbitDirection")
                    or props.get("orbitDirection"),
                    relative_orbit=props.get("s1:relativeOrbitNumber")
                    or props.get("sat:relative_orbit"),
                    platform=props.get("platform")
                    or props.get("platformSerialIdentifier"),
                    product_type=product_type,
                    geometry=feat.get("geometry") or {},
                    properties=props,
                )
            )

        # 時系列順でソート
        scenes.sort(key=lambda s: s.acquisition_time)
        return scenes

    # ------------------------------------------------------------------
    # ユーティリティ: 点 + 時間範囲で Sentinel-1 GRD を検索
    # ------------------------------------------------------------------
    def search_grd_point_time(
        self,
        lat: float,
        lon: float,
        start: datetime,
        end: datetime,
        limit: int = 100,
        bbox_margin_deg: float = 0.2,
    ) -> List[S1Scene]:
        """
        指定した点 (lat, lon) を含む bbox と時間範囲で
        Sentinel-1 GRD (sentinel-1-grd) を検索する。

        - bbox は (lon±bbox_margin, lat±bbox_margin) の矩形
        - instrument_mode=IW だけに絞っている
        """

        if start.tzinfo is None or start.tzinfo.utcoffset(start) is None:
            raise ValueError("start は timezone-aware (UTC) にしてください")
        if end.tzinfo is None or end.tzinfo.utcoffset(end) is None:
            raise ValueError("end は timezone-aware (UTC) にしてください")

        lon_min = lon - bbox_margin_deg
        lon_max = lon + bbox_margin_deg
        lat_min = lat - bbox_margin_deg
        lat_max = lat + bbox_margin_deg

        dt_range = (
            start.isoformat().replace("+00:00", "Z")
            + "/"
            + end.isoformat().replace("+00:00", "Z")
        )

        params: Dict[str, Any] = {
            # STAC コレクション ID
            "collections": "sentinel-1-grd",
            # lon_min, lat_min, lon_max, lat_max
            "bbox": f"{lon_min},{lat_min},{lon_max},{lat_max}",
            # 時間範囲
            "datetime": dt_range,
            # 返すアイテム数
            "limit": limit,
            # Sentinel-1 IW モード限定
            "sar:instrument_mode": "IW",
            # 必要なら極化も絞れる（ここでは VV/VH を優先）
            "sar:polarizations": "VV,VH",
        }

        data = self._stac_search(params)
        scenes = self._features_to_scenes(data)
        logger.info(
            "STAC search result: %d scenes (lat=%.3f, lon=%.3f, %s)",
            len(scenes),
            lat,
            lon,
            dt_range,
        )
        return scenes

    # ------------------------------------------------------------------
    # 後続で使う: 「降雨イベントの後の最初のシーン」と「直前シーン」
    # ------------------------------------------------------------------
    def find_after_scene(
        self,
        lat: float,
        lon: float,
        event_end_utc: datetime,
        after_hours: float,
    ) -> Optional[S1Scene]:
        """
        降雨イベント終了時刻 event_end_utc から after_hours 時間の範囲で
        最初に取得される Sentinel-1 GRD シーンを返す。
        """

        start = event_end_utc
        end = event_end_utc + timedelta(hours=after_hours)

        scenes = self.search_grd_point_time(lat, lon, start, end, limit=100)
        for s in scenes:
            if s.acquisition_time >= event_end_utc:
                return s
        return None

    def find_before_scene_unbounded(
        self,
        lat: float,
        lon: float,
        ref_time_utc: datetime,
        mission_start_utc: datetime | None = None,
    ) -> Optional[S1Scene]:
        """
        ref_time_utc より前で「一番直近」の Sentinel-1 GRD シーンを返す。
        時間制限なし（ミッション開始から全部見る）。

        mission_start_utc を省略した場合は 2014-01-01 をデフォルトにする。
        """

        if mission_start_utc is None:
            mission_start_utc = datetime(2014, 1, 1, tzinfo=timezone.utc)

        scenes = self.search_grd_point_time(
            lat,
            lon,
            mission_start_utc,
            ref_time_utc,
            limit=200,  # 必要に応じて増やせる
        )

        before = [s for s in scenes if s.acquisition_time < ref_time_utc]
        if not before:
            return None
        # ref_time_utc に最も近い過去のシーン
        return max(before, key=lambda s: s.acquisition_time)
