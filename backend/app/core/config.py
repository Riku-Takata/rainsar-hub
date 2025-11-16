# app/core/config.py
from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MySQL
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 3306
    DB_USER: str = "rainsar"
    DB_PASSWORD: str = "rainsar_pw"
    DB_NAME: str = "rainsar_hub"

    # GSMAP バイナリのルート
    GSMAP_DATA_ROOT: str = "/data/gsmap"

    # Copernicus Data Space (CDSE) 認証＆STAC
    CDSE_CLIENT_ID: str | None = None
    CDSE_CLIENT_SECRET: str | None = None
    CDSE_TOKEN_URL: str = (
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    )
    # ここは「search のフルURL」を渡す想定
    CDSE_STAC_URL: str = "https://stac.dataspace.copernicus.eu/v1/search"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @property
    def gsmap_data_path(self) -> Path:
        return Path(self.GSMAP_DATA_ROOT)


settings = Settings()
