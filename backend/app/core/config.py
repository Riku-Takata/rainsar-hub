from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MySQL 接続設定
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 3306
    DB_USER: str = "rainsar"
    DB_PASSWORD: str = "rainsar_pw"
    DB_NAME: str = "rainsar_hub"

    # GSMAP バイナリのルートディレクトリ（コンテナ内パス or ローカル開発用）
    GSMAP_DATA_ROOT: str = "/data/gsmap"

    class Config:
        env_file = ".env"          # ローカル開発用（Docker 外で走らせるとき）
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