from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MySQL 接続設定
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 3306
    DB_USER: str = "rainsar"
    DB_PASSWORD: str = "rainsar_pw"
    DB_NAME: str = "rainsar_hub"

    class Config:
        env_file = ".env"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"mysql+pymysql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )


settings = Settings()
