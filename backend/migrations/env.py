from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# ★ ここでアプリの設定と Base を読み込む
from app.core.config import settings
from app.db.base import Base

# Alembic Config オブジェクト
config = context.config

# ★ .env の設定から URL を差し替え
config.set_main_option("sqlalchemy.url", settings.sqlalchemy_url)

# ログ設定
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# 自動マイグレーション用 metadata
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Offline モードでマイグレーションを実行."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Online モードでマイグレーションを実行."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()