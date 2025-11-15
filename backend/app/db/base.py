from sqlalchemy.orm import declarative_base

Base = declarative_base()

# モデル定義は app.db.models に書き、
# Alembic から import できるようにここで読み込む
from app.db import models  # noqa: E402,F401
