FROM python:3.12-slim

WORKDIR /app

# MySQL クライアント & ビルドツール（必要に応じて）
RUN apt-get update && apt-get install -y \
  build-essential default-libmysqlclient-dev pkg-config \
  libexpat1 \
  && rm -rf /var/lib/apt/lists/*

# ここから下は "context: ../backend" を前提に、
# backend ディレクトリ直下のファイルとして扱う
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app
ENV PYTHONPATH=/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
