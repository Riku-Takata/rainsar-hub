#!/usr/bin/env bash
set -u
echo "===== RainSAR Hub Environment Check ====="
echo "Project dir: $(pwd)"
echo

# ヘルパー関数
check_cmd() {
  local name="$1"
  local cmd="$2"
  echo "- $name:"
  if command -v $cmd >/dev/null 2>&1; then
    $cmd --version 2>/dev/null || $cmd -v 2>/dev/null || echo "  (installed, but version command failed)"
  else
    echo "  NOT FOUND"
  fi
  echo
}

echo "### Git / Docker / Node / Python / pyenv ###"
check_cmd "git" git
check_cmd "docker" docker
check_cmd "docker compose" "docker compose"
check_cmd "node" node
check_cmd "npm" npm
check_cmd "npx" npx
check_cmd "python" python
check_cmd "pyenv" pyenv

echo "### Backend ディレクトリの確認 ###"
if [ -d "backend" ]; then
  echo "- backend/ は存在します。"
  if [ -f "backend/requirements.txt" ]; then
    echo "  backend/requirements.txt: OK"
  else
    echo "  backend/requirements.txt: 見つかりませんでした。"
  fi
  if [ -d "backend/.venv" ]; then
    echo "  backend/.venv: 存在します。バックエンド用の仮想環境があるようです。"
  else
    echo "  backend/.venv: 見つかりませんでした。（まだ仮想環境を作っていない可能性があります）"
  fi
else
  echo "- backend/ ディレクトリがありません。 (まだ作っていないならこのメッセージでOK)"
fi
echo

echo "### Frontend ディレクトリの確認 ###"
if [ -d "frontend" ]; then
  echo "- frontend/ は存在します。"
  if [ -f "frontend/package.json" ]; then
    echo "  frontend/package.json: OK"
  else
    echo "  frontend/package.json: 見つかりませんでした。"
  fi
else
  echo "- frontend/ ディレクトリがありません。 (まだ Next.js を作っていないならこのメッセージでOK)"
fi
echo

echo "### Docker コンテナの確認 (起動していれば) ###"
if command -v docker >/dev/null 2>&1; then
  docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}" || echo "docker ps に失敗しました。"
else
  echo "docker コマンドが見つからないため、コンテナ状況は確認できません。"
fi

echo
echo "===== Check finished. ====="

