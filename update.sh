#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 拉取最新代码..."
git fetch --all --prune
UPSTREAM="$(git rev-parse --abbrev-ref --symbolic-full-name @{u})"
git clean -fd
git reset --hard "$UPSTREAM"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 重建并重启容器..."
docker compose down
docker compose build --no-cache
docker compose up -d

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 更新完成"
docker compose ps
