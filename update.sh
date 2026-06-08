#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 拉取最新代码..."

# 获取当前分支名
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# 拉取远程最新代码
git fetch origin --prune

# 强制重置到远程分支，保证本地代码 100% 被远程覆盖
git reset --hard "origin/${CURRENT_BRANCH}"

# 清理所有未跟踪文件和目录（含 .gitignore 中的文件）
git clean -fdx

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 重建并重启容器..."
docker compose down
docker compose build --no-cache
docker compose up -d

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 更新完成"
docker compose ps
