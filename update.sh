#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 备份本地配置..."
CONFIG_BACKUP=""
if [ -f config.json ]; then
    CONFIG_BACKUP=$(mktemp /tmp/config.json.XXXXXX)
    cp config.json "$CONFIG_BACKUP"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 配置已备份到 $CONFIG_BACKUP"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 拉取最新代码..."

# 获取当前分支名
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# 拉取远程最新代码
git fetch origin --prune

# 强制重置到远程分支，保证本地代码 100% 被远程覆盖
git reset --hard "origin/${CURRENT_BRANCH}"

# 清理所有未跟踪文件和目录（含 .gitignore 中的文件）
git clean -fdx

# 恢复用户配置
if [ -n "$CONFIG_BACKUP" ] && [ -f "$CONFIG_BACKUP" ]; then
    cp "$CONFIG_BACKUP" config.json
    rm -f "$CONFIG_BACKUP"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 用户配置已恢复"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 重建并重启容器..."
docker compose down
docker compose build --no-cache
docker compose up -d

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 更新完成"
docker compose ps
