# Twitter Ero Video Ranking Downloader

[![Docker Pulls](https://img.shields.io/docker/pulls/hexbkyoma/twitter-ero-video-ranking-downloader)](https://hub.docker.com/r/hexbkyoma/twitter-ero-video-ranking-downloader)

从 [truvaze.com](https://truvaze.com/api/media) 按排行自动下载视频，支持按分类、时长、排序方式筛选。提供 Web 界面管理配置、查看日志、浏览瀑布流预览和本地海报墙。

## 功能概览

- **定时下载** — 按配置的排序/范围/时长/分类自动下载，支持每日定时执行，也可一键关闭自动下载
- **分类管理** — 可配置多个标签分类，每个分类独立下载到对应子文件夹
- **瀑布流预览** — 在线浏览远端排行视频，支持独立设置每页数量/排序/时间范围/时长筛选，选择后一键下载
- **海报墙** — 浏览本地已下载的视频，支持预览播放、替换封面、批量删除
- **Web 界面** — 所有操作通过浏览器完成，端口 `2617`

## 配置说明

配置文件为程序同目录下的 `config.json`，首次运行会自动生成默认配置：

```json
{
  "download_root": "/data/downloads",
  "proxy": "http://192.168.1.13:20171",
  "auto_download_enabled": true,
  "schedule_time": "03:00",
  "max_daily_downloads": 10,
  "sort": "pv",
  "range": "daily",
  "min_time": 0,
  "max_time": 86400,
  "time_filter_unit": "seconds",
  "tag_codes": [],
  "waterfall_per_page": 10,
  "waterfall_sort": "pv",
  "waterfall_range": "daily",
  "waterfall_min_time": 0,
  "waterfall_max_time": 86400
}
```

| 字段 | 说明 | 默认值 |
|---|---|---|
| `download_root` | 视频下载的根目录，绝对路径 | `/data/downloads` |
| `proxy` | HTTP 代理地址，为空则不使用代理 | `http://192.168.1.13:20171` |
| `auto_download_enabled` | 是否开启每日自动下载，关闭后仅保留手动下载 | `true` |
| `schedule_time` | 每日定时执行时间，格式 `HH:MM` | `03:00` |
| `max_daily_downloads` | 每个分类每日最大下载数量 | `10` |
| `sort` | 排序方式：`pv` 播放量 / `favorite` 点赞 / `time` 时长 / `created` 最近添加 | `pv` |
| `range` | 时间范围：`daily` 每日 / `weekly` 每周 / `monthly` 每月 / `all` 全部 | `daily` |
| `min_time` | 最短时长（秒），`0` 表示不限制 | `0` |
| `max_time` | 最长时长（秒），`86400` 表示不限制 | `86400` |
| `tag_codes` | 要下载的分类标签列表，为空则只下载无标签分类 | `[]` |
| `waterfall_per_page` | 瀑布流每页展示数量，可选 `10/20/30/50/100` | `10` |
| `waterfall_sort` | 瀑布流排序方式，独立于下载页设置 | `pv` |
| `waterfall_range` | 瀑布流时间范围，独立于下载页设置 | `daily` |
| `waterfall_min_time` | 瀑布流最短时长（秒） | `0` |
| `waterfall_max_time` | 瀑布流最长时长（秒） | `86400` |

---

## 部署方式

### 方式一：Windows 直接运行

适用于 Windows 桌面/服务器环境。

#### 1. Python 源码运行

```bash
# 克隆项目
git clone https://github.com/你的用户名/twitter-ero-video-ranking-downloader.git
cd twitter-ero-video-ranking-downloader

# 创建虚拟环境（推荐）
python -m venv venv
venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt

# 运行
python main.py
```

打开浏览器访问 `http://localhost:2617` 即可使用。

#### 2. 打包为 EXE 单文件

使用 PyInstaller 打包为独立可执行文件，无需安装 Python：

```bash
# 安装 PyInstaller
pip install pyinstaller

# 打包（也可直接运行 build_exe.py）
python build_exe.py
```

打包完成后，`dist\twitter-downloader.exe` 即为独立可执行文件。

运行方式：

```bash
# 直接双击运行，或在命令行中：
twitter-downloader.exe
```

> `config.json` 会自动生成在 EXE 同目录下，修改配置后重启程序生效。

---

### 方式二：Linux 直接运行

适用于 x86_64 Linux 服务器（Ubuntu、Debian、CentOS 等）。

#### 1. Python 源码运行

```bash
# 克隆项目
git clone https://github.com/你的用户名/twitter-ero-video-ranking-downloader.git
cd twitter-ero-video-ranking-downloader

# 创建虚拟环境（推荐）
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 安装 ffmpeg（海报墙时长探测需要 ffprobe）
sudo apt install ffmpeg      # Ubuntu/Debian
sudo yum install ffmpeg      # CentOS（可能需要 EPEL 或 RPM Fusion）

# 运行
python main.py
```

#### 2. systemd 服务（推荐长期运行）

创建 systemd 服务文件，实现开机自启和崩溃自动重启：

```bash
sudo nano /etc/systemd/system/twitter-downloader.service
```

写入以下内容（根据实际路径修改）：

```ini
[Unit]
Description=Twitter Ero Video Ranking Downloader
After=network.target

[Service]
Type=simple
User=你的用户名
WorkingDirectory=/home/你的用户名/twitter-ero-video-ranking-downloader
ExecStart=/home/你的用户名/twitter-ero-video-ranking-downloader/venv/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable twitter-downloader
sudo systemctl start twitter-downloader

# 查看状态
sudo systemctl status twitter-downloader

# 查看日志
journalctl -u twitter-downloader -f
```

---

### 方式三：Docker 容器化运行（支持 ARM）

适用于所有 Docker 环境，包括 x86_64 和 ARM64（ARM NAS、树莓派等）。

#### 0. 直接拉取镜像（推荐）

已构建并发布到 Docker Hub，支持 `linux/amd64` 和 `linux/arm64` 多架构：

```bash
docker pull hexbkyoma/twitter-ero-video-ranking-downloader:latest
```

#### 1. 在线构建

适合需要从源码自定义构建的场景（大多数用户可直接用上面的 `docker pull`）：

```bash
# 克隆项目
git clone https://github.com/你的用户名/twitter-ero-video-ranking-downloader.git
cd twitter-ero-video-ranking-downloader

# 准备配置文件（首次运行会自动生成默认配置，也可手动创建）
# 编辑 config.json，将 download_root 改为容器内的挂载路径：
#   "download_root": "/data/downloads"

# 从源码构建镜像
docker build -t hexbkyoma/twitter-ero-video-ranking-downloader:latest .

# 启动
docker compose up -d
```

> Docker 镜像基于 `python:3.11-slim`，同时支持 `linux/amd64` 和 `linux/arm64` 架构。
> 在 ARM 设备（如群晖 NAS、树莓派）上构建时会自动使用 ARM64 基础镜像，无需额外配置。

#### 2. 离线镜像导入

适合无网络或网络受限的环境（如内网 NAS）：

从 GitHub Releases 下载对应架构的离线镜像包：

| 架构 | 文件名 |
|---|---|
| x86_64 (amd64) | `twitter-ero-video-ranking-downloader-版本号-offline-amd64.tar.gz` |
| ARM64 (arm64) | `twitter-ero-video-ranking-downloader-版本号-offline-arm64.tar.gz` |

导入并启动：

```bash
# 解压并加载镜像
gunzip twitter-ero-video-ranking-downloader-latest-offline-arm64.tar.gz
docker load -i twitter-ero-video-ranking-downloader-latest-offline-arm64.tar

# 给镜像打上 Docker Hub 标签（docker-compose.yml 使用此镜像名）
docker tag twitter-ero-video-ranking-downloader:latest-arm64 hexbkyoma/twitter-ero-video-ranking-downloader:latest

# 准备配置文件和下载目录
mkdir -p nas_downloads
# 编辑 config.json，将 download_root 改为容器内路径：
#   "download_root": "/data/downloads"

# 启动
docker compose up -d
```

#### 3. 配置与目录映射

`docker-compose.yml` 默认映射：

| 容器路径 | 主机路径 | 说明 |
|---|---|---|
| `/app/config.json` | `./config.json` | 配置文件持久化 |
| `/data/downloads` | `./nas_downloads` | 视频下载目录 |

配置文件中的 `download_root` 应设置为容器内路径 `/data/downloads`，而非主机路径。

如需映射到 NAS 其他目录，修改 `docker-compose.yml` 中的 `volumes`：

```yaml
volumes:
  - ./config.json:/app/config.json
  - /vol1/1000/AdultMedia/tw:/data/downloads   # 改为你的 NAS 实际路径
```

#### 4. 管理命令

```bash
# 启动
docker compose up -d

# 停止
docker compose down

# 查看日志
docker compose logs -f

# 更新镜像
docker compose down
docker compose pull
docker compose up -d
```

---

### 方式四：群晖 NAS Docker 部署

适用于 Synology DSM 界面操作。

#### 1. 获取镜像

**方式 A：从 Docker Hub 拉取（推荐，有网络时）**

DSM → **容器管理器** → **注册表** → 搜索 `hexbkyoma/twitter-ero-video-ranking-downloader` → 下载 `latest` 标签。

或通过 SSH：

```bash
docker pull hexbkyoma/twitter-ero-video-ranking-downloader:latest
```

**方式 B：离线镜像导入（无网络时）**

1. 从 GitHub Releases 下载 **`arm64`** 架构的离线镜像包（大部分群晖为 ARM64；部分高端型号为 x86_64，请确认你的架构）
2. DSM → **容器管理器** → **注册表** → **添加** → 从文件导入 `.tar` 镜像
3. 或通过 SSH 命令行导入：

```bash
gunzip twitter-ero-video-ranking-downloader-latest-offline-arm64.tar.gz
docker load -i twitter-ero-video-ranking-downloader-latest-offline-arm64.tar
```

#### 2. 创建容器

1. DSM → **容器管理器** → **创建容器**
2. 选择导入的 `twitter-ero-video-ranking-downloader:latest` 镜像
3. 端口映射：主机 `2617` → 容器 `2617`
4. 存储空间映射：
   - 主机配置文件路径 → 容器 `/app/config.json`（文件级映射）
   - 主机下载目录（如 `/vol1/1000/AdultMedia/tw`） → 容器 `/data/downloads`
5. 环境变量：`TZ=Asia/Shanghai`
6. 勾选 **自动重启**

#### 3. 配置

确保 `config.json` 中 `download_root` 设置为容器内路径 `/data/downloads`：

```json
{
  "download_root": "/data/downloads",
  "proxy": "http://192.168.1.13:20171",
  ...
}
```

---

## 常见问题

### Q: 下载目录在哪里？

**直接运行模式**：`config.json` 中 `download_root` 设置为主机绝对路径（如 `/vol1/1000/AdultMedia/tw` 或 `D:\videos`）。

**Docker 模式**：`config.json` 中 `download_root` 设置为容器内路径 `/data/downloads`，实际文件通过 volume 映射存到主机目录。

### Q: 如何设置代理？

在 `config.json` 中填写 `proxy` 字段，格式为 `http://IP:端口`，如 `http://192.168.1.100:7897`。Docker 容器内如需使用宿主机代理，可填写宿主机 IP（不能填 `127.0.0.1`，因为容器内 localhost 不是宿主机）。

### Q: 海报墙视频时长显示 `--:--`？

时长探测依赖 `ffprobe`（属于 ffmpeg）。请确保运行环境已安装 ffmpeg：

- **Linux**：`sudo apt install ffmpeg`
- **Docker**：镜像已内置 ffmpeg，无需额外安装
- **Windows**：从 [ffmpeg.org](https://ffmpeg.org/download.html) 下载并将 `ffprobe.exe` 放到 PATH 或程序同目录

### Q: 群晖 NAS 如何确认 CPU 架构？

DSM → **控制面板** → **信息中心**，查看 CPU 型号。常见对应：

| CPU 系列 | 架构 |
|---|---|
| Intel (J、N、+ 系列) | x86_64 → 下载 **amd64** 镜像 |
| Realtek (RTD129x) | ARM64 → 下载 **arm64** 镜像 |
| ARM Annapurna Labs | ARM64 → 下载 **arm64** 镜像 |

### Q: 如何更新版本？

- **直接运行**：`git pull` 后重启程序
- **Docker**：`bash update.sh` 或手动 `docker compose down && docker compose build --no-cache && docker compose up -d`
- **群晖 DSM**：删除旧容器 → 导入新镜像 → 重新创建容器

## 技术栈

- **后端**：Python 3.11 + FastAPI + APScheduler
- **前端**：原生 HTML/CSS/JS（Jinja2 模板）
- **容器**：Docker（多架构：amd64 + arm64）
- **打包**：PyInstaller（Windows 单文件 EXE）

## License

MIT
