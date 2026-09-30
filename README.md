# Twitter Ero Video Ranking Downloader

按播放量从远端视频排行榜自动下载视频，支持日/周/月/总榜切换，提供 Web 界面管理配置、查看日志、浏览远程视频和本地视频。

[![Docker Pulls](https://img.shields.io/docker/pulls/hexbkyoma/twitter-ero-video-ranking-downloader)](https://hub.docker.com/r/hexbkyoma/twitter-ero-video-ranking-downloader)

## 功能

- **定时下载** — 按榜单范围定时下载排行靠前的视频，可一键关闭
- **远程视频** — 在线浏览远端排行视频，可设置每页数量，勾选一键下载
- **本地视频** — 浏览本地视频，媒体根目录可独立配置；支持 `.strm` 引用条目播放、预览播放、替换封面、批量删除、服务端分页与排序
- **封面刮削** — 用 ffmpeg 抽取视频首帧生成同名 jpg 封面，已有封面自动跳过
- **Web 界面** — 所有操作通过浏览器完成，端口 `2617`

## 配置

配置文件为程序同目录下的 `config.json`，首次运行自动生成：

| 字段 | 说明 | 默认值 |
|---|---|---|
| `download_root` | 视频下载根目录，绝对路径 | `/data/downloads` |
| `poster_root` | 本地视频媒体根目录（设置页"本地视频"中配置），留空回退 `download_root` | 空 |
| `proxy` | HTTP 代理地址，格式 `http://IP:端口`，空则直连 | 空 |
| `auto_download_enabled` | 是否开启定时自动下载 | `true` |
| `schedule_cron` | 定时执行 cron 表达式（5 位） | `0 3 * * *` |
| `max_daily_downloads` | 每次执行最大下载数量 | `10` |
| `ranking_range` | 榜单范围：`daily` 日榜 / `weekly` 周榜 / `monthly` 月榜 / `all` 总榜 | `daily` |
| `waterfall_per_page` | 远程视频每页展示数量，可选 `10/20/30/50/100` | `10` |

> 配置采用白名单制：仅保留上述键，历史版本遗留的其他键会在下次保存时自动清除。

## 部署

### Docker（推荐，支持 amd64 / arm64）

```bash
docker pull hexbkyoma/twitter-ero-video-ranking-downloader:latest
docker compose up -d
```

`docker-compose.yml` 默认映射：

| 容器路径 | 主机路径 | 说明 |
|---|---|---|
| `/app/config.json` | `./config.json` | 配置文件持久化 |
| `/data/downloads` | `./nas_downloads` | 视频下载目录 |

> `config.json` 中的 `download_root` 应设置为**容器内**路径 `/data/downloads`，而非主机路径。
> 如需把本地视频指向独立媒体库，为 `poster_root` 额外挂载一个容器路径（如 `/data/media`）并在设置页填写该容器内路径。

管理命令：

```bash
docker compose up -d                          # 启动
docker compose down                           # 停止
docker compose logs -f                        # 查看日志
docker compose pull && docker compose up -d   # 更新
bash update.sh                                # 或一键更新（同步部署文件 + 拉取镜像 + 重启）
```

### 源码运行（Windows / Linux）

```bash
git clone https://github.com/你的用户名/twitter-ero-video-ranking-downloader.git
cd twitter-ero-video-ranking-downloader

python -m venv venv && source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 本地视频时长探测与封面刮削依赖 ffmpeg（含 ffprobe）
sudo apt install ffmpeg   # Windows 从 ffmpeg.org 下载后加入 PATH

python main.py
```

访问 `http://localhost:2617`。长期运行可用 systemd 等托管 `main.py`。
打包 Windows 单文件 EXE：`pip install pyinstaller && python build_exe.py`，产物在 `dist\` 下。

### 群晖 NAS / 离线部署

- **群晖 DSM**：容器管理器 → 注册表搜索镜像名下载 `latest`（或从文件导入离线镜像），端口映射 `2617:2617`，配置文件映射到 `/app/config.json`，环境变量 `TZ=Asia/Shanghai`。CPU 架构在控制面板 → 信息中心查看（Intel 系列选 amd64，Realtek/ARM 系列选 arm64）
- **完全离线**：GitHub Actions（Export Docker Image）按架构构建后，从该次运行的 Artifacts 下载离线包，`gunzip` 后 `docker load` 导入

## 常见问题

### Q: 视频时长显示 `--:--`？

时长探测依赖 `ffprobe`（ffmpeg 套件），确保运行环境已安装 ffmpeg。Docker 镜像已内置。

### Q: 如何把本地视频指向自己的媒体库？

**任务页 → 设置 → 本地视频 → 媒体根目录** 填写目录路径，保存后立即扫描；留空则使用视频下载根目录。该目录下的一级子文件夹会作为可筛选的分类。

### Q: `.strm` 文件是什么？如何播放？

`.strm` 是媒体库常用的"引用型"文本文件，内容只有一行——真实媒体的位置。本地视频会把 `.strm` 当作视频条目收录，播放时读取其内容：

- 内容为 `http(s)` 链接 → 服务端 302 重定向，浏览器播放器直接拉流
- 内容为本地文件路径（支持相对路径与 UNC 路径如 `\\NAS\share\a.mp4`）→ 服务端读取该文件供流，相对路径按 `.strm` 所在目录展开
- 同名图片文件（如 `Movie.jpg`）会作为其封面

> `.strm` 指向的远程链接需可直接访问（302/直链），HLS（m3u8）直链在部分浏览器上无法原生播放；Docker 部署时 strm 内的本地路径指的应是**容器内**可见的路径。

### Q: 封面刮削是什么？怎么用？

本地视频工具栏 **刮削封面** 按钮，按当前文件夹筛选范围执行：遍历视频文件（`.strm` 引用条目除外）→ ffmpeg 抽取首帧 → 同目录生成同名 jpg（如 `a.mp4` → `a.jpg`）。已有封面的自动跳过，**不会覆盖**手动设置的封面；作业后台执行，页面轮询进度，失败原因保留在作业状态里。

### Q: 容器启动报 `IsADirectoryError: '/app/config.json.tmp' -> '/app/config.json'`？

部署目录里没有 `config.json` 文件就直接 `docker compose up` 时，Docker 会先在宿主机创建一个同名**目录**挂载进容器，旧版本会因此崩溃循环。新版本已内置自愈：自动把配置写到该目录内的 `config.json`，拉取最新镜像重启即可。建议首次部署先创建好 `config.json` 文件。

## 技术栈

- **后端**：Python 3.11 + FastAPI + APScheduler
- **前端**：Vue 3 + Element Plus + Vite
- **容器**：Docker（多架构 amd64 + arm64）；PyInstaller 打包 Windows EXE

## License

MIT
