# Twitter 视频下载器 (即时下载版)

一个现代化的网页端 Twitter 视频下载工具。支持灵活配置筛选条件、即时启动下载任务，并实时查看下载进度和日志。

## 特性

- **现代化配置界面**：一页完成配置和监控，摒弃繁杂的后台定时任务。
- **实时进度反馈**：直观的进度条、各状态统计（成功/跳过/失败）以及实时滚动日志。
- **灵活筛选下载**：支持按标签、排序（观看数、点赞、最新）、时间范围及视频时长筛选。
- **海报墙管理**：内置精美的视频海报墙页面，可对已下载的视频进行浏览、批量删除或替换封面。
- **配置持久化**：用户设置的下载参数会自动保存，下次打开无需重复配置。

## 启动方式

### 环境准备

- Python 3.9+
- 依赖安装：`pip install -r requirements.txt`

### 运行服务

```bash
python main.py
```

服务默认在 `http://0.0.0.0:2617` 运行。在浏览器中打开 `http://localhost:2617` 即可访问主界面。

## API 文档

### `GET /api/config`
获取当前已保存的下载配置。

### `POST /api/config`
保存用户配置。
- 请求体：JSON 格式的配置项
- 响应：包含校验后的配置

### `POST /api/download/start`
启动下载任务。
- 请求体：
  - `config`: 下载配置字典
  - `persist`: `boolean`，是否同时持久化保存此配置，默认为 true
- 返回：任务的初始状态信息。如果当前已有任务在运行，将返回 409 冲突。

### `GET /api/download/status`
获取当前的下载任务状态及日志。
- 返回：
  - `state`: 当前进度状态（进度、成功数、失败数、当前处理项等）
  - `logs`: 最新日志数组

### 海报墙 API
- `GET /api/poster`：获取所有已下载视频信息。
- `GET /api/poster/{date}`：获取指定日期的视频信息。
- `POST /api/poster/{date}/batch-delete`：批量删除指定视频。
- `POST /api/poster/{date}/{stem}/replace-cover`：替换指定视频的封面图。

## 目录结构
- `main.py`：后端主程序
- `templates/index.html`：下载配置与任务控制台页面
- `templates/poster.html`：海报墙展示页面
- `tests/`：单元测试目录
