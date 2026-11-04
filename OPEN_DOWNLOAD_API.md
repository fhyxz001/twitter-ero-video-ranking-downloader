# 对外开放下载接口文档

通过该接口，外部系统可以把任意 `http/https` 直链交给本项目下载到指定的文件路径。接口采用异步任务模式：提交后立即返回 `task_id`，再用状态接口查询进度与结果，避免大文件下载把调用方拖到超时。

- 服务端口：`2617`
- Base URL：`http://<host>:2617`
- 鉴权方式：请求体中的 `key` 字段，与设置页的「开放下载 Key」比对
- 下载行为：复用项目的 HTTP 代理配置，失败自动重试（共 2 次尝试）

---

## 0. 启用前提

1. key 的默认值为 `912bdbc4a3354d5d86843309e3bc52b7`，开箱即用，无需额外配置。
2. 如需更换，在 Web 界面「任务 → 任务设置」中填写 **开放下载 Key**，点击右上角「保存设置」。
3. 修改后的 key 会写入 `config.json` 的 `open_api_key` 字段。

> `open_api_key` 为空时，提交接口一律返回 `401`，相当于该接口未启用。key 仅做字符串比对，请使用足够长的随机串。

---

## 1. 提交下载任务

```http
POST /api/open/download
Content-Type: application/json
```

### 请求体

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `url` | string | 是 | 下载链接，必须是 `http` 或 `https` 直链 |
| `path` | string | 是 | 下载地址：可以是完整文件路径，也可以是已存在的目录 |
| `key` | string | 是 | 开放下载 Key，与设置页中的值一致 |

### `path` 的解析规则

| 传入形式 | 处理方式 |
|---|---|
| 绝对路径（如 `/data/downloads/a.mp4`、`D:\Downloads\a.mp4`） | 原样使用 |
| 相对路径（如 `sub/a.mp4`） | 基于配置中的下载根目录 `download_root` 展开 |
| 已存在的目录（如 `/data/downloads`） | 追加链接中的文件名；链接无文件名时用 `download.mp4` |
| 父目录不存在 | 自动逐级创建 |

> 目标文件已存在时会**直接覆盖**。key 持有者可写入任意绝对路径，请只在可信环境开启该接口。

### 成功响应

`HTTP 200`，`Content-Type: application/json`

```json
{
  "ok": true,
  "task": {
    "task_id": "52eafaf33b7b4806aac7e5e6e665f139",
    "url": "https://example.com/a.mp4",
    "path": "/data/downloads/a.mp4",
    "status": "pending",
    "error": null,
    "size": null,
    "created_at": "2026-11-04 16:38:48",
    "finished_at": null
  }
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `task_id` | string | 任务 ID（32 位十六进制），用于查询状态 |
| `url` | string | 实际提交的下载链接 |
| `path` | string | 解析后的**绝对**目标文件路径 |
| `status` | string | 任务状态，提交瞬间为 `pending` |
| `error` | string \| null | 失败原因，成功或未完成时为 `null` |
| `size` | int \| null | 下载完成后的文件字节数，未完成时为 `null` |
| `created_at` | string | 任务创建时间（服务器本地时间 `YYYY-MM-DD HH:MM:SS`） |
| `finished_at` | string \| null | 结束时间，未完成时为 `null` |

### 错误响应

```json
{
  "ok": false,
  "error": "下载链接必须是 http/https 直链"
}
```

| HTTP 状态码 | 场景 | `error` 示例 |
|---|---|---|
| `401` | key 错误，或设置中未配置 key | `key 无效或未在设置中启用` |
| `400` | `url` 为空或不是 `http/https` | `下载链接必须是 http/https 直链` |
| `400` | `path` 为空 | `下载路径不能为空` |
| `400` | 目标目录创建失败（无权限、路径非法等） | `无法创建目标目录：...` |
| `422` | 缺少 `url` / `path` 等必填字段（FastAPI 自动返回） | - |

### 示例

```bash
curl -X POST "http://127.0.0.1:2617/api/open/download" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com/a.mp4",
    "path": "/data/downloads/a.mp4",
    "key": "your-key"
  }'
```

---

## 2. 查询任务状态

```http
GET /api/open/download/{task_id}
```

### 路径参数

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `task_id` | string | 是 | 提交接口返回的任务 ID |

### 成功响应

`HTTP 200`，`Content-Type: application/json`

```json
{
  "ok": true,
  "task": {
    "task_id": "52eafaf33b7b4806aac7e5e6e665f139",
    "url": "https://example.com/a.mp4",
    "path": "/data/downloads/a.mp4",
    "status": "success",
    "error": null,
    "size": 15728640,
    "created_at": "2026-11-04 16:38:48",
    "finished_at": "2026-11-04 16:39:02"
  }
}
```

### 任务状态

| `status` | 说明 |
|---|---|
| `pending` | 已登记，等待后台线程开始 |
| `running` | 正在下载 |
| `success` | 下载完成，`size` 为实际字节数 |
| `failed` | 下载失败，`error` 为失败原因（上游 HTTP 错误、超时、写盘失败等） |

### 错误响应

| HTTP 状态码 | 场景 | `error` |
|---|---|---|
| `404` | 任务不存在、已过期或 `task_id` 错误 | `任务不存在或已过期` |

> 状态查询只凭 `task_id`，不校验 key。`task_id` 为随机 32 位十六进制，不要对外暴露进度链接。

### 示例

```bash
curl "http://127.0.0.1:2617/api/open/download/52eafaf33b7b4806aac7e5e6e665f139"
```

---

## 3. 完整调用示例

### 3.1 提交并轮询到结束（Python）

```python
import time

import requests

BASE = "http://127.0.0.1:2617"
KEY = "your-key"

resp = requests.post(
    f"{BASE}/api/open/download",
    json={
        "url": "https://example.com/a.mp4",
        "path": "/data/downloads/a.mp4",
        "key": KEY,
    },
    timeout=10,
)
resp.raise_for_status()
task = resp.json()["task"]
print("任务已创建：", task["task_id"], task["status"])

while task["status"] in {"pending", "running"}:
    time.sleep(3)
    task = requests.get(
        f"{BASE}/api/open/download/{task['task_id']}", timeout=10
    ).json()["task"]

print(task["status"], task.get("size"), task.get("error"))
```

### 3.2 Bash 一行提交

```bash
curl -s -X POST "http://127.0.0.1:2617/api/open/download" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com/a.mp4","path":"/data/downloads/a.mp4","key":"your-key"}'
```

---

## 4. 运行规则与限制

| 项目 | 说明 |
|---|---|
| 执行方式 | 每个任务一个后台线程，接口提交后立即返回，不阻塞调用方 |
| 下载重试 | 失败自动重试，共 2 次尝试，间隔 1.5 秒 |
| 请求超时 | 单次 HTTP 读超时 30 秒 |
| 代理 | 使用设置页配置的 HTTP 代理；未配置则直连 |
| 临时文件 | 下载过程写入 `<目标文件>.part`，成功后原子替换，失败自动清理 |
| 日志 | 提交、完成、失败都会写入「任务 → 日志」面板与 `logs/app.log` |
| 任务保留 | 已完成任务保留 1 小时；超过 200 条时淘汰最旧的任务 |
| 重启影响 | 任务列表仅存在内存中，服务重启后未完成/已完成任务均无法查询 |

---

## 5. 注意事项

- 该接口绕过了 Web 界面的目录选择，可写入任意绝对路径，请务必保管好 key；泄露后建议立即在设置页修改。
- 只接受 `http/https` 直链，HLS（`.m3u8`）等流式地址不会被展开合并，请传真实文件地址。
- 链接含防盗链校验时，下载可能返回 403/404，此时任务状态为 `failed`，`error` 中会带上游错误信息。
- Docker 部署时 `path` 必须是**容器内**可见的路径（如 `/data/downloads/a.mp4`），宿主机路径需先挂载进容器。
- 同一路径并发提交会产生多个任务同时写同一文件，请自行保证调用方不重复提交。
