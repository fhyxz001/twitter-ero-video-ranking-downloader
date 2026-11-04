# 排行榜海报接口文档

提供两组接口：一组返回拼图图片流，一组返回图上序号对应的视频下载地址。数据来源为 `pektino.com` 排行榜（按播放量 `pv` 排序）。

- 服务端口：`2617`
- Base URL：`http://<host>:2617`
- 图片与链接接口使用同一份榜单快照，序号严格一一对应。

---

## 1. 获取拼图图片

```http
GET /api/rank-poster/image
```

### 查询参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `type` | string | 否 | `day` | 榜单类型：`day` / `week` / `month` / `year` |
| `groups` | int | 否 | `1` | 组数，每组 10 张，取值范围 `1-10` |

`type` 也兼容内部档位写法：`daily` / `weekly` / `monthly` / `all`。

### 类型映射

| 传入 type | 实际榜单 | 说明 |
|---|---|---|
| `day` / `daily` | 日榜 | 对应站点 `range=timely` |
| `week` / `weekly` | 周榜 | 对应站点 `range=weekly` |
| `month` / `monthly` | 月榜 | 对应站点 `range=monthly` |
| `year` / `all` | 总榜 | 对应站点 `range=all` |

> 站点只提供日榜、周榜、月榜、总榜，没有真正的年榜，因此 `year` 映射为总榜。

### 图片规则

- 每行 2 张，每 5 行一组（共 10 张）
- 多组时纵向排列，组与组之间有 12px 分隔
- 每张封面左上角标注序号
- 序号跨组全局连续：`groups=2` 时编号为 `1-20`
- 单张封面按 9:16 比例裁切填充（288 × 512）
- 整图宽度 576px，高度为 `groups * 2560 + (groups - 1) * 12`
- 封面下载失败或没有封面的条目使用灰色占位图，序号保持不变

### 成功响应

- `Content-Type: image/jpeg`
- `Cache-Control: public, max-age=600`

直接返回 JPEG 图片流，可用 `<img src>` 或下载工具读取。

### 示例

```bash
# 日榜前 10 名（1 组）
curl -o day_top10.jpg "http://127.0.0.1:2617/api/rank-poster/image?type=day&groups=1"

# 周榜前 20 名（2 组，序号 1-20）
curl -o week_top20.jpg "http://127.0.0.1:2617/api/rank-poster/image?type=week&groups=2"
```

---

## 2. 获取下载链接列表

```http
GET /api/rank-poster/links
```

### 查询参数

与图片接口一致：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `type` | string | 否 | `day` | 榜单类型：`day` / `week` / `month` / `year` |
| `groups` | int | 否 | `1` | 组数，每组 10 条，取值范围 `1-10` |

### 成功响应

`Content-Type: application/json`

```json
{
  "ok": true,
  "type": "daily",
  "label": "日榜",
  "groups": 2,
  "count": 20,
  "items": [
    {
      "group": 1,
      "index": 1,
      "video_url": "https://video.twimg.com/amplify_video/xxx/vid/avc1/720x1280/xxx.mp4"
    },
    {
      "group": 1,
      "index": 2,
      "video_url": "https://video.twimg.com/amplify_video/xxx/vid/avc1/720x1280/xxx.mp4"
    },
    {
      "group": 1,
      "index": 10,
      "video_url": "https://video.twimg.com/amplify_video/xxx/vid/avc1/720x1280/xxx.mp4"
    },
    {
      "group": 2,
      "index": 11,
      "video_url": "https://video.twimg.com/amplify_video/xxx/vid/avc1/720x1280/xxx.mp4"
    }
  ]
}
```

### 字段说明

| 字段 | 类型 | 说明 |
|---|---|---|
| `ok` | bool | 是否成功 |
| `type` | string | 实际使用的榜单档位：`daily` / `weekly` / `monthly` / `all` |
| `label` | string | 榜单中文名：日榜 / 周榜 / 月榜 / 总榜 |
| `groups` | int | 请求的组数 |
| `count` | int | 实际返回的链接数量，正常情况下为 `groups * 10`；榜单条目不足时会更少 |
| `items` | array | 链接列表，按序号升序 |
| `items[].group` | int | 所属组号，从 1 开始 |
| `items[].index` | int | 图片上的序号，从 1 开始，跨组连续 |
| `items[].video_url` | string | 该序号对应的原始视频下载地址（mp4 直链） |

### 示例

```bash
# 日榜前 10 条链接
curl "http://127.0.0.1:2617/api/rank-poster/links?type=day&groups=1"

# 月榜前 30 条链接（3 组，序号 1-30）
curl "http://127.0.0.1:2617/api/rank-poster/links?type=month&groups=3"
```

---

## 3. 序号与链接的对应关系

以 `type=week&groups=2` 为例：

| 图片区域 | 组号 | 图片序号 | 链接 `index` |
|---|---|---|---|
| 上半部分 | 1 | 1-10 | 1-10 |
| 下半部分 | 2 | 11-20 | 11-20 |

图片上的序号与 `items[].index` 完全一致，可直接按序号取 `video_url` 下载。

---

## 4. 错误响应

错误时返回 JSON：

```json
{
  "ok": false,
  "error": "type 仅支持 day / week / month / year"
}
```

| HTTP 状态码 | 场景 |
|---|---|
| `400` | `type` 不合法，或 `groups` 不在 `1-10` 范围内 |
| `404` | 该榜单暂无数据 |
| `502` | 上游榜单拉取失败、代理不可用或拼图生成失败 |
| `422` | 参数类型错误（如 `groups` 传入非数字，由 FastAPI 自动返回） |

---

## 5. 缓存与一致性说明

- 榜单快照按 `(type, groups)` 在内存中缓存 10 分钟，图片接口和链接接口共用同一份快照。
- 拼图生成后按榜单条目 ID 落盘缓存，榜单未变化时直接返回已有图片。
- 图片响应头 `Cache-Control: public, max-age=600`，客户端 10 分钟内可能复用缓存。
- 如果先调用图片接口、再调用链接接口，且两次调用间隔超过 10 分钟，榜单可能已更新，序号内容可能变化。需要严格一致时，请在 10 分钟内成对调用。
