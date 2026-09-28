# 重构改动计划书

> 生成时间：2026-09-28。每项改动独立成条，可单独勾选执行。
> 标注：**工作量** ⭐小（≤30min）/ ⭐⭐中（半天内）/ ⭐⭐⭐大（跨文件联动、需回归验证）

---

## 一、项目现状概览

| 部分 | 现状 |
|---|---|
| 后端 | FastAPI **单文件** `main.py`，1146 行：配置读写、定时调度、pektino 取数、下载任务、海报墙（扫描/ffprobe/缩略图/缓存）、SPA 服务全部混在一起 |
| 前端 | Vue3 + Element Plus + Pinia，约 1550 行，`api/stores/views` 分层清晰，结构健康 |
| 部署 | Docker 多阶段构建（node 构建前端 → python 运行）+ docker-compose + GitHub Actions 导出镜像；另有 PyInstaller exe 打包脚本 |
| 数据源 | `https://pektino.com/api/media`（本次刚从 RSC 页面抓取改为接口调用，已实测可用） |
| 测试 | **无任何测试** |

前端（frontend/src）本身分层合理，本次重构**基本不动前端**，除非选择 P2-2（API 风格统一）。

---

## 二、问题清单与改动项

### P0 安全与数据风险

#### [x] P0-1 配置死键含敏感信息，且通过 API 对外泄露 ⭐
**问题**：`config.json` 里残留已删除功能的键，其中 `twitter_cookie` 含 Twitter 账号的 `auth_token`/`ct0`（等同于登录凭证），`openlist.token` 是有效 JWT。而 `validate_config()` 是"合并制"（未知键原样保留回写），导致这些死键永远清不掉；同时 `GET /status` 和 `POST /save-quick` 把**完整配置原样返回**给浏览器——局域网内任何人打开页面就能拿到这些凭证。

**改动**：
1. `validate_config()` 改为**白名单制**：只保留当前实际使用的 7 个键（`download_root`、`proxy`、`auto_download_enabled`、`schedule_cron`、`max_daily_downloads`、`ranking_range`、`waterfall_per_page`），未知键读取时直接丢弃，保存时自然消失。
2. `/status`、`/save-quick` 返回前按白名单过滤（双保险）。
3. 清理本机 `config.json` 中的死键（含上述敏感信息）。

**风险**：极低——这些键对应的代码已删除，删掉不影响任何功能。

#### [x] P0-2 CORS 全开 + 全接口无鉴权 ⭐
**问题**：`CORSMiddleware(allow_origins=["*"], allow_credentials=True)`——Starlette 在此组合下会回显任意 `Origin`，等于允许任何网站跨域调用本服务；配合**全接口无鉴权**（含删除文件、触发下载），局域网内的恶意网页可诱导浏览器直接调用这些接口。服务绑定 `0.0.0.0`。

**改动（二选一，请指定）**：
- **方案 A（推荐，零成本）**：直接移除 CORSMiddleware。生产环境前后端同源（前端就是 FastAPI 托管的），开发环境走 vite proxy，本来就用不到 CORS。
- **方案 B（加一层轻量鉴权）**：config 增加 `access_token`，为空则不启用（保持现状）；启用后所有非 GET 接口校验 `X-Auth-Token` 头，前端 axios 拦截器统一附带。

**风险**：方案 A 无风险；方案 B 需要前端配合改动（前端约 10 行）。

#### [ ] P0-3 `/api/check-dir` 可探测主机任意路径 ⭐
**问题**：`GET /api/check-dir?path=/any/path` 返回该路径是否存在且为目录，可被用来探测宿主机目录结构（信息泄露，程度轻）。

**改动（三选一，请指定）**：
- **a. 保留现状**（局域网自用，接受此风险）
- **b. 收紧**：只允许校验「配置的 download_root 本身」及「download_root 下一级子目录」
- **c. 移除**：前端去掉即时校验，保存配置时由后端校验并报错

---

### P1 功能缺陷（确定性 bug，建议都修）

#### [x] P1-0 ~~数据源改接口~~（已完成）
本次会话已完成：RSC 抓取 → `pektino.com/api/media` 接口调用，瀑布流改服务端分页。

#### [x] P1-1 SPA 兜底路由吞掉未知 API 请求 ⭐
**问题**：`@app.get("/{full_path:path}")` 兜底路由会把**任何**未匹配的 GET 请求（包括拼错的 `/api/xxx`）都返回 `index.html`。前端期望 JSON 时会拿到 HTML，报难以排查的解析错误。

**改动**：兜底路由中 `full_path` 以 `api/` 开头时返回 404 JSON。

#### [x] P1-2 README 与现状严重不符 ⭐
**问题**：配置说明表里 `sort/range/min_time/max_time/time_filter_unit/tag_codes/waterfall_sort/waterfall_range/waterfall_min_time/waterfall_max_time/schedule_time` 等字段**代码中已全部不存在**；功能概述中的"分类管理、时长筛选"也已删除。

**改动**：按当前真实配置项（7 个键）和当前功能重写 README 的配置与功能部分。

#### [x] P1-3 换封面/删除文件会清空全库时长缓存 ⭐
**问题**：`_invalidate_poster_cache()` 在任何写操作后把 `.duration_cache.json` 整个删掉——换一张封面就导致**全部**视频重新 ffprobe（大库会卡很久）。

**改动**：写操作只清空内存扫描缓存 + 删除受影响条目的时长记录，保留其余时长缓存。

#### [x] P1-4 时长缓存文件写入非原子 ⭐
**问题**：后台探测线程 `json.dump` 直接写 `.duration_cache.json`，并发读可能读到写了一半的内容 → JSON 解析失败 → 被当成"无缓存"全量重探。

**改动**：先写临时文件再 `os.replace()` 原子替换；读侧加锁。

#### [x] P1-5 requirements.txt 含未使用依赖 ⭐
**问题**：`httpx` 全项目无引用（遗留）。另外函数内部有 `import shutil`、`import os` 局部导入，应提到顶部。

**改动**：移除 httpx；整理导入。

---

### P2 架构重构（核心诉求，请重点决策）

#### [x] P2-1 拆分 main.py 为模块包 ⭐⭐⭐
**问题**：1146 行单文件，七种职责混在一起，改动一处要在 1000+ 行里找上下文，也难以写测试。

**改动（推荐方案 a）**：
```
main.py              ← 薄壳入口：uvicorn 启动（保留，PyInstaller/Docker 入口不变）
app/
  __init__.py
  config.py          ← 常量、DEFAULT_CONFIG、load/save/validate（白名单制）
  logging_utils.py   ← append_log / get_logs / runtime_state
  media_api.py       ← pektino 客户端（_fetch_pektino_media、_normalize_media_item）
  downloader.py      ← run_download_job、_download_items、download_binary、调度注册
  poster.py          ← 海报墙：扫描、缓存、ffprobe、缩略图、增删改查
  routes.py          ← 所有 FastAPI 路由（或按 poster/waterfall/config 再拆）
```
同步修改：`Dockerfile` 的 `COPY main.py` → `COPY main.py app/`（两条 COPY）；`build_exe.py` 无需改（PyInstaller 会顺着 import 打包 app 包，入口仍是 main.py）。

**备选方案 b**：不拆文件，只在 main.py 内部按分区注释重排 + 去重。改动最小，但治标不治本。

**风险**：方案 a 需要回归验证全部接口 + 重新构建 Docker/exe 验证打包正确。

#### [x] P2-2 API 风格统一 ⭐⭐
**问题**：接口风格不一致——`/save`、`/save-quick` 收 `form-data`，`/api/waterfall/settings` 收 JSON；删除接口返回 `{ok, deleted}`，瀑布流下载返回 `{ok, success, skipped, failed}`，字段名各搞各的。没有 pydantic 请求模型，全靠手工 `form.get()` / `body.get()`。

**改动**：写接口统一 JSON body + pydantic 模型；响应统一 `{ok, data|error}` 结构。**需要前端同步小改**（约 5 处调用）。

#### [x] P2-3 配置读取每请求落盘 ⭐
**问题**：`get_current_config()` 每次都读盘 + 校验，而 `/api/poster-thumb`、`/api/poster-video` 这类**每张图/每段视频都会调**的接口也各读一次。浏览一页海报墙 = 几十次多余的磁盘读 + JSON 解析。

**改动**：按文件 mtime 缓存解析结果，文件没变直接用缓存（保存时天然失效）。

#### [x] P2-4 时长缓存"内存 + 文件"双轨逻辑合并 ⭐⭐
**问题**：进程内 `_duration_cache`（TTL 1h）和持久化 `.duration_cache.json` 两套缓存并存：`_probe_video_duration` 查内存，`_collect_poster_items` 查文件，互相不通气，逻辑绕且浪费。

**改动**：合并为「文件为主 + 进程内 mtime 缓存」单一读路径，探测结果写文件。

---

### P3 体验与性能（可选增强）

#### [x] P3-1 海报墙大数据量分页 ⭐⭐
`/api/poster?folder=`（不传 folder 时）一次性返回全库所有条目。已有 60s 扫描缓存 + 后台 ffprobe + WebP 缩略图三重优化（根目录的 poster-perf-diagnosis.html 就是当时调优的痕迹），中小库够用；但如果库到几千个视频，建议加服务端分页（接口加 `page/page_size`，前端瀑布流已具备分页范式可复用）。

#### [x] P3-2 下载并行 + 失败重试 ⭐
`_download_items` 串行下载、失败不重试。可改为 2-3 并发 + 单文件重试 1-2 次。收益取决于网络质量。

#### [x] P3-3 日志落盘 ⭐
日志只在内存存 300 条，重启即失。可加 `logs/app.log`（RotatingFileHandler），内存保留最近 300 条供前端展示。

---

### P4 仓库卫生（零风险清扫，建议执行）

#### [x] P4-1 移除 git 跟踪的垃圾文件 ⭐
以下文件**已被 git 跟踪**，属于调试/临时产物：
- `_rendered_js.txt`（25KB 抓取页面缓存）
- `main.pyc.tmp.1918073409200`（64KB 编译临时文件）
- `poster-perf-diagnosis.html`（性能诊断调优遗留，内容已沉淀进代码）
- `.workbuddy/`（AI 工具记忆目录）

**改动**：`git rm --cached` + 本地保留/删除按你意愿；`.gitignore` 补充 `.agents/`、`.workbuddy/`。

#### [x] P4-2 清理空目录与本地产物 ⭐
- `demo/`、`templates/` 已是空目录 → 删除
- `build/`、`dist/`、`venv/`、`__pycache__/` 已被 .gitignore 覆盖，不处理

---

## 三、工作量与风险对照

| 编号 | 改动 | 工作量 | 风险 | 需要动前端 |
|---|---|---|---|---|
| P0-1 | 配置白名单 + 响应脱敏 | ⭐ | 极低 | 否 |
| P0-2 | CORS 收紧 / 可选鉴权 | ⭐ | 低 | 仅方案 B |
| P0-3 | check-dir 收紧（三选一） | ⭐ | 低 | 方案 c 需要 |
| P1-1 | SPA 兜底排除 /api | ⭐ | 极低 | 否 |
| P1-2 | README 重写 | ⭐ | 无 | 否 |
| P1-3 | 时长缓存精细失效 | ⭐ | 低 | 否 |
| P1-4 | 缓存原子写 | ⭐ | 极低 | 否 |
| P1-5 | 依赖与导入清理 | ⭐ | 极低 | 否 |
| P2-1 | **拆分模块包** | ⭐⭐⭐ | 中（需回归+重新打包验证） | 否 |
| P2-2 | API 风格统一 | ⭐⭐ | 中 | 是 |
| P2-3 | 配置 mtime 缓存 | ⭐ | 极低 | 否 |
| P2-4 | 时长缓存双轨合并 | ⭐⭐ | 低 | 否 |
| P3-1 | 海报墙分页 | ⭐⭐ | 中 | 是 |
| P3-2 | 下载并行重试 | ⭐ | 低 | 否 |
| P3-3 | 日志落盘 | ⭐ | 低 | 否 |
| P4-1 | 移除跟踪的垃圾文件 | ⭐ | 无 | 否 |
| P4-2 | 清理空目录 | ⭐ | 无 | 否 |

## 四、验证方式

- 每项改完后：`python -m py_compile` + pytest（若选 P2-5 加测试）+ 用 TestClient 冒烟跑 `/status`、`/api/waterfall`、`/api/poster` 三大接口
- 涉及打包的改动（P2-1）：重新 `docker compose build` 启动验证
- 前端改动：`npm run build` + 浏览器过一遍四个页面（设置/瀑布流/海报墙/日志）

## 五、建议执行顺序

**P4（清扫）→ P1（确定性 bug）→ P0（安全）→ P2-3/P2-4（低风险重构）→ P2-1（大重构）→ P2-2/P3（可选）**

先做低风险高确定性的事；P2-1 拆包放后面单独做、单独验证。

---

## 待你决策的事项

1. **执行哪些编号？**（可直接回复，如"P4 全部 + P1 全部 + P0-1 + P0-2方案A + P2-1"）
2. P0-2：CORS 收紧选**方案 A**（移除中间件）还是**方案 B**（加令牌鉴权）？
3. P0-3：check-dir 选 **a 保留 / b 收紧 / c 移除**？
4. P0-1 清理 config.json 时，`twitter_cookie`、`openlist` 等死键**直接删除**还是先备份一份 `config.json.bak`？
5. 是否需要顺手加一个最小 pytest 测试集？（未列入编号，默认不加）
