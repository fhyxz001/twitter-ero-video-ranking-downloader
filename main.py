import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, urlparse

from contextlib import asynccontextmanager

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


# 适配 PyInstaller 路径处理
def get_resource_path(relative_path: str) -> Path:
    """获取资源文件的绝对路径，适配开发环境和 PyInstaller 编译环境"""
    if getattr(sys, "frozen", False):
        # PyInstaller 运行时路径
        base_path = Path(sys._MEIPASS)
    else:
        # 开发环境路径
        base_path = Path(__file__).resolve().parent
    return base_path / relative_path


def get_exe_dir() -> Path:
    """获取程序执行文件所在的目录，用于存放配置文件和下载内容"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parent


APP_DIR = get_exe_dir()
CONFIG_PATH = APP_DIR / "config.json"
TEMPLATES_PATH = get_resource_path("templates")
STATIC_PATH = get_resource_path("static")
MEDIA_API_URL = "https://truvaze.com/api/media"
REQUEST_TIMEOUT = 30
TIME_FILTER_MIN = 0
TIME_FILTER_MAX = 24 * 60 * 60
ALLOWED_SORTS = {"time", "favorite", "pv", "created"}
ALLOWED_RANGES = {"daily", "weekly", "monthly", "all"}
ALLOWED_WATERFALL_PAGE_SIZES = {10, 20, 30, 50, 100}
UNTAGGED_FOLDER_NAME = "无标签"

DEFAULT_CONFIG: Dict[str, object] = {
    "download_root": "/vol1/1000/AdultMedia/tw",
    "proxy": "",
    "auto_download_enabled": True,
    "schedule_time": "03:00",
    "max_daily_downloads": 30,
    "sort": "pv",
    "range": "daily",
    "min_time": TIME_FILTER_MIN,
    "max_time": TIME_FILTER_MAX,
    "time_filter_unit": "seconds",
    "tag_codes": [],
    "waterfall_per_page": 10,
    "waterfall_sort": "pv",
    "waterfall_range": "daily",
    "waterfall_min_time": TIME_FILTER_MIN,
    "waterfall_max_time": TIME_FILTER_MAX,
}

TIME_FILTER_OPTIONS = [
    {"label": "全部", "min": 0, "max": TIME_FILTER_MAX},
    {"label": "0-5分钟", "min": 0, "max": 5 * 60},
    {"label": "5-15分钟", "min": 5 * 60, "max": 15 * 60},
    {"label": "15-30分钟", "min": 15 * 60, "max": 30 * 60},
    {"label": "30分钟-1小时", "min": 30 * 60, "max": 60 * 60},
    {"label": "一小时以上", "min": 60 * 60, "max": TIME_FILTER_MAX},
]

SORT_OPTIONS = [
    {"value": "created", "label": "最近添加"},
    {"value": "time", "label": "按时长"},
    {"value": "favorite", "label": "按点赞"},
    {"value": "pv", "label": "按观看数"},
]

RANGE_OPTIONS = [
    {"value": "daily", "label": "每日"},
    {"value": "weekly", "label": "每周"},
    {"value": "monthly", "label": "每月"},
    {"value": "all", "label": "全部"},
]

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    cfg = get_current_config()
    if not scheduler.running:
        scheduler.start()
    update_schedule(cfg)
    append_log("服务启动完成")
    yield
    # shutdown
    if scheduler.running:
        scheduler.shutdown(wait=False)
    append_log("服务已停止")


app = FastAPI(title="twitter-ero-video-ranking-downloader", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_PATH)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_PATH))
scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

config_lock = threading.Lock()
log_lock = threading.Lock()
runtime_lock = threading.Lock()

runtime_state = {
    "is_running": False,
    "last_run_time": None,
    "last_result": "尚未执行",
}
log_lines: List[str] = []


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def append_log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_lock:
        log_lines.append(f"[{timestamp}] {message}")
        if len(log_lines) > 300:
            del log_lines[:-300]


def get_logs() -> List[str]:
    with log_lock:
        return list(log_lines)


def validate_time_window(min_value: object, max_value: object, label: str) -> tuple[int, int]:
    min_time = int(min_value)
    max_time = int(max_value)
    if min_time < TIME_FILTER_MIN or min_time > TIME_FILTER_MAX:
        raise ValueError(f"{label}最小时长必须在 {TIME_FILTER_MIN} 到 {TIME_FILTER_MAX} 秒之间")
    if max_time < TIME_FILTER_MIN or max_time > TIME_FILTER_MAX:
        raise ValueError(f"{label}最大时长必须在 {TIME_FILTER_MIN} 到 {TIME_FILTER_MAX} 秒之间")
    if min_time > max_time:
        raise ValueError(f"{label}最小时长不能大于最大时长")
    return min_time, max_time


def validate_config(raw: Dict[str, object]) -> Dict[str, object]:
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(raw or {})

    download_root = str(cfg.get("download_root", "")).strip()
    if not download_root:
        raise ValueError("下载根目录不能为空")
    cfg["download_root"] = download_root

    proxy = str(cfg.get("proxy", "")).strip()
    cfg["proxy"] = proxy

    cfg["auto_download_enabled"] = parse_bool(cfg.get("auto_download_enabled", True))

    sort = str(cfg.get("sort", "pv")).strip()
    if sort not in ALLOWED_SORTS:
        raise ValueError("排序方式必须是 time、favorite、pv 或 created")
    cfg["sort"] = sort

    range_value = str(cfg.get("range", "daily")).strip()
    if range_value not in ALLOWED_RANGES:
        raise ValueError("时间范围必须是 daily、weekly、monthly 或 all")
    cfg["range"] = range_value

    schedule_time = str(cfg.get("schedule_time", "")).strip()
    try:
        time.strptime(schedule_time, "%H:%M")
    except ValueError as exc:
        raise ValueError("定时执行时间格式必须为 HH:MM") from exc
    cfg["schedule_time"] = schedule_time

    max_daily = int(cfg.get("max_daily_downloads", 0))
    if max_daily <= 0:
        raise ValueError("每类视频下载数必须大于0")
    cfg["max_daily_downloads"] = max_daily

    min_time, max_time = validate_time_window(
        cfg.get("min_time", TIME_FILTER_MIN),
        cfg.get("max_time", TIME_FILTER_MAX),
        "",
    )
    cfg["min_time"] = min_time
    cfg["max_time"] = max_time
    cfg["time_filter_unit"] = "seconds"

    tag_codes = cfg.get("tag_codes", [])
    if isinstance(tag_codes, str):
        try:
            tag_codes = json.loads(tag_codes)
        except (json.JSONDecodeError, TypeError):
            tag_codes = []
    if not isinstance(tag_codes, list):
        tag_codes = []
    tag_codes = [str(t).strip() for t in tag_codes if str(t).strip()]
    # deduplicate while preserving order
    seen = set()
    deduped = []
    for t in tag_codes:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    cfg["tag_codes"] = deduped

    waterfall_per_page = int(cfg.get("waterfall_per_page", 10))
    if waterfall_per_page not in ALLOWED_WATERFALL_PAGE_SIZES:
        raise ValueError("瀑布流每页展示数量必须是 10、20、30、50 或 100")
    cfg["waterfall_per_page"] = waterfall_per_page

    waterfall_sort = str(cfg.get("waterfall_sort", "pv")).strip()
    if waterfall_sort not in ALLOWED_SORTS:
        raise ValueError("瀑布流排序方式必须是 time、favorite、pv 或 created")
    cfg["waterfall_sort"] = waterfall_sort

    waterfall_range = str(cfg.get("waterfall_range", "daily")).strip()
    if waterfall_range not in ALLOWED_RANGES:
        raise ValueError("瀑布流时间范围必须是 daily、weekly、monthly 或 all")
    cfg["waterfall_range"] = waterfall_range

    waterfall_min_time, waterfall_max_time = validate_time_window(
        cfg.get("waterfall_min_time", TIME_FILTER_MIN),
        cfg.get("waterfall_max_time", TIME_FILTER_MAX),
        "瀑布流",
    )
    cfg["waterfall_min_time"] = waterfall_min_time
    cfg["waterfall_max_time"] = waterfall_max_time
    return cfg


def resolve_download_root(download_root: object) -> Path:
    root = Path(str(download_root)).expanduser()
    if not root.is_absolute():
        root = APP_DIR / root
    return root.resolve()


def load_config() -> Dict[str, object]:
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return dict(DEFAULT_CONFIG)
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        # backward compatibility: old configs stored duration in minutes.
        if raw.get("time_filter_unit") != "seconds" and 0 < int(raw.get("min_time", 0)) <= 180:
            raw["min_time"] = int(raw["min_time"]) * 60
        if raw.get("time_filter_unit") != "seconds" and 0 < int(raw.get("max_time", 0)) <= 180:
            raw["max_time"] = int(raw["max_time"]) * 60
        raw["time_filter_unit"] = "seconds"
        # backward compatibility: convert old tag_code string to tag_codes list
        if "tag_code" in raw and "tag_codes" not in raw:
            old_tag = str(raw["tag_code"]).strip()
            raw["tag_codes"] = [old_tag] if old_tag else []
            del raw["tag_code"]
        return validate_config(raw)
    except Exception as exc:
        append_log(f"读取配置失败，已回退默认配置：{exc}")
        save_config(DEFAULT_CONFIG)
        return dict(DEFAULT_CONFIG)


def save_config(cfg: Dict[str, object]) -> None:
    validated = validate_config(cfg)
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(validated, f, ensure_ascii=False, indent=2)


def get_current_config() -> Dict[str, object]:
    with config_lock:
        return load_config()


def update_schedule(cfg: Dict[str, object]) -> None:
    scheduler.remove_all_jobs()
    if not bool(cfg.get("auto_download_enabled", True)):
        append_log("定时下载已关闭")
        return

    hour, minute = cfg["schedule_time"].split(":")
    scheduler.add_job(
        run_download_job,
        trigger=CronTrigger(hour=int(hour), minute=int(minute)),
        id="daily_download_job",
        replace_existing=True,
    )
    append_log(f"定时任务已更新：每天 {cfg['schedule_time']} 执行")


def get_waterfall_config(cfg: Dict[str, object]) -> Dict[str, object]:
    return {
        "per_page": int(cfg.get("waterfall_per_page", 10)),
        "sort": str(cfg.get("waterfall_sort", "pv")),
        "range": str(cfg.get("waterfall_range", "daily")),
        "min_time": int(cfg.get("waterfall_min_time", TIME_FILTER_MIN)),
        "max_time": int(cfg.get("waterfall_max_time", TIME_FILTER_MAX)),
    }


def get_file_ext_from_url(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    ext = Path(parsed.path).suffix.lower()
    if ext:
        return ext
    return fallback


def build_proxies(proxy: str) -> Optional[Dict[str, str]]:
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def build_media_request_params(
    cfg: Dict[str, object],
    tag_code: str = "",
    per_page: int = 30,
    page: int = 1,
) -> Dict[str, object]:
    params: Dict[str, object] = {
        "page": max(1, int(page)),
        "per_page": per_page,
        "ids": "",
        "isAnimeOnly": 0,
        "sort": str(cfg["sort"]),
    }
    min_time = int(cfg["min_time"])
    max_time = int(cfg["max_time"])
    if min_time > TIME_FILTER_MIN:
        params["min_time"] = min_time
    if max_time < TIME_FILTER_MAX:
        params["max_time"] = max_time
    tc = str(tag_code).strip()
    if tc:
        params["category"] = tc
    range_value = str(cfg["range"])
    if range_value != "daily":
        params["range"] = range_value
    return params


TAGS_JSON_PATH = get_resource_path("templates/code.json")

_tags_cache: Optional[List[Dict[str, object]]] = None


def _load_tags_static() -> List[Dict[str, object]]:
    global _tags_cache
    if _tags_cache is not None:
        return _tags_cache

    with TAGS_JSON_PATH.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    tags: List[Dict[str, object]] = []
    for item in raw:
        code = str(item.get("code", "")).strip()
        if not code:
            continue
        name = str(item.get("name_zh_cn", "")).strip() or code
        tags.append({"code": code, "name": name})
    _tags_cache = tags
    return tags


_INVALID_FOLDER_CHARS = '\\/:*?"<>|'


def sanitize_folder_name(name: str) -> str:
    """清理文件夹名中的非法字符与控制字符，保证可作为目录名。"""
    cleaned = "".join(
        ch for ch in str(name)
        if ch not in _INVALID_FOLDER_CHARS and ord(ch) >= 32
    ).strip().strip(".")
    return cleaned


def get_tag_name(tag_code: str) -> str:
    """根据标签 code 查中文名，查不到则回退到 code 本身。"""
    code = str(tag_code).strip()
    if not code:
        return ""
    for tag in _load_tags_static():
        if tag.get("code") == code:
            return str(tag.get("name") or code)
    return code


def resolve_target_dir(cfg: Dict[str, object], download_root: Path, tag_code: str = "") -> Path:
    """根据分类决定视频落地的目标文件夹（无标签或标签子文件夹）。"""
    tc = str(tag_code).strip()
    if not tc:
        return download_root / UNTAGGED_FOLDER_NAME
    folder = sanitize_folder_name(get_tag_name(tc)) or sanitize_folder_name(tc)
    if not folder:
        return download_root
    return download_root / folder


def build_download_categories(cfg: Dict[str, object]) -> List[Dict[str, str]]:
    """构建本次任务要下载的分类。无标签分类固定排在第一位。"""
    categories = [{"tag_code": "", "name": UNTAGGED_FOLDER_NAME}]
    for tag_code in cfg.get("tag_codes", []):
        code = str(tag_code).strip()
        if not code:
            continue
        categories.append({"tag_code": code, "name": get_tag_name(code) or code})
    return categories


def download_binary(session: requests.Session, url: str, target_path: Path, proxies: Optional[Dict[str, str]]) -> bool:
    try:
        with session.get(url, stream=True, timeout=REQUEST_TIMEOUT, proxies=proxies) as resp:
            resp.raise_for_status()
            with target_path.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as exc:
        append_log(f"下载失败 {url} -> {target_path.name}，错误：{exc}")
        if target_path.exists():
            try:
                target_path.unlink()
            except OSError:
                pass
        return False


def _fetch_media_items(session: requests.Session, cfg: Dict[str, object], tag_code: str, proxies) -> List[dict]:
    """从 API 获取媒体列表，返回 items 数组。"""
    resp = session.get(
        MEDIA_API_URL,
        params=build_media_request_params(cfg, tag_code=tag_code),
        timeout=REQUEST_TIMEOUT,
        proxies=proxies,
    )
    resp.raise_for_status()
    raw_text = resp.text.strip()
    if not raw_text:
        raise ValueError(f"API 返回空响应（HTTP {resp.status_code}），请检查接口或代理设置")
    try:
        payload = resp.json()
    except json.JSONDecodeError as exc:
        preview = raw_text[:200]
        raise ValueError(f"API 返回非 JSON 内容（{exc}）：{preview}") from exc
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("API 返回的 items 不是数组")
    return items


def _download_items(session: requests.Session, items: list, target_dir: Path, max_count: int, proxies) -> tuple:
    """下载指定列表中的视频，返回 (success_count, skip_count, fail_count)。"""
    success_count = 0
    skip_count = 0
    fail_count = 0

    for item in items:
        if success_count >= max_count:
            append_log(f"本次已下载 {success_count} 个，达到上限 {max_count}，停止继续下载")
            break

        if not isinstance(item, dict):
            skip_count += 1
            continue

        video_id = str(item.get("id", "")).strip()
        video_url = str(item.get("url", "")).strip()
        thumbnail_url = str(item.get("thumbnail", "")).strip()
        if not video_id:
            skip_count += 1
            append_log("条目缺少 id，已跳过")
            continue
        if not video_url:
            skip_count += 1
            append_log(f"条目 {video_id} 缺少 url，已跳过")
            continue

        # 以 id 命名，去重只检查目标文件夹内是否已存在同 id 的视频文件。
        if any((target_dir / f"{video_id}{ext}").exists() for ext in VIDEO_EXTS):
            skip_count += 1
            append_log(f"id {video_id} 已存在，跳过")
            continue

        video_ext = get_file_ext_from_url(video_url, ".mp4")
        thumb_ext = get_file_ext_from_url(thumbnail_url, ".jpg") if thumbnail_url else ".jpg"
        video_path = target_dir / f"{video_id}{video_ext}"
        thumb_path = target_dir / f"{video_id}{thumb_ext}"

        ok_video = download_binary(session, video_url, video_path, proxies)
        ok_thumb = True
        if thumbnail_url:
            ok_thumb = download_binary(session, thumbnail_url, thumb_path, proxies)

        if ok_video and ok_thumb:
            success_count += 1
            append_log(f"下载完成：{target_dir.name}/{video_path.name}")
        else:
            fail_count += 1

    return success_count, skip_count, fail_count


def _is_safe_remote_media_url(url: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _normalize_waterfall_item(item: dict, tag_code: str) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    video_id = str(item.get("id", "")).strip()
    video_url = str(item.get("url", "")).strip()
    thumbnail_url = str(item.get("thumbnail", "")).strip()
    if not video_id or not _is_safe_remote_media_url(video_url):
        return None
    return {
        "id": video_id,
        "url": video_url,
        "preview_url": video_url,
        "thumbnail": thumbnail_url if _is_safe_remote_media_url(thumbnail_url) else "",
        "tag_code": tag_code,
        "title": str(item.get("title") or item.get("name") or video_id),
        "duration": item.get("duration") or item.get("time") or "",
        "favorite": item.get("favorite") or item.get("favorites") or "",
        "pv": item.get("pv") or item.get("views") or "",
    }


def _waterfall_tabs(cfg: Dict[str, object]) -> List[dict]:
    tabs = [{"code": "", "name": "全部"}]
    for tag_code in cfg.get("tag_codes", []):
        code = str(tag_code).strip()
        if code:
            tabs.append({"code": code, "name": get_tag_name(code) or code})
    return tabs


def _validate_waterfall_tag(cfg: Dict[str, object], tag_code: str) -> bool:
    code = str(tag_code or "").strip()
    if not code:
        return True
    return code in {str(t).strip() for t in cfg.get("tag_codes", [])}


def run_download_job() -> None:
    with runtime_lock:
        if runtime_state["is_running"]:
            append_log("任务已在运行中，跳过本次触发")
            return
        runtime_state["is_running"] = True

    started = datetime.now()
    runtime_state["last_run_time"] = started.strftime("%Y-%m-%d %H:%M:%S")
    append_log("开始执行下载任务")

    try:
        cfg = get_current_config()
        download_root = resolve_download_root(cfg["download_root"])
        download_root.mkdir(parents=True, exist_ok=True)

        max_per_category = int(cfg["max_daily_downloads"])
        proxy = str(cfg["proxy"]).strip()
        proxies = build_proxies(proxy)
        session = requests.Session()

        total_success = 0
        total_skip = 0
        total_fail = 0

        categories = build_download_categories(cfg)
        expected_total = len(categories) * max_per_category
        category_names = "、".join(category["name"] for category in categories)
        append_log(
            f"本次计划下载 {len(categories)} 个分类：{category_names}；"
            f"每类 {max_per_category} 个，理论最多 {expected_total} 个"
        )

        for category in categories:
            tag_code = category["tag_code"]
            category_name = category["name"]
            target_dir = resolve_target_dir(cfg, download_root, tag_code=tag_code)
            target_dir.mkdir(parents=True, exist_ok=True)
            tag_label = "无标签" if not tag_code else f"{category_name}({tag_code})"
            append_log(
                f"当前筛选：range={cfg['range']} sort={cfg['sort']} "
                f"time={cfg['min_time']}s-{cfg['max_time']}s "
                f"分类={tag_label} 目录={target_dir.name}"
            )
            try:
                items = _fetch_media_items(session, cfg, tag_code, proxies)
                s, k, f = _download_items(session, items, target_dir, max_per_category, proxies)
            except Exception as exc:
                s, k, f = 0, 0, 1
                append_log(f"分类 [{category_name}] 失败：{exc}")
            append_log(f"分类 [{category_name}] 完成：成功 {s}，跳过 {k}，失败 {f}")
            total_success += s
            total_skip += k
            total_fail += f

        result = (
            f"任务完成：计划最多 {expected_total}，成功 {total_success}，"
            f"跳过 {total_skip}，失败 {total_fail}"
        )
        append_log(result)
        runtime_state["last_result"] = result
    except Exception as exc:
        err_msg = f"任务异常：{exc}"
        append_log(err_msg)
        runtime_state["last_result"] = err_msg
    finally:
        runtime_state["is_running"] = False


@app.get("/")
def index(request: Request):
    cfg = get_current_config()
    state = dict(runtime_state)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "config": cfg,
            "state": state,
            "logs": "\n".join(get_logs()),
            "time_filter_options": TIME_FILTER_OPTIONS,
            "tag_codes_json": json.dumps(cfg.get("tag_codes", []), ensure_ascii=False),
            "sort_options": SORT_OPTIONS,
            "range_options": RANGE_OPTIONS,
        },
    )


@app.post("/save")
async def save(request: Request):
    try:
        form = await request.form()
        download_root = str(form.get("download_root", "")).strip()
        proxy = str(form.get("proxy", "")).strip()
        auto_download_enabled = parse_bool(form.get("auto_download_enabled", "1"))
        schedule_time = str(form.get("schedule_time", "")).strip()
        max_daily_downloads = int(form.get("max_daily_downloads", 0))
        sort = str(form.get("sort", "pv")).strip()
        range_value = str(form.get("range", "daily")).strip()
        min_time = int(form.get("min_time", TIME_FILTER_MIN))
        max_time = int(form.get("max_time", TIME_FILTER_MAX))
        tag_codes_raw = str(form.get("tag_codes", "[]")).strip()
        try:
            tag_codes = json.loads(tag_codes_raw)
        except json.JSONDecodeError:
            tag_codes = []

        with config_lock:
            cfg = load_config()
            cfg.update({
                "download_root": download_root,
                "proxy": proxy,
                "auto_download_enabled": auto_download_enabled,
                "schedule_time": schedule_time,
                "max_daily_downloads": max_daily_downloads,
                "sort": sort,
                "range": range_value,
                "min_time": min_time,
                "max_time": max_time,
                "tag_codes": tag_codes,
            })
            save_config(cfg)
            updated = load_config()
        update_schedule(updated)
        append_log("配置已保存")
        return JSONResponse({"ok": True})
    except Exception as exc:
        append_log(f"配置保存失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/save-quick")
async def save_quick(request: Request):
    try:
        form = await request.form()
        with config_lock:
            cfg = load_config()

            if "download_root" in form:
                cfg["download_root"] = str(form.get("download_root", "")).strip()

            if "tag_codes" in form:
                tag_codes_raw = str(form.get("tag_codes", "[]")).strip()
                try:
                    cfg["tag_codes"] = json.loads(tag_codes_raw)
                except json.JSONDecodeError:
                    cfg["tag_codes"] = []

            save_config(cfg)
            updated = load_config()
        append_log("下载根目录/标签筛选已自动保存")
        return JSONResponse({"ok": True, "config": updated})
    except Exception as exc:
        append_log(f"自动保存下载根目录/标签筛选失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/waterfall/settings")
async def save_waterfall_settings(request: Request):
    try:
        body = await request.json()
        with config_lock:
            cfg = load_config()
            cfg["waterfall_per_page"] = int(body.get("per_page", cfg.get("waterfall_per_page", 10)))
            cfg["waterfall_sort"] = str(body.get("sort", cfg.get("waterfall_sort", "pv"))).strip()
            cfg["waterfall_range"] = str(body.get("range", cfg.get("waterfall_range", "daily"))).strip()
            cfg["waterfall_min_time"] = int(body.get("min_time", cfg.get("waterfall_min_time", TIME_FILTER_MIN)))
            cfg["waterfall_max_time"] = int(body.get("max_time", cfg.get("waterfall_max_time", TIME_FILTER_MAX)))
            save_config(cfg)
            updated = load_config()
        append_log("瀑布流配置已保存")
        return JSONResponse({"ok": True, "config": get_waterfall_config(updated)})
    except Exception as exc:
        append_log(f"瀑布流配置保存失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/run-now")
def run_now():
    if runtime_state["is_running"]:
        return JSONResponse({"ok": False, "message": "任务正在运行中"})

    threading.Thread(target=run_download_job, daemon=True).start()
    append_log("已触发手动执行任务")
    return JSONResponse({"ok": True, "message": "任务已启动"})


@app.get("/status")
def status():
    return JSONResponse(
        {
            "ok": True,
            "state": runtime_state,
            "logs": get_logs(),
            "config": get_current_config(),
        }
    )


@app.get("/api/tags")
def api_tags(page: int = 1, per_page: int = 10):
    safe_per_page = max(1, min(per_page, 30))
    tags = _load_tags_static()

    total = len(tags)
    total_pages = max(1, (total + safe_per_page - 1) // safe_per_page)
    safe_page = min(max(page, 1), total_pages)
    start = (safe_page - 1) * safe_per_page
    end = start + safe_per_page

    return JSONResponse(
        {
            "ok": True,
            "items": tags[start:end],
            "pagination": {
                "page": safe_page,
                "per_page": safe_per_page,
                "total": total,
                "total_pages": total_pages,
            },
        }
    )


@app.get("/api/waterfall")
def api_waterfall(tag: str = "", page: int = 1):
    cfg = get_current_config()
    tag_code = str(tag or "").strip()
    if not _validate_waterfall_tag(cfg, tag_code):
        return JSONResponse({"ok": False, "error": "无效标签"}, status_code=400)

    waterfall_cfg = get_waterfall_config(cfg)
    per_page = int(waterfall_cfg["per_page"])
    safe_page = max(1, int(page))
    proxies = build_proxies(str(cfg.get("proxy", "")).strip())
    session = requests.Session()
    try:
        params = build_media_request_params(waterfall_cfg, tag_code=tag_code, per_page=per_page, page=safe_page)
        resp = session.get(MEDIA_API_URL, params=params, timeout=REQUEST_TIMEOUT, proxies=proxies)
        resp.raise_for_status()
        payload = resp.json()
        raw_items = payload.get("items", [])
        if not isinstance(raw_items, list):
            raise ValueError("API 返回的 items 不是数组")
        items = [
            normalized
            for normalized in (_normalize_waterfall_item(item, tag_code) for item in raw_items)
            if normalized is not None
        ]
        return JSONResponse({
            "ok": True,
            "tag": tag_code,
            "tabs": _waterfall_tabs(cfg),
            "items": items,
            "config": waterfall_cfg,
            "pagination": {
                "page": safe_page,
                "per_page": per_page,
                "has_next": len(raw_items) >= per_page,
            },
        })
    except Exception as exc:
        append_log(f"瀑布流预览加载失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)


@app.post("/api/waterfall/download")
async def api_waterfall_download(request: Request):
    body = await request.json()
    tag_code = str(body.get("tag_code", "")).strip()
    raw_items = body.get("items", [])
    if not isinstance(raw_items, list):
        return JSONResponse({"ok": False, "error": "items 必须是数组"}, status_code=400)

    cfg = get_current_config()
    if not _validate_waterfall_tag(cfg, tag_code):
        return JSONResponse({"ok": False, "error": "无效标签"}, status_code=400)

    items = [
        normalized
        for normalized in (_normalize_waterfall_item(item, tag_code) for item in raw_items)
        if normalized is not None
    ]
    if not items:
        return JSONResponse({"ok": False, "error": "没有可下载的视频"}, status_code=400)

    root = resolve_download_root(cfg["download_root"])
    target_dir = resolve_target_dir(cfg, root, tag_code=tag_code)
    root.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    proxies = build_proxies(str(cfg.get("proxy", "")).strip())
    session = requests.Session()
    success_count, skip_count, fail_count = _download_items(
        session,
        items,
        target_dir,
        max_count=len(items),
        proxies=proxies,
    )
    append_log(
        f"瀑布流手动下载完成：分类={get_tag_name(tag_code) or '全部'}，"
        f"成功 {success_count}，跳过 {skip_count}，失败 {fail_count}"
    )
    return JSONResponse({
        "ok": True,
        "target_dir": str(target_dir),
        "success": success_count,
        "skipped": skip_count,
        "failed": fail_count,
    })


@app.get("/health")
def health():
    return {"status": "ok"}


VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}


def resolve_media_folder(root: Path, folder: str) -> Optional[Path]:
    """把 folder 参数（空=根目录 / 一级子文件夹名）解析为安全的目录路径。

    非法（含路径分隔符、.. 或越权、非目录）时返回 None。
    """
    folder = str(folder or "").strip()
    if not folder:
        return root
    if "/" in folder or "\\" in folder or folder in {".", ".."}:
        return None
    candidate = root / folder
    try:
        resolved = candidate.resolve()
    except OSError:
        return None
    if resolved.parent != root.resolve() or not candidate.is_dir():
        return None
    return candidate


def _count_videos(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(
        1 for p in directory.iterdir()
        if p.is_file() and p.suffix.lower() in VIDEO_EXTS
    )


def _media_folders(download_root: Path) -> List[dict]:
    """返回含视频的文件夹列表：根目录（folder=""）+ 各一级子文件夹。"""
    folders: List[dict] = []
    if not download_root.exists():
        return folders
    root_count = _count_videos(download_root)
    if root_count > 0:
        folders.append({"folder": "", "count": root_count})
    for d in sorted(p for p in download_root.iterdir() if p.is_dir()):
        count = _count_videos(d)
        if count > 0:
            folders.append({"folder": d.name, "count": count})
    return folders


def _list_folder_items(directory: Path) -> List[dict]:
    items = []
    if not directory.exists():
        return items
    videos = {p.stem: p for p in directory.iterdir() if p.is_file() and p.suffix.lower() in VIDEO_EXTS}
    thumbs = {p.stem: p for p in directory.iterdir() if p.is_file() and p.suffix.lower() in IMAGE_EXTS}
    for stem, vp in videos.items():
        tp = thumbs.get(stem)
        items.append({
            "stem": stem,
            "video": vp.name,
            "thumb": tp.name if tp else None,
            "size": vp.stat().st_size,
        })
    items.sort(key=lambda x: x["stem"])
    return items


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _probe_video_duration(video_path: Path) -> Optional[str]:
    ffprobe_cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    try:
        result = subprocess.run(
            ffprobe_cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None

    if result.returncode != 0:
        return None

    output = result.stdout.strip()
    if not output:
        return None

    try:
        return _format_duration(float(output))
    except ValueError:
        return None


def _build_poster_item(folder: str, item: dict) -> dict:
    thumb_name = item.get("thumb")
    video_name = item.get("video")
    folder_q = quote(folder)
    return {
        **item,
        "folder": folder,
        "thumbnail_url": (
            f"/api/poster-thumb?folder={folder_q}&name={quote(thumb_name)}"
            if thumb_name
            else None
        ),
        "video_url": f"/api/poster-video?folder={folder_q}&name={quote(video_name)}",
        "duration": None,
    }


def _collect_poster_items(download_root: Path, folder: Optional[str] = None) -> List[dict]:
    if folder is None:
        folder_names = [f["folder"] for f in _media_folders(download_root)]
    else:
        folder_names = [folder]
    items: List[dict] = []
    for name in folder_names:
        directory = resolve_media_folder(download_root, name)
        if directory is None:
            continue
        for item in _list_folder_items(directory):
            built = _build_poster_item(name, item)
            built["duration"] = _probe_video_duration(directory / item["video"])
            items.append(built)

    items.sort(key=lambda x: (x["folder"], x["stem"]), reverse=True)
    return items


@app.get("/api/poster")
def api_poster_all(folder: Optional[str] = None):
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    if folder is not None and resolve_media_folder(root, folder) is None:
        return JSONResponse({"ok": False, "error": "无效文件夹"}, status_code=400)
    items = _collect_poster_items(root, folder=folder)
    return JSONResponse({
        "ok": True,
        "folder": folder,
        "folders": _media_folders(root),
        "items": items,
    })


@app.get("/api/poster-thumb")
def api_thumb(folder: str = "", name: str = ""):
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    directory = resolve_media_folder(root, folder)
    if directory is None or "/" in name or "\\" in name:
        return JSONResponse({"error": "无效路径"}, status_code=400)
    path = directory / name
    if not path.exists() or not path.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    resolved = path.resolve()
    if not str(resolved).startswith(str(root.resolve())):
        return JSONResponse({"error": "禁止访问"}, status_code=403)
    return FileResponse(str(resolved))


@app.get("/api/poster-video")
def api_video(folder: str = "", name: str = ""):
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    directory = resolve_media_folder(root, folder)
    if directory is None or "/" in name or "\\" in name:
        return JSONResponse({"error": "无效路径"}, status_code=400)
    path = directory / name
    if not path.exists() or not path.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    if path.suffix.lower() not in VIDEO_EXTS:
        return JSONResponse({"error": "不是视频文件"}, status_code=400)
    resolved = path.resolve()
    if not str(resolved).startswith(str(root.resolve())):
        return JSONResponse({"error": "禁止访问"}, status_code=403)
    return FileResponse(str(resolved))


@app.post("/api/poster/delete")
async def api_delete(request: Request):
    body = await request.json()
    folder = str(body.get("folder", ""))
    stems: List[str] = body.get("stems", [])
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    directory = resolve_media_folder(root, folder)
    if directory is None:
        return JSONResponse({"ok": False, "error": "无效文件夹"}, status_code=400)
    deleted = []
    for stem in stems:
        if "/" in str(stem) or "\\" in str(stem):
            continue
        for p in list(directory.glob(f"{stem}.*")):
            if p.suffix.lower() in VIDEO_EXTS | IMAGE_EXTS:
                p.unlink(missing_ok=True)
                deleted.append(p.name)
    return JSONResponse({"ok": True, "deleted": deleted})


@app.post("/api/poster/replace-cover")
async def api_replace_cover(folder: str = Form(""), stem: str = Form(...), file: UploadFile = File(...)):
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    directory = resolve_media_folder(root, folder)
    if directory is None or "/" in stem or "\\" in stem:
        return JSONResponse({"ok": False, "error": "无效路径"}, status_code=400)
    suffix = Path(file.filename or "cover.jpg").suffix.lower() or ".jpg"
    if suffix not in IMAGE_EXTS:
        return JSONResponse({"ok": False, "error": "不支持的图片格式"}, status_code=400)
    # Remove old thumb files for this stem
    for p in list(directory.glob(f"{stem}.*")):
        if p.suffix.lower() in IMAGE_EXTS:
            p.unlink(missing_ok=True)
    new_path = directory / f"{stem}{suffix}"
    content = await file.read()
    new_path.write_bytes(content)
    return JSONResponse({"ok": True, "thumb": new_path.name})


@app.get("/poster")
def poster_page(request: Request, folder: str = ""):
    return templates.TemplateResponse("poster.html", {"request": request, "folder": folder})


@app.get("/waterfall")
def waterfall_page(request: Request):
    cfg = get_current_config()
    return templates.TemplateResponse(
        "waterfall.html",
        {
            "request": request,
            "tabs": _waterfall_tabs(cfg),
            "config": get_waterfall_config(cfg),
            "time_filter_options": TIME_FILTER_OPTIONS,
            "sort_options": SORT_OPTIONS,
            "range_options": RANGE_OPTIONS,
            "page_size_options": sorted(ALLOWED_WATERFALL_PAGE_SIZES),
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=2617)
