import json
import subprocess
import sys
import threading
import time
from copy import deepcopy
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, urlparse

import requests
from fastapi import FastAPI, File, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates


# 适配 PyInstaller 路径处理
def get_resource_path(relative_path: str) -> Path:
    """获取资源文件的绝对路径，适配开发环境和 PyInstaller 编译环境"""
    if getattr(sys, "frozen", False):
        base_path = Path(sys._MEIPASS)
    else:
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
MEDIA_API_URL = "https://twitter-ero-video-ranking.com/api/media"
REQUEST_TIMEOUT = 30
TIME_FILTER_MIN = 0
TIME_FILTER_MAX = 10800
TIME_FILTER_MAX_MINUTES = TIME_FILTER_MAX // 60
ALLOWED_SORTS = {"time", "favorite", "pv"}
ALLOWED_RANGES = {"daily", "weekly", "monthly", "all"}
VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

DEFAULT_CONFIG: Dict[str, object] = {
    "source_url": MEDIA_API_URL,
    "download_root": "downloads",
    "proxy": "",
    "download_limit": 10,
    "video_format": "original",
    "download_thumbnails": True,
    "sort": "pv",
    "range": "daily",
    "min_time": TIME_FILTER_MIN,
    "max_time": TIME_FILTER_MAX,
    "tag_code": "",
}

app = FastAPI(title="twitter-ero-video-ranking-downloader")
templates = Jinja2Templates(directory=str(TEMPLATES_PATH))

config_lock = threading.Lock()
log_lock = threading.Lock()
runtime_lock = threading.Lock()


TAGS_JSON_PATH = get_resource_path("templates/code.json")
_tags_cache: Optional[List[Dict[str, object]]] = None


def _new_runtime_state() -> Dict[str, object]:
    return {
        "is_running": False,
        "status": "idle",
        "last_run_time": None,
        "finished_at": None,
        "last_result": "尚未执行",
        "current_stage": "等待开始",
        "current_item": "",
        "progress_percent": 0,
        "target_count": 0,
        "fetched_count": 0,
        "processed_count": 0,
        "completed_count": 0,
        "skip_count": 0,
        "fail_count": 0,
        "active_config": None,
    }


runtime_state = _new_runtime_state()
log_lines: List[str] = []


# 统一记录日志，供页面和接口轮询展示。
def append_log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_lock:
        log_lines.append(f"[{timestamp}] {message}")
        if len(log_lines) > 300:
            del log_lines[:-300]



def get_logs() -> List[str]:
    with log_lock:
        return list(log_lines)



def parse_bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(f"{field_name}必须是布尔值")



def validate_config(raw: Dict[str, object]) -> Dict[str, object]:
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(raw or {})

    cfg["source_url"] = MEDIA_API_URL

    download_root = str(cfg.get("download_root", "")).strip()
    if not download_root:
        raise ValueError("保存路径不能为空")
    cfg["download_root"] = download_root

    proxy = str(cfg.get("proxy", "")).strip()
    cfg["proxy"] = proxy

    try:
        download_limit = int(cfg.get("download_limit", 0))
    except (TypeError, ValueError) as exc:
        raise ValueError("下载数量必须是整数") from exc
    if download_limit <= 0 or download_limit > 100:
        raise ValueError("下载数量必须在 1 到 100 之间")
    cfg["download_limit"] = download_limit

    cfg["video_format"] = "original"

    cfg["download_thumbnails"] = parse_bool(cfg.get("download_thumbnails", True), "封面下载开关")

    sort = str(cfg.get("sort", "pv")).strip()
    if sort not in ALLOWED_SORTS:
        raise ValueError("排序方式必须是 time、favorite 或 pv")
    cfg["sort"] = sort

    range_value = str(cfg.get("range", "daily")).strip()
    if range_value not in ALLOWED_RANGES:
        raise ValueError("时间范围必须是 daily、weekly、monthly 或 all")
    cfg["range"] = range_value

    try:
        min_time = int(cfg.get("min_time", TIME_FILTER_MIN))
        max_time = int(cfg.get("max_time", TIME_FILTER_MAX))
    except (TypeError, ValueError) as exc:
        raise ValueError("视频时长筛选必须是整数") from exc

    duration_minutes_raw = cfg.get("duration_minutes", None)
    if duration_minutes_raw is not None and str(duration_minutes_raw).strip() != "":
        try:
            duration_minutes = int(duration_minutes_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("时长分钟数必须是整数") from exc

        if duration_minutes < 0 or duration_minutes > TIME_FILTER_MAX_MINUTES:
            raise ValueError(f"时长分钟数必须在 0 到 {TIME_FILTER_MAX_MINUTES} 之间")

        if duration_minutes == 0:
            min_time = TIME_FILTER_MIN
            max_time = TIME_FILTER_MAX
        else:
            seconds = duration_minutes * 60
            min_time = seconds
            max_time = seconds
    if min_time < TIME_FILTER_MIN or min_time > TIME_FILTER_MAX:
        raise ValueError(f"最小时长必须在 {TIME_FILTER_MIN} 到 {TIME_FILTER_MAX} 秒之间")
    if max_time < TIME_FILTER_MIN or max_time > TIME_FILTER_MAX:
        raise ValueError(f"最大时长必须在 {TIME_FILTER_MIN} 到 {TIME_FILTER_MAX} 秒之间")
    if min_time > max_time:
        raise ValueError("最小时长不能大于最大时长")
    cfg["min_time"] = min_time
    cfg["max_time"] = max_time

    cfg.pop("duration_minutes", None)

    tag_code = str(cfg.get("tag_code", "")).strip()
    cfg["tag_code"] = tag_code
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
        return validate_config(raw)
    except Exception as exc:
        append_log(f"读取配置失败，已回退默认配置：{exc}")
        save_config(DEFAULT_CONFIG)
        return dict(DEFAULT_CONFIG)



def save_config(cfg: Dict[str, object]) -> Dict[str, object]:
    validated = validate_config(cfg)
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(validated, f, ensure_ascii=False, indent=2)
    return validated



def get_current_config() -> Dict[str, object]:
    with config_lock:
        return load_config()



def summarize_config(cfg: Dict[str, object]) -> Dict[str, object]:
    return {
        "download_root": str(resolve_download_root(cfg["download_root"])),
        "download_limit": int(cfg["download_limit"]),
        "download_thumbnails": bool(cfg["download_thumbnails"]),
        "sort": cfg["sort"],
        "range": cfg["range"],
        "min_time": int(cfg["min_time"]),
        "max_time": int(cfg["max_time"]),
        "tag_code": cfg.get("tag_code") or "",
        "proxy_enabled": bool(str(cfg.get("proxy", "")).strip()),
    }



def snapshot_runtime_state() -> Dict[str, object]:
    with runtime_lock:
        return deepcopy(runtime_state)



def _update_runtime_state(**kwargs: object) -> None:
    with runtime_lock:
        runtime_state.update(kwargs)



def _mark_runtime_started(cfg: Dict[str, object]) -> None:
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with runtime_lock:
        runtime_state.clear()
        runtime_state.update(_new_runtime_state())
        runtime_state.update(
            {
                "is_running": True,
                "status": "running",
                "last_run_time": started_at,
                "last_result": "任务启动中",
                "current_stage": "准备下载配置",
                "active_config": summarize_config(cfg),
            }
        )



def _mark_runtime_finished(status: str, result: str, **extra: object) -> None:
    payload = {
        "is_running": False,
        "status": status,
        "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_result": result,
        "progress_percent": 100 if status == "success" else runtime_state.get("progress_percent", 0),
    }
    payload.update(extra)
    _update_runtime_state(**payload)



def get_file_ext_from_url(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    ext = Path(parsed.path).suffix.lower()
    if ext:
        return ext
    return fallback



def resolve_video_ext(url: str, video_format: str) -> str:
    if video_format == "original":
        return get_file_ext_from_url(url, ".mp4")
    return f".{video_format}"



def build_proxies(proxy: str) -> Optional[Dict[str, str]]:
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}



def build_media_request_params(cfg: Dict[str, object]) -> Dict[str, object]:
    download_limit = int(cfg["download_limit"])
    min_time = int(cfg["min_time"])
    max_time = int(cfg["max_time"])
    params: Dict[str, object] = {
        "page": 1,
        "per_page": min(max(download_limit * 3, 30), 100),
        "ids": "",
        "isAnimeOnly": 0,
        "sort": str(cfg["sort"]),
        "min_time": min_time,
        "max_time": max_time,
    }
    tag_code = str(cfg.get("tag_code", "")).strip()
    if tag_code:
        params["category"] = tag_code
    range_value = str(cfg["range"])
    if range_value != "daily":
        params["range"] = range_value
    return params



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



def load_downloaded_url_hashes(day_dir: Path) -> set:
    marker_file = day_dir / ".downloaded_urls.txt"
    if not marker_file.exists():
        return set()
    try:
        return {line.strip() for line in marker_file.read_text(encoding="utf-8").splitlines() if line.strip()}
    except Exception as exc:
        append_log(f"读取去重记录失败：{exc}")
        return set()



def append_downloaded_url_hash(day_dir: Path, url_hash: str) -> None:
    marker_file = day_dir / ".downloaded_urls.txt"
    try:
        with marker_file.open("a", encoding="utf-8") as f:
            f.write(f"{url_hash}\n")
    except Exception as exc:
        append_log(f"写入去重记录失败：{exc}")



def _set_progress(current_stage: str, total_items: int, success_count: int, skip_count: int, fail_count: int, current_item: str = "") -> None:
    processed_count = success_count + skip_count + fail_count
    denominator = max(total_items, 1)
    progress_percent = min(99, int(processed_count / denominator * 100)) if total_items else 0
    _update_runtime_state(
        current_stage=current_stage,
        current_item=current_item,
        processed_count=processed_count,
        completed_count=success_count,
        skip_count=skip_count,
        fail_count=fail_count,
        progress_percent=progress_percent,
    )



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



def run_download_job(cfg: Dict[str, object]) -> None:
    try:
        cfg = validate_config(cfg)
        if not snapshot_runtime_state().get("is_running"):
            _mark_runtime_started(cfg)

        download_root = resolve_download_root(cfg["download_root"])
        download_root.mkdir(parents=True, exist_ok=True)
        day_dir = download_root / datetime.now().strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)

        proxy = str(cfg["proxy"]).strip()
        proxies = build_proxies(proxy)
        downloaded_hashes = load_downloaded_url_hashes(day_dir)
        session = requests.Session()

        append_log("开始执行即时下载任务")
        append_log(
            f"下载参数：limit={cfg['download_limit']} format={cfg['video_format']} "
            f"range={cfg['range']} sort={cfg['sort']} "
            f"time={int(cfg['min_time'])}-{int(cfg['max_time'])}s "
            f"tag={cfg.get('tag_code') or 'default'}"
        )
        _update_runtime_state(current_stage="请求媒体列表", progress_percent=5)

        resp = session.get(
            str(cfg["source_url"]),
            params=build_media_request_params(cfg),
            timeout=REQUEST_TIMEOUT,
            proxies=proxies,
        )
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("items", [])
        if not isinstance(items, list):
            raise ValueError("API 返回的 items 不是数组")

        total_items = len(items)
        target_count = int(cfg["download_limit"])
        _update_runtime_state(
            current_stage="媒体列表已获取",
            fetched_count=total_items,
            target_count=target_count,
            progress_percent=10,
        )
        if not items:
            result = "任务完成：未获取到可下载内容"
            append_log(result)
            _mark_runtime_finished("success", result, current_stage="无可下载数据", progress_percent=100)
            return

        success_count = 0
        skip_count = 0
        fail_count = 0

        for index, item in enumerate(items, start=1):
            if success_count >= target_count:
                append_log("已达到本次下载数量上限，停止继续下载")
                break

            current_item = f"第 {index} 项"
            if not isinstance(item, dict):
                skip_count += 1
                _set_progress("跳过无效条目", total_items, success_count, skip_count, fail_count, current_item)
                continue

            video_url = str(item.get("url", "")).strip()
            thumbnail_url = str(item.get("thumbnail", "")).strip()
            current_item = video_url or f"第 {index} 项"
            _update_runtime_state(current_stage="处理下载项", current_item=current_item)

            if not video_url:
                skip_count += 1
                append_log("条目缺少视频链接，已跳过")
                _set_progress("跳过缺少视频链接的条目", total_items, success_count, skip_count, fail_count, current_item)
                continue

            video_hash = sha256(video_url.encode("utf-8")).hexdigest()
            if video_hash in downloaded_hashes:
                skip_count += 1
                append_log("检测到重复视频 URL，已跳过")
                _set_progress("跳过重复视频", total_items, success_count, skip_count, fail_count, current_item)
                continue

            timestamp = f"{int(time.time() * 1000)}_{index}"
            video_ext = resolve_video_ext(video_url, str(cfg["video_format"]))
            thumb_ext = get_file_ext_from_url(thumbnail_url, ".jpg") if thumbnail_url else ".jpg"
            video_path = day_dir / f"{timestamp}{video_ext}"
            thumb_path = day_dir / f"{timestamp}{thumb_ext}"

            ok_video = download_binary(session, video_url, video_path, proxies)
            ok_thumb = True
            if ok_video and bool(cfg["download_thumbnails"]) and thumbnail_url:
                ok_thumb = download_binary(session, thumbnail_url, thumb_path, proxies)

            if ok_video and ok_thumb:
                success_count += 1
                downloaded_hashes.add(video_hash)
                append_downloaded_url_hash(day_dir, video_hash)
                append_log(f"下载完成：{video_path.name}")
                _set_progress("下载成功", total_items, success_count, skip_count, fail_count, current_item)
            else:
                fail_count += 1
                if ok_video and not ok_thumb and video_path.exists():
                    append_log(f"封面下载失败，已保留视频文件：{video_path.name}")
                _set_progress("下载失败", total_items, success_count, skip_count, fail_count, current_item)

            time.sleep(0.01)

        result = f"任务完成：成功 {success_count}，跳过 {skip_count}，失败 {fail_count}"
        append_log(result)
        _mark_runtime_finished(
            "success",
            result,
            current_stage="下载完成",
            current_item="",
            processed_count=success_count + skip_count + fail_count,
            completed_count=success_count,
            skip_count=skip_count,
            fail_count=fail_count,
            fetched_count=total_items,
            target_count=target_count,
            progress_percent=100,
        )
    except Exception as exc:
        err_msg = f"任务异常：{exc}"
        append_log(err_msg)
        _mark_runtime_finished("error", err_msg, current_stage="执行失败", current_item="")



def start_download_task(cfg: Dict[str, object]) -> Dict[str, object]:
    validated = validate_config(cfg)
    with runtime_lock:
        if runtime_state["is_running"]:
            raise RuntimeError("已有下载任务正在运行")
    _mark_runtime_started(validated)
    worker = threading.Thread(target=run_download_job, args=(validated,), daemon=True)
    worker.start()
    append_log("已创建即时下载任务")
    return snapshot_runtime_state()


@app.on_event("startup")
def on_startup() -> None:
    get_current_config()
    append_log("服务启动完成，当前模式：即时下载")


@app.on_event("shutdown")
def on_shutdown() -> None:
    append_log("服务已停止")


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    cfg = get_current_config()
    state = snapshot_runtime_state()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "config": cfg,
            "state": state,
            "logs": "\n".join(get_logs()),
            "time_filter_min": TIME_FILTER_MIN,
            "time_filter_max": TIME_FILTER_MAX,
            "sort_options": [
                {"value": "time", "label": "按时长"},
                {"value": "favorite", "label": "按点赞"},
                {"value": "pv", "label": "按观看数"},
            ],
            "range_options": [
                {"value": "daily", "label": "每日"},
                {"value": "weekly", "label": "每周"},
                {"value": "monthly", "label": "每月"},
                {"value": "all", "label": "全部"},
            ],
        },
    )


@app.get("/api/config")
def api_get_config():
    return JSONResponse({"ok": True, "config": get_current_config()})


@app.post("/api/config")
async def api_save_config(request: Request):
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise ValueError("请求体必须是 JSON 对象")
        cfg = save_config(payload)
        append_log("配置保存成功")
        return JSONResponse({"ok": True, "config": cfg})
    except Exception as exc:
        append_log(f"配置保存失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/download/start")
async def api_start_download(request: Request):
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise ValueError("请求体必须是 JSON 对象")
        config_payload = payload.get("config", payload)
        if not isinstance(config_payload, dict):
            raise ValueError("config 必须是对象")
        persist = parse_bool(payload.get("persist", True), "配置持久化开关")
        validated = validate_config(config_payload)
        if persist:
            save_config(validated)
        state = start_download_task(validated)
        return JSONResponse({"ok": True, "message": "下载任务已启动", "state": state})
    except RuntimeError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
    except Exception as exc:
        append_log(f"启动下载失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.get("/api/download/status")
def api_download_status():
    return JSONResponse({"ok": True, "state": snapshot_runtime_state(), "logs": get_logs(), "config": get_current_config()})


@app.get("/status")
def status():
    return api_download_status()


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



def _day_dirs(download_root: Path) -> List[Path]:
    if not download_root.exists():
        return []
    dirs = sorted(
        (d for d in download_root.iterdir() if d.is_dir() and d.name.isdigit() and len(d.name) == 8),
        reverse=True,
    )
    return dirs



def _list_day_items(day_dir: Path) -> List[dict]:
    items = []
    if not day_dir.exists():
        return items
    videos = {p.stem: p for p in day_dir.iterdir() if p.is_file() and p.suffix.lower() in VIDEO_EXTS}
    thumbs = {p.stem: p for p in day_dir.iterdir() if p.is_file() and p.suffix.lower() in IMAGE_EXTS}
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



def _build_poster_item(date: str, item: dict) -> dict:
    thumb_name = item.get("thumb")
    video_name = item.get("video")
    return {
        **item,
        "date": date,
        "thumbnail_url": (
            f"/api/poster/{date}/thumb/{quote(thumb_name)}"
            if thumb_name
            else None
        ),
        "video_url": f"/api/poster/{date}/video/{quote(video_name)}",
        "duration": None,
    }



def _collect_poster_items(download_root: Path, date: str = "") -> List[dict]:
    dates = [date] if date else [d.name for d in _day_dirs(download_root)]
    items: List[dict] = []
    for current_date in dates:
        day_dir = download_root / current_date
        for item in _list_day_items(day_dir):
            built = _build_poster_item(current_date, item)
            built["duration"] = _probe_video_duration(day_dir / item["video"])
            items.append(built)

    items.sort(key=lambda x: (x["date"], x["stem"]), reverse=True)
    return items



def _list_poster_days(download_root: Path) -> List[dict]:
    days = []
    for day_dir in _day_dirs(download_root):
        count = sum(1 for p in day_dir.iterdir() if p.is_file() and p.suffix.lower() in VIDEO_EXTS)
        if count > 0:
            days.append({"date": day_dir.name, "count": count})
    return days


@app.get("/api/poster-days")
def api_poster_days():
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    return JSONResponse({"days": _list_poster_days(root)})


@app.get("/api/poster")
def api_poster_all(date: str = ""):
    if date and (not date.isdigit() or len(date) != 8):
        return JSONResponse({"ok": False, "error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    items = _collect_poster_items(root, date=date)
    return JSONResponse({"ok": True, "date": date or None, "days": _list_poster_days(root), "items": items})


@app.get("/api/poster/{date}")
def api_poster_date(date: str):
    if not date.isdigit() or len(date) != 8:
        return JSONResponse({"ok": False, "error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    items = _collect_poster_items(root, date=date)
    return JSONResponse({"ok": True, "date": date, "items": items})


@app.get("/api/poster/{date}/thumb/{filename}")
def api_thumb(date: str, filename: str):
    if not date.isdigit() or len(date) != 8:
        return JSONResponse({"error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    path = root / date / filename
    if not path.exists() or not path.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    resolved = path.resolve()
    if not str(resolved).startswith(str(root.resolve())):
        return JSONResponse({"error": "禁止访问"}, status_code=403)
    return FileResponse(str(resolved))


@app.get("/api/poster/{date}/video/{filename}")
def api_video(date: str, filename: str):
    if not date.isdigit() or len(date) != 8:
        return JSONResponse({"error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    path = root / date / filename
    if not path.exists() or not path.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    if path.suffix.lower() not in VIDEO_EXTS:
        return JSONResponse({"error": "不是视频文件"}, status_code=400)
    resolved = path.resolve()
    if not str(resolved).startswith(str(root.resolve())):
        return JSONResponse({"error": "禁止访问"}, status_code=403)
    return FileResponse(str(resolved))


@app.delete("/api/poster/{date}/{stem}")
def api_delete_item(date: str, stem: str):
    if not date.isdigit() or len(date) != 8:
        return JSONResponse({"ok": False, "error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    day_dir = root / date
    deleted = []
    for p in list(day_dir.glob(f"{stem}.*")):
        if p.suffix.lower() in VIDEO_EXTS | IMAGE_EXTS:
            p.unlink(missing_ok=True)
            deleted.append(p.name)
    return JSONResponse({"ok": True, "deleted": deleted})


@app.post("/api/poster/{date}/batch-delete")
async def api_batch_delete(date: str, request: Request):
    if not date.isdigit() or len(date) != 8:
        return JSONResponse({"ok": False, "error": "无效日期"}, status_code=400)
    body = await request.json()
    stems: List[str] = body.get("stems", [])
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    day_dir = root / date
    deleted = []
    for stem in stems:
        for p in list(day_dir.glob(f"{stem}.*")):
            if p.suffix.lower() in VIDEO_EXTS | IMAGE_EXTS:
                p.unlink(missing_ok=True)
                deleted.append(p.name)
    return JSONResponse({"ok": True, "deleted": deleted})


@app.post("/api/poster/{date}/{stem}/replace-cover")
async def api_replace_cover(date: str, stem: str, file: UploadFile = File(...)):
    if not date.isdigit() or len(date) != 8:
        return JSONResponse({"ok": False, "error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    day_dir = root / date
    if not day_dir.exists():
        return JSONResponse({"ok": False, "error": "日期目录不存在"}, status_code=404)
    suffix = Path(file.filename or "cover.jpg").suffix.lower() or ".jpg"
    if suffix not in IMAGE_EXTS:
        return JSONResponse({"ok": False, "error": "不支持的图片格式"}, status_code=400)
    for p in list(day_dir.glob(f"{stem}.*")):
        if p.suffix.lower() in IMAGE_EXTS:
            p.unlink(missing_ok=True)
    new_path = day_dir / f"{stem}{suffix}"
    content = await file.read()
    new_path.write_bytes(content)
    return JSONResponse({"ok": True, "thumb": new_path.name})


@app.get("/poster")
def poster_page(request: Request, date: str = ""):
    return templates.TemplateResponse("poster.html", {"request": request, "date": date})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=2617)
