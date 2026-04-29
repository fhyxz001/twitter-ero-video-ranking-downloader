import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, urlparse

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
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
MEDIA_API_URL = "https://twitter-ero-video-ranking.com/api/media"
REQUEST_TIMEOUT = 30
TIME_FILTER_MIN = 0
TIME_FILTER_MAX = 1800
ALLOWED_SORTS = {"time", "favorite", "pv"}
ALLOWED_RANGES = {"daily", "weekly", "monthly", "all"}

DEFAULT_CONFIG: Dict[str, object] = {
    "download_root": "/data/downloads" if os.name != "nt" else str(APP_DIR / "downloads"),
    "proxy": "",
    "schedule_time": "03:00",
    "max_daily_downloads": 20,
    "sort": "favorite",
    "range": "daily",
    "min_time": TIME_FILTER_MIN,
    "max_time": TIME_FILTER_MAX,
}

app = FastAPI(title="twitter-ero-video-ranking-downloader")
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


def append_log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_lock:
        log_lines.append(f"[{timestamp}] {message}")
        if len(log_lines) > 300:
            del log_lines[:-300]


def get_logs() -> List[str]:
    with log_lock:
        return list(log_lines)


def validate_config(raw: Dict[str, object]) -> Dict[str, object]:
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(raw or {})

    download_root = str(cfg.get("download_root", "")).strip()
    if not download_root:
        raise ValueError("下载根目录不能为空")
    cfg["download_root"] = download_root

    proxy = str(cfg.get("proxy", "")).strip()
    cfg["proxy"] = proxy

    sort = str(cfg.get("sort", "favorite")).strip()
    if sort not in ALLOWED_SORTS:
        raise ValueError("排序方式必须是 time、favorite 或 pv")
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
        raise ValueError("每日最大下载数量必须大于0")
    cfg["max_daily_downloads"] = max_daily

    min_time = int(cfg.get("min_time", TIME_FILTER_MIN))
    max_time = int(cfg.get("max_time", TIME_FILTER_MAX))
    if min_time < TIME_FILTER_MIN or min_time > TIME_FILTER_MAX:
        raise ValueError(f"最小时长必须在 {TIME_FILTER_MIN} 到 {TIME_FILTER_MAX} 秒之间")
    if max_time < TIME_FILTER_MIN or max_time > TIME_FILTER_MAX:
        raise ValueError(f"最大时长必须在 {TIME_FILTER_MIN} 到 {TIME_FILTER_MAX} 秒之间")
    if min_time > max_time:
        raise ValueError("最小时长不能大于最大时长")
    cfg["min_time"] = min_time
    cfg["max_time"] = max_time
    return cfg


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


def save_config(cfg: Dict[str, object]) -> None:
    validated = validate_config(cfg)
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(validated, f, ensure_ascii=False, indent=2)


def get_current_config() -> Dict[str, object]:
    with config_lock:
        return load_config()


def update_schedule(cfg: Dict[str, object]) -> None:
    hour, minute = cfg["schedule_time"].split(":")
    scheduler.remove_all_jobs()
    scheduler.add_job(
        run_download_job,
        trigger=CronTrigger(hour=int(hour), minute=int(minute)),
        id="daily_download_job",
        replace_existing=True,
    )
    append_log(f"定时任务已更新：每天 {cfg['schedule_time']} 执行")


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


def build_media_request_params(cfg: Dict[str, object]) -> Dict[str, object]:
    params: Dict[str, object] = {
        "page": 1,
        "per_page": 30,
        "category": "",
        "ids": "",
        "isAnimeOnly": 0,
        "sort": str(cfg["sort"]),
        "min_time": int(cfg["min_time"]),
        "max_time": int(cfg["max_time"]),
    }
    range_value = str(cfg["range"])
    if range_value != "daily":
        params["range"] = range_value
    return params


def already_downloaded(day_dir: Path) -> int:
    if not day_dir.exists():
        return 0
    # 只统计视频文件，避免封面数量干扰每日限制判断。
    count = 0
    for item in day_dir.iterdir():
        if item.is_file() and item.suffix.lower() in {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv"}:
            count += 1
    return count


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
        download_root = Path(str(cfg["download_root"])).expanduser().resolve()
        download_root.mkdir(parents=True, exist_ok=True)
        day_dir = download_root / datetime.now().strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)

        max_daily = int(cfg["max_daily_downloads"])
        already_count = already_downloaded(day_dir)
        downloaded_hashes = load_downloaded_url_hashes(day_dir)
        if already_count >= max_daily:
            msg = f"今日已下载 {already_count} 个视频，达到上限 {max_daily}，任务结束"
            append_log(msg)
            runtime_state["last_result"] = msg
            return

        proxy = str(cfg["proxy"]).strip()
        proxies = build_proxies(proxy)
        session = requests.Session()

        append_log(
            f"当前筛选：range={cfg['range']} sort={cfg['sort']} "
            f"time={cfg['min_time']}s-{cfg['max_time']}s"
        )
        resp = session.get(
            MEDIA_API_URL,
            params=build_media_request_params(cfg),
            timeout=REQUEST_TIMEOUT,
            proxies=proxies,
        )
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("items", [])
        if not isinstance(items, list):
            raise ValueError("API 返回的 items 不是数组")

        success_count = 0
        skip_count = 0
        fail_count = 0

        for item in items:
            if success_count + already_count >= max_daily:
                append_log("已达到当日最大下载数量，停止继续下载")
                break

            if not isinstance(item, dict):
                skip_count += 1
                continue

            video_url = str(item.get("url", "")).strip()
            thumbnail_url = str(item.get("thumbnail", "")).strip()
            if not video_url:
                skip_count += 1
                append_log("条目缺少 url，已跳过")
                continue
            video_hash = sha256(video_url.encode("utf-8")).hexdigest()
            if video_hash in downloaded_hashes:
                skip_count += 1
                append_log("检测到重复视频 URL，已跳过")
                continue

            timestamp = str(int(time.time() * 1000))
            video_ext = get_file_ext_from_url(video_url, ".mp4")
            thumb_ext = get_file_ext_from_url(thumbnail_url, ".jpg") if thumbnail_url else ".jpg"
            video_path = day_dir / f"{timestamp}{video_ext}"
            thumb_path = day_dir / f"{timestamp}{thumb_ext}"

            # 同名文件存在即认为本条已处理过，避免重复下载。
            if video_path.exists() or thumb_path.exists():
                skip_count += 1
                append_log(f"发现重复文件名 {timestamp}，已跳过")
                continue

            ok_video = download_binary(session, video_url, video_path, proxies)
            ok_thumb = True
            if thumbnail_url:
                ok_thumb = download_binary(session, thumbnail_url, thumb_path, proxies)

            if ok_video and ok_thumb:
                success_count += 1
                downloaded_hashes.add(video_hash)
                append_downloaded_url_hash(day_dir, video_hash)
                append_log(f"下载完成：{video_path.name}")
            else:
                fail_count += 1

            # 防止同毫秒命名冲突
            time.sleep(0.01)

        result = f"任务完成：成功 {success_count}，跳过 {skip_count}，失败 {fail_count}"
        append_log(result)
        runtime_state["last_result"] = result
    except Exception as exc:
        err_msg = f"任务异常：{exc}"
        append_log(err_msg)
        runtime_state["last_result"] = err_msg
    finally:
        runtime_state["is_running"] = False


@app.on_event("startup")
def on_startup() -> None:
    cfg = get_current_config()
    if not scheduler.running:
        scheduler.start()
    update_schedule(cfg)
    append_log("服务启动完成")


@app.on_event("shutdown")
def on_shutdown() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
    append_log("服务已停止")


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


@app.post("/save")
def save(
    download_root: str = Form(...),
    proxy: str = Form(""),
    schedule_time: str = Form(...),
    max_daily_downloads: int = Form(...),
    sort: str = Form(...),
    range: str = Form(...),
    min_time: int = Form(...),
    max_time: int = Form(...),
):
    try:
        cfg = {
            "download_root": download_root,
            "proxy": proxy,
            "schedule_time": schedule_time,
            "max_daily_downloads": max_daily_downloads,
            "sort": sort,
            "range": range,
            "min_time": min_time,
            "max_time": max_time,
        }
        save_config(cfg)
        update_schedule(get_current_config())
        append_log("配置保存成功")
        return RedirectResponse(url="/", status_code=303)
    except Exception as exc:
        append_log(f"配置保存失败：{exc}")
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


@app.get("/health")
def health():
    return {"status": "ok"}


VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}


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
    root = Path(str(cfg["download_root"])).expanduser().resolve()
    return JSONResponse({"days": _list_poster_days(root)})


@app.get("/api/poster")
def api_poster_all(date: str = ""):
    if date and (not date.isdigit() or len(date) != 8):
        return JSONResponse({"ok": False, "error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = Path(str(cfg["download_root"])).expanduser().resolve()
    items = _collect_poster_items(root, date=date)
    return JSONResponse({"ok": True, "date": date or None, "days": _list_poster_days(root), "items": items})


@app.get("/api/poster/{date}")
def api_poster_date(date: str):
    if not date.isdigit() or len(date) != 8:
        return JSONResponse({"ok": False, "error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = Path(str(cfg["download_root"])).expanduser().resolve()
    items = _collect_poster_items(root, date=date)
    return JSONResponse({"ok": True, "date": date, "items": items})


@app.get("/api/poster/{date}/thumb/{filename}")
def api_thumb(date: str, filename: str):
    if not date.isdigit() or len(date) != 8:
        return JSONResponse({"error": "无效日期"}, status_code=400)
    cfg = get_current_config()
    root = Path(str(cfg["download_root"])).expanduser().resolve()
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
    root = Path(str(cfg["download_root"])).expanduser().resolve()
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
    root = Path(str(cfg["download_root"])).expanduser().resolve()
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
    root = Path(str(cfg["download_root"])).expanduser().resolve()
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
    root = Path(str(cfg["download_root"])).expanduser().resolve()
    day_dir = root / date
    if not day_dir.exists():
        return JSONResponse({"ok": False, "error": "日期目录不存在"}, status_code=404)
    suffix = Path(file.filename or "cover.jpg").suffix.lower() or ".jpg"
    if suffix not in IMAGE_EXTS:
        return JSONResponse({"ok": False, "error": "不支持的图片格式"}, status_code=400)
    # Remove old thumb files for this stem
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
