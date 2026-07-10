import asyncio
import hashlib
import json
import os
import pickle
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

from contextlib import asynccontextmanager

import httpx
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.gzip import GZipMiddleware

from PIL import Image


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
MEDIA_API_BASE = "https://pektino.com/zh-CN"
RANKING_RANGE_SUFFIX = {
    "daily": "",
    "weekly": "/weekly",
    "monthly": "/monthly",
    "all": "/all",
}
RANKING_RANGE_OPTIONS = list(RANKING_RANGE_SUFFIX.keys())
REQUEST_TIMEOUT = 30
ALLOWED_WATERFALL_PAGE_SIZES = {10, 20, 30, 50, 100}

DEFAULT_CONFIG: Dict[str, object] = {
    "download_root": "/data/downloads",
    "proxy": "http://192.168.1.13:20171",
    "auto_download_enabled": True,
    "schedule_cron": "0 3 * * *",
    "max_daily_downloads": 10,
    "ranking_range": "daily",
    "waterfall_per_page": 10,
    "twitter_cookie": "",
    "twitter_blogger_list": [],
    "twitter_blogger_enabled": True,
    "twitter_blogger_cron": "0 4 * * *",
    "twitter_blogger_max_media": -1,
    "twitter_blogger_has_retweet": False,

    # --- OpenList 网盘上传 ---
    "openlist": {
        "enabled": False,
        "base_url": "http://192.168.1.13:5244",
        "token": "",
        "remote_root": "/115/tw",
        "overwrite": False,
        "upload_video": True,
        "upload_thumbnail": True,
        "auto_upload_after_download": True,
        "delete_local_after_upload": False,
        "timeout": 300,
        "max_retries": 2,
        "path_template": "{folder}/{filename}",
    },
}

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
app.add_middleware(GZipMiddleware, minimum_size=500)


# ── 缓存策略中间件 ──
@app.middleware("http")
async def add_cache_headers(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    elif path.startswith("/api/poster-thumb"):
        if "Cache-Control" not in response.headers:
            response.headers["Cache-Control"] = "public, max-age=86400"
        response.headers["Vary"] = "Accept-Encoding"
    elif path.startswith("/api/poster-video"):
        response.headers["Cache-Control"] = "public, max-age=3600"
    return response


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
blogger_state = {
    "is_running": False,
    "last_run_time": None,
    "last_result": "尚未执行",
}
log_lines: List[str] = []

# ── OpenList 网盘上传状态 ──
openlist_state = {
    "is_running": False,
    "last_run_time": None,
    "last_result": "尚未执行",
    "total_uploaded": 0,
    "total_failed": 0,
}
OPENLIST_UPLOAD_ENDPOINT = "/api/fs/put"
OPENLIST_LIST_ENDPOINT = "/api/fs/list"
OPENLIST_MKDIR_ENDPOINT = "/api/fs/mkdir"


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


# ── OpenList 网盘上传模块 ──

def _openlist_build_headers(token: str, file_path: str, content_length: int, overwrite: bool) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "File-Path": file_path,
        "Content-Length": str(content_length),
        "Overwrite": "true" if overwrite else "false",
    }


def _openlist_resolve_remote_path(template: str, remote_root: str, folder: str, filename: str) -> str:
    stem, ext = os.path.splitext(filename)
    path = template.format(folder=folder, filename=filename, stem=stem, ext=ext)
    # 确保以 remote_root 开头且不含 ..
    full = remote_root.rstrip("/") + "/" + path.lstrip("/")
    parts = full.replace("\\", "/").split("/")
    clean = []
    for p in parts:
        if p == "..":
            raise ValueError(f"路径包含 '..'：{full}")
        clean.append(p)
    return "/".join(clean)


def _openlist_ensure_remote_dir(base_url: str, token: str, remote_dir: str, proxies) -> bool:
    """确保远端目录存在，不存在则递归创建。"""
    url = f"{base_url}{OPENLIST_MKDIR_ENDPOINT}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = requests.post(url, headers=headers, json={"path": remote_dir, "mkdir": True}, proxies=proxies, timeout=15)
        return resp.status_code in (200, 201)
    except requests.RequestException as exc:
        append_log(f"[网盘] 创建远端目录失败 {remote_dir}：{exc}")
        return False


def _openlist_upload_file(local_path: Path, remote_path: str, cfg: dict, proxies) -> tuple[bool, str]:
    """上传单个文件，返回 (success, message)。"""
    token = cfg["token"]
    base_url = cfg["base_url"]
    overwrite = cfg["overwrite"]
    timeout = cfg["timeout"]
    max_retries = cfg["max_retries"]
    if not local_path.exists():
        return False, f"本地文件不存在：{local_path}"

    content_length = local_path.stat().st_size
    if content_length == 0:
        return False, f"文件大小为 0：{local_path.name}"

    file_path_encoded = remote_path
    headers = _openlist_build_headers(token, file_path_encoded, content_length, overwrite)
    url = f"{base_url}{OPENLIST_UPLOAD_ENDPOINT}"

    # 如果不覆盖，先检查远端是否已存在
    if not overwrite:
        list_url = f"{base_url}{OPENLIST_LIST_ENDPOINT}?path={urllib.parse.quote(remote_path.rsplit('/', 1)[0] if '/' in remote_path else '/')}"
        list_headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(list_url, headers=list_headers, proxies=proxies, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                remote_name = remote_path.rsplit("/", 1)[-1]
                existing = data.get("files") or data.get("data") or []
                if any(f.get("name") == remote_name for f in existing):
                    return False, f"远端已存在（跳过）：{remote_name}"
        except requests.RequestException:
            pass  # 检查失败就继续尝试上传

    last_err = ""
    for attempt in range(max_retries + 1):
        try:
            with open(local_path, "rb") as f:
                resp = requests.put(url, headers=headers, data=f, proxies=proxies, timeout=timeout)
            if resp.status_code in (200, 201):
                return True, f"上传完成：{local_path.name} ({content_length / 1024 / 1024:.1f} MB)"
            last_err = f"HTTP {resp.status_code} {resp.text[:200]}"
        except requests.RequestException as exc:
            last_err = str(exc)

        if attempt < max_retries:
            import time as _time
            _time.sleep(2 ** attempt)
            append_log(f"[网盘] {local_path.name} 重试 {attempt + 1}/{max_retries}：{last_err}")

    return False, f"上传失败：{last_err}"


def _openlist_upload_media(video_path: Path, thumb_path: Optional[Path], folder: str, cfg: dict, proxies) -> dict:
    """上传视频和封面，返回结果 dict。"""
    token = cfg["token"]
    base_url = cfg["base_url"]
    remote_root = cfg["remote_root"]
    remote_dir = remote_root.rstrip("/") + "/" + folder.lstrip("/")

    # 确保远端目录存在
    _openlist_ensure_remote_dir(base_url, token, remote_dir, proxies)

    result = {"video": None, "thumbnail": None, "ok": True}

    if cfg["upload_video"] and video_path:
        remote_path = _openlist_resolve_remote_path(
            cfg["path_template"], remote_root, folder, video_path.name
        )
        ok, msg = _openlist_upload_file(video_path, remote_path, cfg, proxies)
        result["video"] = {"path": remote_path, "ok": ok, "msg": msg}
        if not ok:
            result["ok"] = False
        if ok and cfg["delete_local_after_upload"]:
            try:
                video_path.unlink()
                append_log(f"[网盘] 已删除本地文件：{video_path.name}")
            except OSError as exc:
                append_log(f"[网盘] 删除本地文件失败 {video_path.name}：{exc}")

    if cfg["upload_thumbnail"] and thumb_path and thumb_path.exists():
        remote_thumb_root = remote_root.rstrip("/") + "/_thumbnails/" + folder.lstrip("/")
        _openlist_ensure_remote_dir(base_url, token, remote_thumb_root, proxies)
        remote_thumb_path = _openlist_resolve_remote_path(
            "{folder}/{filename}", remote_root, "_thumbnails/" + folder, thumb_path.name
        )
        ok, msg = _openlist_upload_file(thumb_path, remote_thumb_path, cfg, proxies)
        result["thumbnail"] = {"path": remote_thumb_path, "ok": ok, "msg": msg}

    return result


def _openlist_test_connection(base_url: str, token: str, proxies) -> dict:
    """测试连接 OpenList 服务。"""
    url = f"{base_url}{OPENLIST_LIST_ENDPOINT}?path=/"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, proxies=proxies, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return {"ok": True, "message": "连接成功"}
        return {"ok": False, "message": f"HTTP {resp.status_code}：{resp.text[:200]}"}
    except requests.RequestException as exc:
        return {"ok": False, "message": f"连接失败：{exc}"}


def _openlist_upload_all_local(cfg: dict, download_root: Path, proxies) -> dict:
    """扫描本地所有视频，批量上传。"""
    token = cfg["token"]
    base_url = cfg["base_url"]
    if not token or not base_url:
        return {"ok": False, "message": "网盘 Token 或地址未配置"}

    video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv"}
    total = 0
    success = 0
    failed = 0
    skipped = 0
    results = []

    for video_path in download_root.rglob("*"):
        if video_path.suffix.lower() not in video_exts:
            continue
        # 跳过 blogger 目录
        rel = video_path.relative_to(download_root)
        parts = rel.parts
        if len(parts) >= 1 and parts[0] == "blogger":
            folder = "/".join(parts[:-1])
        else:
            folder = "/".join(parts[:-1]) if len(parts) > 1 else ""

        total += 1
        remote_path = _openlist_resolve_remote_path(
            cfg["path_template"], cfg["remote_root"], folder, video_path.name
        )

        # 检查已存在（不覆盖模式下）
        if not cfg["overwrite"]:
            list_url = f"{base_url}{OPENLIST_LIST_ENDPOINT}?path={urllib.parse.quote(remote_path.rsplit('/', 1)[0] if '/' in remote_path else '/')}"
            headers = {"Authorization": f"Bearer {token}"}
            try:
                resp = requests.get(list_url, headers=headers, proxies=proxies, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    remote_name = remote_path.rsplit("/", 1)[-1]
                    existing = data.get("files") or data.get("data") or []
                    if any(f.get("name") == remote_name for f in existing):
                        skipped += 1
                        append_log(f"[网盘] 批量跳过已存在：{rel}")
                        results.append({"file": str(rel), "ok": True, "msg": "跳过（已存在）"})
                        continue
            except requests.RequestException:
                pass

        remote_dir = cfg["remote_root"].rstrip("/") + "/" + folder
        _openlist_ensure_remote_dir(base_url, token, remote_dir, proxies)
        ok, msg = _openlist_upload_file(video_path, remote_path, cfg, proxies)
        if ok:
            success += 1
        else:
            failed += 1
        results.append({"file": str(rel), "ok": ok, "msg": msg})

    return {
        "ok": failed == 0,
        "total": total,
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "results": results,
        "message": f"批量上传：总计 {total}，成功 {success}，跳过 {skipped}，失败 {failed}",
    }


def parse_pektino_rsc(text: str) -> List[dict]:
    """Parse Next.js RSC payload from pektino.com to extract video items.

    The RSC payload contains obfuscated component references (e.g. $L13) and
    a JSON array under the key "initialItems". Each item has fields:
      id, url_cd, mp4 URL, thumbnail, pv, favorite, tweet_url, etc.

    We extract the initialItems JSON array using regex and parse it.
    """
    items: List[dict] = []
    # Find the initialItems JSON array in the RSC payload
    # The pattern looks for "initialItems" followed by a JSON array
    match = re.search(r'"initialItems"\s*:\s*(\[)', text)
    if not match:
        return items

    start = match.start(1)
    # Find the matching closing bracket by counting nesting depth
    depth = 0
    end = start
    for i in range(start, len(text)):
        ch = text[i]
        if ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end <= start:
        return items

    json_str = text[start:end]
    try:
        raw_items = json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        return items

    if not isinstance(raw_items, list):
        return items

    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        # Extract fields: id, url_cd, mp4 URL, thumbnail, pv, favorite, tweet_url
        video_id = str(raw.get("id", "")).strip()
        mp4_url = str(raw.get("mp4", "")).strip()
        thumb_url = str(raw.get("thumbnail", "")).strip()
        tweet_url = str(raw.get("tweet_url", "")).strip()
        url_cd = str(raw.get("url_cd", "")).strip()

        # Only real http/https URLs are valid for downloading.
        # url_cd is a short code (e.g. "Edz_jXxp39QO7Y4f"), not a download URL.
        video_url = mp4_url if mp4_url.startswith("http") else ""
        if not video_url:
            for key in ("url", "video_url", "src", "source"):
                candidate = str(raw.get(key, "")).strip()
                if candidate.startswith("http"):
                    video_url = candidate
                    break

        if not video_url:
            continue

        if not video_id:
            video_id = _generate_video_id(video_url)
        if not video_id:
            continue

        items.append({
            "id": video_id,
            "url": video_url,
            "thumbnail": thumb_url if thumb_url.startswith("http") else "",
            "title": str(raw.get("title", "")).strip() or video_id,
            "pv": raw.get("pv"),
            "favorite": raw.get("favorite"),
            "tweet_url": tweet_url,
            "url_cd": url_cd,
        })
    return items


def _generate_video_id(url: str) -> str:
    """Extract a stable ID from a video URL. Prefers Twitter's numeric ID in the path."""
    parsed = urlparse(url)
    path_parts = parsed.path.strip("/").split("/")
    # Look for a numeric Twitter-style ID (19 digits typical) in the path
    for part in path_parts:
        if part.isdigit() and len(part) >= 15:
            return part
    # Fallback: use the filename without extension
    filename = Path(parsed.path).stem
    if filename:
        return filename
    # Last resort: MD5 hash of the URL
    return hashlib.md5(url.encode()).hexdigest()[:16]


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

    schedule_cron = str(cfg.get("schedule_cron", "")).strip()
    try:
        CronTrigger.from_crontab(schedule_cron)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"定时执行 cron 表达式无效：{schedule_cron}") from exc
    cfg["schedule_cron"] = schedule_cron

    max_daily = int(cfg.get("max_daily_downloads", 0))
    if max_daily <= 0:
        raise ValueError("每次下载数必须大于0")
    cfg["max_daily_downloads"] = max_daily

    ranking_range = str(cfg.get("ranking_range", "daily")).strip()
    if ranking_range not in RANKING_RANGE_OPTIONS:
        ranking_range = "daily"
    cfg["ranking_range"] = ranking_range

    waterfall_per_page = int(cfg.get("waterfall_per_page", 10))
    if waterfall_per_page not in ALLOWED_WATERFALL_PAGE_SIZES:
        raise ValueError("瀑布流每页展示数量必须是 10、20、30、50 或 100")
    cfg["waterfall_per_page"] = waterfall_per_page

    # --- Twitter blogger config ---
    cfg["twitter_cookie"] = str(cfg.get("twitter_cookie", "")).strip()

    blogger_list = cfg.get("twitter_blogger_list", [])
    if isinstance(blogger_list, str):
        try:
            blogger_list = json.loads(blogger_list)
        except (json.JSONDecodeError, TypeError):
            blogger_list = []
    if not isinstance(blogger_list, list):
        blogger_list = []
    blogger_list = [
        str(name).strip().lstrip("@").lower()
        for name in blogger_list if str(name).strip()
    ]
    seen_bl = set()
    deduped_bl = []
    for name in blogger_list:
        if name not in seen_bl:
            seen_bl.add(name)
            deduped_bl.append(name)
    cfg["twitter_blogger_list"] = deduped_bl

    cfg["twitter_blogger_enabled"] = parse_bool(cfg.get("twitter_blogger_enabled", True))

    blogger_cron = str(cfg.get("twitter_blogger_cron", "0 4 * * *")).strip()
    try:
        CronTrigger.from_crontab(blogger_cron)
    except (ValueError, TypeError):
        blogger_cron = "0 4 * * *"
    cfg["twitter_blogger_cron"] = blogger_cron

    max_media = int(cfg.get("twitter_blogger_max_media", -1))
    if max_media < -1:
        max_media = -1
    if max_media > 500:
        max_media = 500
    cfg["twitter_blogger_max_media"] = max_media

    cfg["twitter_blogger_has_retweet"] = parse_bool(cfg.get("twitter_blogger_has_retweet", False))

    # --- OpenList 网盘上传配置校验 ---
    openlist_raw = cfg.get("openlist", {})
    if not isinstance(openlist_raw, dict):
        openlist_raw = {}
    ol = dict(openlist_raw)
    ol["enabled"] = parse_bool(ol.get("enabled", False))
    base_url = str(ol.get("base_url", "")).strip().rstrip("/")
    if ol["enabled"] and not base_url:
        raise ValueError("网盘上传已启用，但 base_url 为空")
    if base_url and not base_url.startswith("http"):
        raise ValueError(f"网盘 base_url 格式无效：{base_url}，须以 http:// 或 https:// 开头")
    ol["base_url"] = base_url
    ol["token"] = str(ol.get("token", "")).strip()
    remote_root = str(ol.get("remote_root", "")).strip()
    if ol["enabled"] and not remote_root:
        raise ValueError("网盘上传已启用，但 remote_root 为空")
    if remote_root and not remote_root.startswith("/"):
        remote_root = "/" + remote_root
    ol["remote_root"] = remote_root
    ol["overwrite"] = parse_bool(ol.get("overwrite", False))
    ol["upload_video"] = parse_bool(ol.get("upload_video", True))
    ol["upload_thumbnail"] = parse_bool(ol.get("upload_thumbnail", True))
    ol["auto_upload_after_download"] = parse_bool(ol.get("auto_upload_after_download", True))
    ol["delete_local_after_upload"] = parse_bool(ol.get("delete_local_after_upload", False))
    ol["timeout"] = max(30, min(3600, int(ol.get("timeout", 300))))
    ol["max_retries"] = max(0, min(5, int(ol.get("max_retries", 2))))
    ol["path_template"] = str(ol.get("path_template", "{folder}/{filename}"))
    # 校验 path_template 中只允许安全占位符，禁止 ..
    allowed_placeholders = {"{folder}", "{filename}", "{stem}", "{ext}"}
    import re as _re
    for ph in _re.findall(r"\{[^}]+\}", ol["path_template"]):
        if ph not in allowed_placeholders:
            raise ValueError(f"path_template 包含不支持的占位符：{ph}")
    if ".." in ol["path_template"]:
        raise ValueError("path_template 不允许包含 ..")
    cfg["openlist"] = ol

    return cfg


def resolve_download_root(download_root: object) -> Path:
    root = Path(str(download_root)).expanduser()
    if not root.is_absolute():
        root = APP_DIR / root
    return root.resolve()


def load_config() -> Dict[str, object]:
    # Docker volume 映射时如果宿主机文件不存在，会创建一个同名目录，需处理
    if CONFIG_PATH.is_dir():
        import shutil
        append_log(f"检测到 {CONFIG_PATH} 是目录而非文件，已自动移除并重建默认配置")
        shutil.rmtree(CONFIG_PATH, ignore_errors=True)
        save_config(DEFAULT_CONFIG)
        return dict(DEFAULT_CONFIG)
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
    scheduler.remove_all_jobs()
    if not bool(cfg.get("auto_download_enabled", True)):
        append_log("定时下载已关闭")
    else:
        cron_expr = str(cfg.get("schedule_cron", "0 3 * * *"))
        scheduler.add_job(
            run_download_job,
            trigger=CronTrigger.from_crontab(cron_expr),
            id="daily_download_job",
            replace_existing=True,
        )
        append_log(f"定时任务已更新：cron={cron_expr}")

    # Blogger crawl job
    if not bool(cfg.get("twitter_blogger_enabled", True)):
        append_log("博主定时爬取已关闭")
    else:
        blogger_cron = str(cfg.get("twitter_blogger_cron", "0 4 * * *"))
        scheduler.add_job(
            run_blogger_crawl_job,
            trigger=CronTrigger.from_crontab(blogger_cron),
            id="blogger_crawl_job",
            replace_existing=True,
        )
        append_log(f"博主定时爬取已更新：cron={blogger_cron}")


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


def _fetch_pektino_media(session: requests.Session, proxies, ranking_range: str = "daily") -> List[dict]:
    """从 pektino.com 获取排行榜视频列表，解析 Next.js RSC 负载返回 items 数组。"""
    suffix = RANKING_RANGE_SUFFIX.get(ranking_range, "")
    url = f"{MEDIA_API_BASE}{suffix}"
    headers = {
        "Accept": "text/x-component",
        "RSC": "1",
        "Next-Router-State-Tree": "%5B%22%22%2C%7B%22children%22%3A%5B%22zh-CN%22%2C%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%5D%7D%5D%7D%2Cnull%2Cnull%2Ctrue%5D",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    }
    resp = session.get(
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
        proxies=proxies,
    )
    resp.raise_for_status()
    raw_text = resp.text
    if not raw_text or not raw_text.strip():
        raise ValueError(f"API 返回空响应（HTTP {resp.status_code}），请检查接口或代理设置")
    items = parse_pektino_rsc(raw_text)
    if not items:
        raise ValueError("未能从页面解析到任何视频")
    return items


def _download_items(session: requests.Session, items: list, target_dir: Path, max_count: int, proxies, cfg: Optional[dict] = None) -> tuple:
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
            # 自动上传到网盘
            if cfg:
                ol = cfg.get("openlist", {})
                if ol.get("enabled") and ol.get("auto_upload_after_download"):
                    folder = target_dir.name
                    ol_proxies = proxies
                    try:
                        result = _openlist_upload_media(
                            video_path,
                            thumb_path if ok_thumb and thumb_path.exists() else None,
                            folder,
                            ol,
                            ol_proxies,
                        )
                        if result["ok"]:
                            append_log(f"[网盘] 自动上传完成：{video_path.name}")
                        else:
                            video_msg = result.get("video", {}).get("msg", "")
                            append_log(f"[网盘] 自动上传部分失败 {video_path.name}：{video_msg}")
                        if result.get("video", {}).get("ok") and ol.get("delete_local_after_upload"):
                            try:
                                video_path.unlink()
                                append_log(f"[网盘] 已删除本地文件：{video_path.name}")
                            except OSError as exc:
                                append_log(f"[网盘] 删除本地文件失败 {video_path.name}：{exc}")
                    except Exception as exc:
                        append_log(f"[网盘] 自动上传异常 {video_path.name}：{exc}")
        else:
            fail_count += 1

    return success_count, skip_count, fail_count


def _is_safe_remote_media_url(url: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _normalize_waterfall_item(item: dict) -> Optional[dict]:
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
        "title": str(item.get("title") or video_id),
        "pv": item.get("pv"),
        "favorite_count": item.get("favorite"),
        "tweet_url": str(item.get("tweet_url", "")).strip() or None,
    }


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

        max_downloads = int(cfg["max_daily_downloads"])
        proxy = str(cfg["proxy"]).strip()
        proxies = build_proxies(proxy)
        session = requests.Session()

        append_log(f"本次计划最多下载 {max_downloads} 个视频")

        ranking_range = str(cfg.get("ranking_range", "daily")).strip()
        try:
            items = _fetch_pektino_media(session, proxies, ranking_range)
            s, k, f = _download_items(session, items, download_root, max_downloads, proxies, cfg)
        except Exception as exc:
            s, k, f = 0, 0, 1
            append_log(f"下载任务失败：{exc}")

        result = (
            f"任务完成：计划最多 {max_downloads}，成功 {s}，"
            f"跳过 {k}，失败 {f}"
        )
        append_log(result)
        runtime_state["last_result"] = result
    except Exception as exc:
        err_msg = f"任务异常：{exc}"
        append_log(err_msg)
        runtime_state["last_result"] = err_msg
    finally:
        runtime_state["is_running"] = False


# ──────────────────────────────────────────────────
# Twitter 博主爬取核心逻辑
# ──────────────────────────────────────────────────

TWITTER_BEARER = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
TWITTER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
BLOGGER_DIR_NAME = "blogger"
BLOGGER_CACHE_FILE = "blogger_cache.pkl"
BLOGGER_INFO_CACHE_FILE = "blogger_info_cache.pkl"


def _build_twitter_headers(cookie: str) -> Dict[str, str]:
    import re as _re
    headers = {
        "user-agent": TWITTER_UA,
        "authorization": TWITTER_BEARER,
        "cookie": cookie,
    }
    ct0_match = _re.findall(r"ct0=(.*?);", cookie)
    if ct0_match:
        headers["x-csrf-token"] = ct0_match[0]
    return headers


def _quote_twitter_url(url: str) -> str:
    return url.replace("{", "%7B").replace("}", "%7D")


def _load_blogger_cache(download_root: Path) -> set:
    cache_path = download_root / BLOGGER_DIR_NAME / BLOGGER_CACHE_FILE
    if cache_path.exists():
        try:
            with cache_path.open("rb") as f:
                return pickle.load(f)
        except Exception:
            pass
    return set()


def _save_blogger_cache(download_root: Path, cache: set) -> None:
    cache_dir = download_root / BLOGGER_DIR_NAME
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / BLOGGER_CACHE_FILE
    with cache_path.open("wb") as f:
        pickle.dump(cache, f)


def _load_blogger_info_cache(download_root: Path) -> dict:
    """读取博主资料缓存（头像、简介等）。"""
    cache_path = download_root / BLOGGER_DIR_NAME / BLOGGER_INFO_CACHE_FILE
    if cache_path.exists():
        try:
            with cache_path.open("rb") as f:
                data = pickle.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}


def _save_blogger_info_cache(download_root: Path, cache: dict) -> None:
    """写入博主资料缓存。"""
    cache_dir = download_root / BLOGGER_DIR_NAME
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / BLOGGER_INFO_CACHE_FILE
    with cache_path.open("wb") as f:
        pickle.dump(cache, f)


def twitter_get_user_info(screen_name: str, headers: Dict[str, str], proxy: Optional[str]) -> Optional[dict]:
    """获取 Twitter 用户基本信息，返回 dict 或 None。"""
    url = (
        "https://twitter.com/i/api/graphql/xc8f1g7BYqr6VTzTbvNlGw/UserByScreenName?variables="
        '{"screen_name":"' + screen_name + '","withSafetyModeUserFields":false}'
        '&features={"hidden_profile_likes_enabled":false,"hidden_profile_subscriptions_enabled":false,'
        '"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,'
        '"subscriptions_verification_info_verified_since_enabled":true,"highlights_tweets_tab_ui_enabled":true,'
        '"creator_subscriptions_tweet_preview_api_enabled":true,'
        '"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,'
        '"responsive_web_graphql_timeline_navigation_enabled":true}'
        '&fieldToggles={"withAuxiliaryUserLabels":false}'
    )
    try:
        resp = httpx.get(_quote_twitter_url(url), headers=headers, proxy=proxy, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        data = resp.json()
        user_result = data["data"]["user"]["result"]
        return {
            "rest_id": user_result["rest_id"],
            "name": user_result["legacy"]["name"],
            "screen_name": user_result["legacy"]["screen_name"],
            "statuses_count": user_result["legacy"]["statuses_count"],
            "media_count": user_result["legacy"]["media_count"],
            "profile_image_url": user_result["legacy"].get("profile_image_url_https", ""),
            "description": user_result["legacy"].get("description", ""),
        }
    except Exception as exc:
        append_log(f"[博主] 获取 @{screen_name} 信息失败：{exc}")
        return None


def _get_highest_video_quality(variants: list) -> Optional[str]:
    if len(variants) == 1:
        return variants[0].get("url")
    max_bitrate = 0
    best_url = None
    for v in variants:
        if "bitrate" in v:
            br = int(v["bitrate"])
            if br > max_bitrate:
                max_bitrate = br
                best_url = v["url"]
    return best_url


def _msecs_to_label(msecs: int) -> str:
    t = time.localtime(msecs / 1000)
    return time.strftime("%Y-%m-%d %H-%M", t)


def twitter_fetch_tweets(
    rest_id: str,
    screen_name: str,
    cursor: Optional[str],
    headers: Dict[str, str],
    proxy: Optional[str],
    has_retweet: bool,
) -> tuple:
    """
    获取用户推文列表，返回 (media_list, next_cursor, has_more)。
    media_list 每项为 (url, filename_prefix, is_video)。
    """
    if has_retweet:
        url_top = (
            "https://twitter.com/i/api/graphql/2GIWTr7XwadIixZDtyXd4A/UserTweets?variables="
            '{"userId":"' + rest_id + '","count":20,'
        )
        url_bottom = (
            '"includePromotedContent":false,"withQuickPromoteEligibilityTweetFields":true,'
            '"withVoice":true,"withV2Timeline":true}'
            '&features={"rweb_lists_timeline_redesign_enabled":true,'
            '"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,'
            '"creator_subscriptions_tweet_preview_api_enabled":true,'
            '"responsive_web_graphql_timeline_navigation_enabled":true,'
            '"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,'
            '"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,'
            '"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,'
            '"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,'
            '"responsive_web_twitter_article_tweet_consumption_enabled":false,'
            '"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,'
            '"standardized_nudges_misinfo":true,'
            '"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,'
            '"longform_notetweets_rich_text_read_enabled":true,'
            '"longform_notetweets_inline_media_enabled":true,'
            '"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
            '&fieldToggles={"withAuxiliaryUserLabels":false,"withArticleRichContentState":false}'
        )
    else:
        url_top = (
            "https://twitter.com/i/api/graphql/Le6KlbilFmSu-5VltFND-Q/UserMedia?variables="
            '{"userId":"' + rest_id + '","count":500,'
        )
        url_bottom = (
            '"includePromotedContent":false,"withClientEventToken":false,'
            '"withBirdwatchNotes":false,"withVoice":true,"withV2Timeline":true}'
            '&features={"responsive_web_graphql_exclude_directive_enabled":true,'
            '"verified_phone_label_enabled":false,'
            '"creator_subscriptions_tweet_preview_api_enabled":true,'
            '"responsive_web_graphql_timeline_navigation_enabled":true,'
            '"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,'
            '"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,'
            '"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,'
            '"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,'
            '"responsive_web_twitter_article_tweet_consumption_enabled":false,'
            '"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,'
            '"standardized_nudges_misinfo":true,'
            '"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,'
            '"longform_notetweets_rich_text_read_enabled":true,'
            '"longform_notetweets_inline_media_enabled":true,'
            '"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
        )

    if cursor:
        url = url_top + '"cursor":"' + cursor + '",' + url_bottom
    else:
        url = url_top + url_bottom

    try:
        resp = httpx.get(
            _quote_twitter_url(url), headers=headers, proxy=proxy,
            timeout=REQUEST_TIMEOUT, follow_redirects=True,
        )
        raw_text = resp.text.strip()
        if "Rate limit exceeded" in raw_text:
            append_log(f"[博主] @{screen_name} API 次数已超限")
            return [], None, False
        raw_data = resp.json()
    except Exception as exc:
        append_log(f"[博主] @{screen_name} 获取推文失败：{exc}")
        return [], None, False

    # 解析响应结构
    try:
        if has_retweet:
            entries = raw_data["data"]["user"]["result"]["timeline_v2"]["timeline"]["instructions"][-1]["entries"]
        else:
            instructions = raw_data["data"]["user"]["result"]["timeline_v2"]["timeline"]["instructions"]
            entries = instructions[-1]["entries"] if instructions else []
    except (KeyError, IndexError, TypeError):
        append_log(f"[博主] @{screen_name} 解析推文数据失败")
        return [], None, False

    if has_retweet and entries and "cursor-top" in entries[0].get("entryId", ""):
        return [], None, False

    # 提取媒体
    media_list: List[tuple] = []
    next_cursor: Optional[str] = None

    for entry in entries:
        entry_id = entry.get("entryId", "")

        if "cursor-bottom" in entry_id:
            next_cursor = entry.get("content", {}).get("value")
            continue

        if "promoted-tweet" in entry_id:
            continue

        try:
            if has_retweet:
                if "tweet" not in entry_id:
                    continue
                item_content = entry.get("content", {})
                tweet_result = item_content.get("itemContent", {}).get("tweet_results", {}).get("result", {})
                if "tweet" in tweet_result:
                    legacy = tweet_result["tweet"]["legacy"]
                    edit_ctrl = tweet_result["tweet"].get("edit_control", {})
                else:
                    legacy = tweet_result.get("legacy", {})
                    edit_ctrl = tweet_result.get("edit_control", {})
            else:
                # UserMedia 模式
                items_container = entry.get("content", {}).get("items", [])
                if not items_container:
                    # 可能是 moduleItems 结构
                    module_items = entry.get("moduleItems", [])
                    if module_items:
                        items_container = [{"item": {"itemContent": mi.get("itemContent", {})}} for mi in module_items]
                for item_wrapper in items_container:
                    item_c = item_wrapper.get("item", {}).get("itemContent", {})
                    tweet_result = item_c.get("tweet_results", {}).get("result", {})
                    if "tweet" in tweet_result:
                        legacy = tweet_result["tweet"]["legacy"]
                        edit_ctrl = tweet_result["tweet"].get("edit_control", {})
                    else:
                        legacy = tweet_result.get("legacy", {})
                        edit_ctrl = tweet_result.get("edit_control", {})

                    # 检查是否为转推
                    if "retweeted_status_result" in legacy:
                        continue

                    msecs_str = edit_ctrl.get("editable_until_msecs", "0")
                    tweet_msecs = int(msecs_str) - 3600000 if msecs_str else 0
                    timestr = _msecs_to_label(tweet_msecs)

                    if "extended_entities" in legacy:
                        for media in legacy["extended_entities"]["media"]:
                            if "video_info" in media:
                                v_url = _get_highest_video_quality(media["video_info"]["variants"])
                                if v_url:
                                    media_list.append((v_url, f"{timestr}-vid", True))
                            else:
                                media_list.append((media["media_url_https"], f"{timestr}-img", False))
                continue  # 已处理 UserMedia 条目

            # has_retweet 模式的媒体提取
            if "retweeted_status_result" in legacy:
                rt_legacy = legacy["retweeted_status_result"]["result"]["legacy"]
                rt_msecs_str = tweet_result.get("edit_control", {}).get("editable_until_msecs", "0") if "tweet" in tweet_result else tweet_result.get("edit_control", {}).get("editable_until_msecs", "0")
                tweet_msecs = int(rt_msecs_str) - 3600000 if rt_msecs_str else 0
                timestr = _msecs_to_label(tweet_msecs)
                if "extended_entities" in rt_legacy:
                    for media in rt_legacy["extended_entities"]["media"]:
                        if "video_info" in media:
                            v_url = _get_highest_video_quality(media["video_info"]["variants"])
                            if v_url:
                                media_list.append((v_url, f"{timestr}-vid-rt", True))
                        else:
                            media_list.append((media["media_url_https"], f"{timestr}-img-rt", False))
            else:
                msecs_str = edit_ctrl.get("editable_until_msecs", "0")
                tweet_msecs = int(msecs_str) - 3600000 if msecs_str else 0
                timestr = _msecs_to_label(tweet_msecs)
                if "extended_entities" in legacy:
                    for media in legacy["extended_entities"]["media"]:
                        if "video_info" in media:
                            v_url = _get_highest_video_quality(media["video_info"]["variants"])
                            if v_url:
                                media_list.append((v_url, f"{timestr}-vid", True))
                        else:
                            media_list.append((media["media_url_https"], f"{timestr}-img", False))

        except Exception:
            continue

    # UserMedia 模式下一页 cursor
    if not has_retweet and not next_cursor:
        try:
            for entry in entries:
                if "bottom" in entry.get("entryId", ""):
                    next_cursor = entry.get("content", {}).get("value")
                    break
        except Exception:
            pass

    has_more = bool(next_cursor) and len(entries) > 1
    return media_list, next_cursor, has_more


def _twitter_download_file(
    url: str,
    save_path: Path,
    proxy: Optional[str],
    is_video: bool,
) -> bool:
    """下载单个 Twitter 媒体文件。"""
    try:
        if is_video:
            dl_url = url
        else:
            dl_url = url + "?name=orig"

        with httpx.Client(proxy=proxy, timeout=(5, 60), follow_redirects=True) as client:
            resp = client.get(_quote_twitter_url(dl_url))
            if resp.status_code == 404 and not is_video:
                dl_url = url + "?name=4096x4096"
                resp = client.get(_quote_twitter_url(dl_url))
            resp.raise_for_status()
            with save_path.open("wb") as f:
                f.write(resp.content)
        return True
    except Exception as exc:
        append_log(f"[博主] 下载失败 {url} -> {save_path.name}，{exc}")
        if save_path.exists():
            try:
                save_path.unlink()
            except OSError:
                pass
        return False


def twitter_crawl_blogger(
    screen_name: str,
    cfg: Dict[str, object],
    download_root: Path,
    cache: set,
) -> tuple:
    """
    爬取单个博主，返回 (success_count, skip_count, fail_count, updated_cache)。
    """
    cookie = str(cfg.get("twitter_cookie", "")).strip()
    if not cookie:
        append_log(f"[博主] Twitter Cookie 未配置，跳过 @{screen_name}")
        return 0, 0, 0, cache

    proxy = str(cfg.get("proxy", "")).strip()
    proxy_url: Optional[str] = proxy if proxy else None
    headers = _build_twitter_headers(cookie)
    headers["referer"] = "https://twitter.com/" + screen_name
    has_retweet = bool(cfg.get("twitter_blogger_has_retweet", False))
    max_media = int(cfg.get("twitter_blogger_max_media", -1))

    user_info = twitter_get_user_info(screen_name, headers, proxy_url)
    if not user_info:
        return 0, 0, 1, cache

    # 将博主资料写入缓存，供前端展示
    try:
        info_cache = _load_blogger_info_cache(download_root)
        info_cache[screen_name] = {
            "name": user_info.get("name") or screen_name,
            "profile_image_url": user_info.get("profile_image_url", ""),
            "description": user_info.get("description", ""),
            "updated_at": time.time(),
        }
        _save_blogger_info_cache(download_root, info_cache)
    except Exception as exc:
        append_log(f"[博主] 写入 @{screen_name} 资料缓存失败：{exc}")

    append_log(
        f"[博主] @{screen_name} ({user_info['name']}) "
        f"媒体数:{user_info['media_count']} 开始爬取..."
    )

    # 创建保存目录
    blogger_dir = download_root / BLOGGER_DIR_NAME / screen_name
    blogger_dir.mkdir(parents=True, exist_ok=True)

    success_count = 0
    skip_count = 0
    fail_count = 0
    cursor: Optional[str] = None
    total_collected = 0

    while max_media == -1 or total_collected < max_media:
        media_list, next_cursor, has_more = twitter_fetch_tweets(
            user_info["rest_id"], screen_name, cursor, headers, proxy_url, has_retweet,
        )

        if not media_list and not has_more:
            break

        for media_url, prefix, is_video in media_list:
            if max_media != -1 and total_collected >= max_media:
                break
            total_collected += 1

            # 去重：检查 cache
            if media_url in cache:
                skip_count += 1
                continue

            # 去重：检查本地文件
            ext = ".mp4" if is_video else ".jpg"
            # 从 url 获取更精确的后缀
            if not is_video:
                url_path = urlparse(media_url).path
                url_ext = Path(url_path).suffix.lower()
                if url_ext in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                    ext = url_ext
            filename = f"{prefix}_{total_collected}{ext}"
            save_path = blogger_dir / filename

            if save_path.exists():
                skip_count += 1
                cache.add(media_url)
                continue

            ok = _twitter_download_file(media_url, save_path, proxy_url, is_video)
            if ok:
                success_count += 1
                cache.add(media_url)
                append_log(f"[博主] @{screen_name} 下载完成：{filename}")
                # 自动上传到网盘
                ol = cfg.get("openlist", {})
                if ol.get("enabled") and ol.get("auto_upload_after_download") and is_video:
                    try:
                        folder = f"blogger/{screen_name}"
                        ol_proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else {}
                        result = _openlist_upload_media(
                            save_path, None, folder, ol, ol_proxies,
                        )
                        if result.get("video", {}).get("ok") and ol.get("delete_local_after_upload"):
                            try:
                                save_path.unlink()
                                append_log(f"[网盘] 已删除本地文件：{filename}")
                            except OSError as exc:
                                append_log(f"[网盘] 删除本地文件失败 {filename}：{exc}")
                    except Exception as exc:
                        append_log(f"[网盘] 自动上传异常 {filename}：{exc}")
            else:
                fail_count += 1

        if not has_more or not next_cursor:
            break
        cursor = next_cursor

    append_log(
        f"[博主] @{screen_name} 爬取完成：成功 {success_count}，"
        f"跳过 {skip_count}，失败 {fail_count}"
    )
    return success_count, skip_count, fail_count, cache


def run_blogger_crawl_job() -> None:
    """博主爬取定时任务入口。"""
    with runtime_lock:
        if blogger_state["is_running"]:
            append_log("[博主] 任务已在运行中，跳过本次触发")
            return
        blogger_state["is_running"] = True

    started = datetime.now()
    blogger_state["last_run_time"] = started.strftime("%Y-%m-%d %H:%M:%S")
    append_log("[博主] 开始执行博主爬取任务")

    try:
        cfg = get_current_config()
        download_root = resolve_download_root(cfg["download_root"])
        download_root.mkdir(parents=True, exist_ok=True)

        blogger_list = cfg.get("twitter_blogger_list", [])
        if not blogger_list:
            append_log("[博主] 博主列表为空，跳过")
            blogger_state["last_result"] = "博主列表为空，已跳过"
            return

        cache = _load_blogger_cache(download_root)

        total_success = 0
        total_skip = 0
        total_fail = 0

        append_log(f"[博主] 本次计划爬取 {len(blogger_list)} 个博主")

        for screen_name in blogger_list:
            s, k, f, cache = twitter_crawl_blogger(
                screen_name, cfg, download_root, cache,
            )
            total_success += s
            total_skip += k
            total_fail += f

        # 保存缓存
        _save_blogger_cache(download_root, cache)

        result = (
            f"博主爬取完成：共 {len(blogger_list)} 个博主，"
            f"成功 {total_success}，跳过 {total_skip}，失败 {total_fail}"
        )
        append_log(result)
        blogger_state["last_result"] = result
    except Exception as exc:
        err_msg = f"[博主] 任务异常：{exc}"
        append_log(err_msg)
        blogger_state["last_result"] = err_msg
    finally:
        blogger_state["is_running"] = False


# ── OpenList 网盘上传 API ──

@app.get("/api/openlist/status")
def api_openlist_status():
    return JSONResponse({
        "ok": True,
        "state": openlist_state,
    })


@app.post("/api/openlist/test")
async def api_openlist_test(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    cfg = get_current_config()
    ol = cfg.get("openlist", {})
    base_url = body.get("base_url", ol.get("base_url", ""))
    token = body.get("token", ol.get("token", ""))
    proxy = str(cfg.get("proxy", "")).strip()
    proxies = build_proxies(proxy)

    if not base_url or not token:
        return JSONResponse({"ok": False, "message": "base_url 和 token 不能为空"})

    result = _openlist_test_connection(base_url, token, proxies)
    return JSONResponse(result)


@app.post("/api/openlist/upload")
async def api_openlist_upload(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    folder = str(body.get("folder", "")).strip()
    stem = str(body.get("stem", "")).strip()
    if not folder or not stem:
        return JSONResponse({"ok": False, "message": "folder 和 stem 不能为空"})
    cfg = get_current_config()
    download_root = resolve_download_root(cfg["download_root"])
    # 查找视频文件
    video_file = None
    thumb_file = None
    for ext in {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv"}:
        p = download_root / folder / f"{stem}{ext}"
        if p.exists():
            video_file = p
            break
    for ext in {".jpg", ".jpeg", ".png", ".webp"}:
        p = download_root / folder / f"{stem}{ext}"
        if p.exists():
            thumb_file = p
            break
    if not video_file:
        return JSONResponse({"ok": False, "message": f"未找到本地视频文件：{folder}/{stem}"})

    ol = cfg.get("openlist", {})
    if not ol.get("token") or not ol.get("base_url"):
        return JSONResponse({"ok": False, "message": "网盘未配置 Token 或地址"})

    proxy = str(cfg.get("proxy", "")).strip()
    proxies = build_proxies(proxy)
    result = _openlist_upload_media(video_file, thumb_file, folder, ol, proxies)
    append_log(f"[网盘] 手动上传 {video_file.name}：{'成功' if result['ok'] else '部分失败'}")
    return JSONResponse({"ok": result["ok"], "result": result})


@app.post("/api/openlist/upload-all")
def api_openlist_upload_all(request: Request):
    cfg = get_current_config()
    ol = cfg.get("openlist", {})
    if not ol.get("token") or not ol.get("base_url"):
        return JSONResponse({"ok": False, "message": "网盘未配置 Token 或地址"})

    if openlist_state["is_running"]:
        return JSONResponse({"ok": False, "message": "批量上传正在进行中"})

    openlist_state["is_running"] = True
    started = datetime.now()
    openlist_state["last_run_time"] = started.strftime("%Y-%m-%d %H:%M:%S")

    def _run():
        try:
            download_root = resolve_download_root(cfg["download_root"])
            proxy = str(cfg.get("proxy", "")).strip()
            proxies = build_proxies(proxy)
            result = _openlist_upload_all_local(ol, download_root, proxies)
            msg = result.get("message", "批量上传完成")
            append_log(f"[网盘] {msg}")
            openlist_state["last_result"] = msg
            openlist_state["total_uploaded"] += result.get("success", 0)
            openlist_state["total_failed"] += result.get("failed", 0)
        except Exception as exc:
            err = f"批量上传异常：{exc}"
            append_log(f"[网盘] {err}")
            openlist_state["last_result"] = err
        finally:
            openlist_state["is_running"] = False

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse({"ok": True, "message": "批量上传已启动"})


@app.post("/api/openlist/save")
async def api_openlist_save(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    with config_lock:
        cfg = load_config()
        ol = dict(cfg.get("openlist", {}))
        for key in ("enabled", "base_url", "token", "remote_root", "overwrite",
                     "upload_video", "upload_thumbnail", "auto_upload_after_download",
                     "delete_local_after_upload", "timeout", "max_retries", "path_template"):
            if key in body:
                ol[key] = body[key]
        cfg["openlist"] = ol
        try:
            save_config(cfg)
            append_log("[网盘] 上传配置已保存")
            return JSONResponse({"ok": True, "config": get_current_config().get("openlist", {})})
        except ValueError as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


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
            "blogger_state": dict(blogger_state),
            "openlist_state": dict(openlist_state),
            "logs": "\n".join(get_logs()),
        },
    )


@app.post("/save")
async def save(request: Request):
    try:
        form = await request.form()
        download_root = str(form.get("download_root", "")).strip()
        proxy = str(form.get("proxy", "")).strip()
        auto_download_enabled = parse_bool(form.get("auto_download_enabled", "1"))
        schedule_cron = str(form.get("schedule_cron", "")).strip()
        max_daily_downloads = int(form.get("max_daily_downloads", 0))

        with config_lock:
            cfg = load_config()
            cfg.update({
                "download_root": download_root,
                "proxy": proxy,
                "auto_download_enabled": auto_download_enabled,
                "schedule_cron": schedule_cron,
                "max_daily_downloads": max_daily_downloads,
                "ranking_range": str(form.get("ranking_range", cfg.get("ranking_range", "daily"))).strip(),
                "twitter_cookie": str(form.get("twitter_cookie", cfg.get("twitter_cookie", ""))).strip(),
                "twitter_blogger_enabled": parse_bool(form.get("twitter_blogger_enabled", cfg.get("twitter_blogger_enabled", True))),
                "twitter_blogger_cron": str(form.get("twitter_blogger_cron", cfg.get("twitter_blogger_cron", "0 4 * * *"))).strip(),
                "twitter_blogger_max_media": int(form.get("twitter_blogger_max_media", cfg.get("twitter_blogger_max_media", -1))),
                "twitter_blogger_has_retweet": parse_bool(form.get("twitter_blogger_has_retweet", cfg.get("twitter_blogger_has_retweet", False))),
            })
            # OpenList 配置以 JSON 字符串形式提交
            ol_raw = form.get("openlist", "")
            if ol_raw:
                try:
                    ol_parsed = json.loads(ol_raw)
                    # 类型纠正：布尔值在 form 中可能被转为字符串
                    for bk in ("enabled", "overwrite", "upload_video", "upload_thumbnail",
                               "auto_upload_after_download", "delete_local_after_upload"):
                        if bk in ol_parsed and isinstance(ol_parsed[bk], str):
                            ol_parsed[bk] = ol_parsed[bk].lower() in ("true", "1", "yes")
                    for ik in ("timeout", "max_retries"):
                        if ik in ol_parsed and isinstance(ol_parsed[ik], str):
                            ol_parsed[ik] = int(ol_parsed[ik])
                    cfg["openlist"] = ol_parsed
                except (json.JSONDecodeError, TypeError) as exc:
                    append_log(f"OpenList 配置解析失败：{exc}")
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

            save_config(cfg)
            updated = load_config()
        append_log("下载根目录已自动保存")
        return JSONResponse({"ok": True, "config": updated})
    except Exception as exc:
        append_log(f"自动保存下载根目录失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/api/waterfall/settings")
async def save_waterfall_settings(request: Request):
    try:
        body = await request.json()
        with config_lock:
            cfg = load_config()
            cfg["waterfall_per_page"] = int(body.get("per_page", cfg.get("waterfall_per_page", 10)))
            save_config(cfg)
            updated = load_config()
        append_log("瀑布流配置已保存")
        return JSONResponse({"ok": True, "config": {"per_page": int(updated.get("waterfall_per_page", 10))}})
    except Exception as exc:
        append_log(f"瀑布流配置保存失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@app.post("/run-now")
def run_now():
    messages = []
    if runtime_state["is_running"]:
        messages.append("下载任务正在运行中")
    else:
        threading.Thread(target=run_download_job, daemon=True).start()
        append_log("已触发手动执行下载任务")
        messages.append("下载任务已启动")

    if blogger_state["is_running"]:
        messages.append("博主爬取任务正在运行中")
    else:
        cfg = get_current_config()
        if cfg.get("twitter_blogger_list"):
            threading.Thread(target=run_blogger_crawl_job, daemon=True).start()
            append_log("已触发手动执行博主爬取任务")
            messages.append("博主爬取任务已启动")
        else:
            messages.append("博主列表为空，跳过博主爬取")

    return JSONResponse({"ok": True, "message": "；".join(messages)})


@app.get("/status")
def status():
    return JSONResponse(
        {
            "ok": True,
            "state": runtime_state,
            "blogger_state": blogger_state,
            "openlist_state": openlist_state,
            "logs": get_logs(),
            "config": get_current_config(),
        }
    )


@app.get("/api/waterfall")
def api_waterfall(page: int = 1, range: str = ""):
    cfg = get_current_config()
    per_page = int(cfg.get("waterfall_per_page", 10))
    safe_page = max(1, int(page))
    ranking_range = str(range or cfg.get("ranking_range", "daily")).strip()
    if ranking_range not in RANKING_RANGE_OPTIONS:
        ranking_range = "daily"
    proxies = build_proxies(str(cfg.get("proxy", "")).strip())
    session = requests.Session()
    try:
        all_items = _fetch_pektino_media(session, proxies, ranking_range)
        # Client-side pagination: slice the full result set
        total = len(all_items)
        start = (safe_page - 1) * per_page
        end = start + per_page
        page_items = all_items[start:end]
        items = [
            normalized
            for normalized in (_normalize_waterfall_item(item) for item in page_items)
            if normalized is not None
        ]
        return JSONResponse({
            "ok": True,
            "items": items,
            "config": {"per_page": per_page},
            "pagination": {
                "page": safe_page,
                "per_page": per_page,
                "has_next": end < total,
            },
        })
    except Exception as exc:
        append_log(f"瀑布流预览加载失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)


@app.post("/api/waterfall/download")
async def api_waterfall_download(request: Request):
    body = await request.json()
    raw_items = body.get("items", [])
    if not isinstance(raw_items, list):
        return JSONResponse({"ok": False, "error": "items 必须是数组"}, status_code=400)

    cfg = get_current_config()

    items = [
        normalized
        for normalized in (_normalize_waterfall_item(item) for item in raw_items)
        if normalized is not None
    ]
    if not items:
        return JSONResponse({"ok": False, "error": "没有可下载的视频"}, status_code=400)

    root = resolve_download_root(cfg["download_root"])
    target_dir = root
    target_dir.mkdir(parents=True, exist_ok=True)

    proxies = build_proxies(str(cfg.get("proxy", "")).strip())
    session = requests.Session()
    success_count, skip_count, fail_count = _download_items(
        session,
        items,
        target_dir,
        max_count=len(items),
        proxies=proxies,
        cfg=cfg,
    )
    append_log(
        f"瀑布流手动下载完成：成功 {success_count}，跳过 {skip_count}，失败 {fail_count}"
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

# ── 扫描缓存 ──
# 目的：避免每次 /api/poster 都重新 iterdir + ffprobe 整个下载根。
# 行为：按 (root, folder) 缓存 60s，写操作（delete / replace-cover）会清空。
POSTER_SCAN_TTL = 60.0  # 秒
_poster_scan_cache: Dict[str, Tuple[float, dict]] = {}
_poster_folders_cache: Dict[str, Tuple[float, List[dict]]] = {}
_duration_cache: Dict[str, Tuple[float, Optional[str]]] = {}
DURATION_CACHE_TTL = 3600.0  # 秒，ffprobe 结果几乎不变
DURATION_CACHE_PATH = APP_DIR / ".duration_cache.json"  # 持久化缓存文件

# ── 缩略图缓存 ──
THUMB_CACHE_DIR = APP_DIR / ".thumb_cache"
THUMB_SIZE = (320, 180)  # 16:9, 2x 显示密度

_duration_cache_lock = threading.Lock()


def _load_duration_cache() -> Dict[str, str]:
    """从 JSON 文件加载持久化时长缓存。"""
    try:
        if DURATION_CACHE_PATH.exists():
            with open(DURATION_CACHE_PATH, "r") as f:
                return json.load(f)
    except (json.JSONDecodeError, OSError):
        pass
    return {}


def _save_duration_cache(cache: Dict[str, str]) -> None:
    """将时长缓存写入 JSON 文件。"""
    try:
        with open(DURATION_CACHE_PATH, "w") as f:
            json.dump(cache, f)
    except OSError:
        pass


def _scan_key(root: Path, folder: Optional[str]) -> str:
    return f"{root}::{'' if folder is None else folder}"


def _scan_cache_get(key: str):
    entry = _poster_scan_cache.get(key)
    if not entry:
        return None
    expires_at, payload = entry
    if expires_at < time.monotonic():
        _poster_scan_cache.pop(key, None)
        return None
    return payload


def _scan_cache_set(key: str, payload: dict) -> None:
    _poster_scan_cache[key] = (time.monotonic() + POSTER_SCAN_TTL, payload)


def _folders_cache_get(key: str):
    entry = _poster_folders_cache.get(key)
    if not entry:
        return None
    expires_at, payload = entry
    if expires_at < time.monotonic():
        _poster_folders_cache.pop(key, None)
        return None
    return payload


def _folders_cache_set(key: str, payload: List[dict]) -> None:
    _poster_folders_cache[key] = (time.monotonic() + POSTER_SCAN_TTL, payload)


def _invalidate_poster_cache() -> None:
    _poster_scan_cache.clear()
    _poster_folders_cache.clear()
    _duration_cache.clear()
    # 同时清除持久化时长缓存（修改后重新探测）
    try:
        if DURATION_CACHE_PATH.exists():
            DURATION_CACHE_PATH.unlink()
    except OSError:
        pass


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
    key = str(download_root)
    cached = _folders_cache_get(key)
    if cached is not None:
        return cached
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
    _folders_cache_set(key, folders)
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


def _get_or_create_thumb(source_path: Path) -> Path:
    """生成或返回缓存的 WebP 缩略图。"""
    THUMB_CACHE_DIR.mkdir(exist_ok=True)
    key = hashlib.md5(f"{source_path.stat().st_mtime_ns}::{source_path}".encode()).hexdigest()[:16]
    cache_file = THUMB_CACHE_DIR / f"{key}.webp"
    if cache_file.exists():
        return cache_file
    try:
        img = Image.open(source_path)
        img.thumbnail(THUMB_SIZE, Image.LANCZOS)
        # 转为 RGB（处理 RGBA/P 模式）
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        img.save(cache_file, "WEBP", quality=80)
    except Exception:
        # 生成失败返回原图
        return source_path
    return cache_file


def _probe_video_duration(video_path: Path) -> Optional[str]:
    key = f"{video_path.stat().st_mtime_ns}::{video_path}"
    cached = _duration_cache.get(key)
    if cached is not None:
        expires_at, value = cached
        if expires_at >= time.monotonic():
            return value
        _duration_cache.pop(key, None)

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
        label = _format_duration(float(output))
    except ValueError:
        return None

    _duration_cache[key] = (time.monotonic() + DURATION_CACHE_TTL, label)
    return label


def _build_poster_item(folder: str, item: dict, duration: Optional[str] = None) -> dict:
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
        "duration": duration,
    }


def _duration_key(folder: str, stem: str) -> str:
    return f"{folder}::{stem}"


def _collect_poster_items(download_root: Path, folder: Optional[str] = None) -> List[dict]:
    cache_key = _scan_key(download_root, folder)
    cached = _scan_cache_get(cache_key)
    if cached is not None:
        return list(cached.get("items", []))

    if folder is None:
        folder_names = [f["folder"] for f in _media_folders(download_root)]
    else:
        folder_names = [folder]
    items: List[dict] = []
    persistent_cache = _load_duration_cache()
    pending: List[tuple] = []  # (folder, stem, directory, video_path)
    for name in folder_names:
        directory = resolve_media_folder(download_root, name)
        if directory is None:
            continue
        for item in _list_folder_items(directory):
            dk = _duration_key(name, item["stem"])
            dur = persistent_cache.get(dk)  # 先从持久化缓存读取
            built = _build_poster_item(name, item, dur)
            items.append(built)
            if not dur:
                pending.append((name, item["stem"], directory, directory / item["video"]))

    items.sort(key=lambda x: (x["folder"], x["stem"]))
    _scan_cache_set(cache_key, {"items": items})

    # 后台线程池并行探测缺失的时长
    if pending:
        threading.Thread(
            target=_batch_probe_durations,
            args=(pending,),
            daemon=True,
        ).start()

    return items


def _batch_probe_durations(pending: List[tuple]) -> None:
    """后台线程池并行探测时长，结果写入持久化缓存。"""
    def probe_one(folder: str, stem: str, vp: Path) -> Optional[tuple]:
        dur = _probe_video_duration(vp)
        if dur:
            return (_duration_key(folder, stem), dur)
        return None

    results = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(probe_one, f, s, v) for f, s, _, v in pending]
        for f in futures:
            r = f.result()
            if r:
                results[r[0]] = r[1]

    if results:
        with _duration_cache_lock:
            existing = _load_duration_cache()
            existing.update(results)
            _save_duration_cache(existing)


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
    if not resolved.is_relative_to(root.resolve()):
        return JSONResponse({"error": "禁止访问"}, status_code=403)
    # 生成 WebP 缩略图
    thumb_path = _get_or_create_thumb(resolved)
    mimetype = "image/webp" if thumb_path.suffix.lower() == ".webp" else None
    return FileResponse(
        str(thumb_path),
        media_type=mimetype,
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/api/poster-durations")
def api_poster_durations(folder: str = ""):
    """返回已探测完成的视频时长，供前端轮询补充。"""
    cfg = get_current_config()
    root = resolve_download_root(cfg["download_root"])
    if folder and resolve_media_folder(root, folder) is None:
        return JSONResponse({"ok": False, "error": "无效文件夹"}, status_code=400)

    persistent_cache = _load_duration_cache()
    matching = {}
    prefix = f"{folder}::" if folder else ""
    for k, v in persistent_cache.items():
        if not prefix or k.startswith(prefix):
            matching[k] = v
    return JSONResponse({"ok": True, "durations": matching})


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
    if not resolved.is_relative_to(root.resolve()):
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
    _invalidate_poster_cache()
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
    _invalidate_poster_cache()
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
            "config": {"per_page": int(cfg.get("waterfall_per_page", 10))},
            "page_size_options": sorted(ALLOWED_WATERFALL_PAGE_SIZES),
        },
    )


# ──────────────────────────────────────────────────
# 博主管理 API
# ──────────────────────────────────────────────────

@app.get("/api/check-dir")
def api_check_dir(path: str = ""):
    """校验下载目录是否存在"""
    import os
    p = Path(path.strip()) if path else None
    if not p:
        return JSONResponse({"ok": False, "error": "路径不能为空"})
    if p.exists() and p.is_dir():
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": False, "error": "目录不存在"})


@app.get("/api/blogger/list")
def api_blogger_list():
    cfg = get_current_config()
    download_root = resolve_download_root(cfg["download_root"])
    blogger_dir = download_root / BLOGGER_DIR_NAME
    info_cache = _load_blogger_info_cache(download_root)
    bloggers = []
    for name in cfg.get("twitter_blogger_list", []):
        user_dir = blogger_dir / name
        file_count = 0
        if user_dir.exists():
            file_count = sum(1 for p in user_dir.iterdir() if p.is_file())
        info = info_cache.get(name, {}) if isinstance(info_cache, dict) else {}
        bloggers.append({
            "screen_name": name,
            "name": info.get("name") or name,
            "profile_image_url": info.get("profile_image_url", ""),
            "description": info.get("description", ""),
            "file_count": file_count,
        })
    return JSONResponse({
        "ok": True,
        "bloggers": bloggers,
        "state": dict(blogger_state),
        "settings": {
            "twitter_blogger_enabled": cfg.get("twitter_blogger_enabled", True),
            "twitter_blogger_cron": cfg.get("twitter_blogger_cron", "0 4 * * *"),
            "twitter_blogger_max_media": cfg.get("twitter_blogger_max_media", -1),
            "twitter_blogger_has_retweet": cfg.get("twitter_blogger_has_retweet", False),
            "twitter_cookie_set": bool(str(cfg.get("twitter_cookie", "")).strip()),
        },
    })


@app.post("/api/blogger/add")
async def api_blogger_add(request: Request):
    body = await request.json()
    screen_name = str(body.get("screen_name", "")).strip().lstrip("@").lower()
    if not screen_name or not all(c.isalnum() or c == "_" for c in screen_name):
        return JSONResponse({"ok": False, "error": "无效的用户名"}, status_code=400)
    with config_lock:
        cfg = load_config()
        bl = list(cfg.get("twitter_blogger_list", []))
        if screen_name in bl:
            return JSONResponse({"ok": False, "error": f"@{screen_name} 已在列表中"}, status_code=400)
        bl.append(screen_name)
        cfg["twitter_blogger_list"] = bl
        save_config(cfg)
    append_log(f"[博主] 已添加博主 @{screen_name}")

    # 后台异步获取博主资料（头像 / 简介），不影响主流程
    def _fetch_info_async():
        try:
            snapshot = load_config()
            download_root_local = resolve_download_root(snapshot["download_root"])
            cookie = str(snapshot.get("twitter_cookie", "")).strip()
            if not cookie:
                return
            proxy = str(snapshot.get("proxy", "")).strip() or None
            headers = _build_twitter_headers(cookie)
            headers["referer"] = "https://twitter.com/" + screen_name
            user_info = twitter_get_user_info(screen_name, headers, proxy)
            if not user_info:
                return
            info_cache = _load_blogger_info_cache(download_root_local)
            info_cache[screen_name] = {
                "name": user_info.get("name") or screen_name,
                "profile_image_url": user_info.get("profile_image_url", ""),
                "description": user_info.get("description", ""),
                "updated_at": time.time(),
            }
            _save_blogger_info_cache(download_root_local, info_cache)
            append_log(f"[博主] 已更新 @{screen_name} 资料缓存")
        except Exception as exc:
            append_log(f"[博主] 异步获取 @{screen_name} 资料失败：{exc}")

    threading.Thread(target=_fetch_info_async, daemon=True).start()

    return JSONResponse({"ok": True, "screen_name": screen_name})


@app.post("/api/blogger/remove")
async def api_blogger_remove(request: Request):
    body = await request.json()
    screen_name = str(body.get("screen_name", "")).strip().lstrip("@").lower()
    if not screen_name:
        return JSONResponse({"ok": False, "error": "无效的用户名"}, status_code=400)
    with config_lock:
        cfg = load_config()
        bl = list(cfg.get("twitter_blogger_list", []))
        if screen_name not in bl:
            return JSONResponse({"ok": False, "error": f"@{screen_name} 不在列表中"}, status_code=400)
        bl.remove(screen_name)
        cfg["twitter_blogger_list"] = bl
        save_config(cfg)
    append_log(f"[博主] 已移除博主 @{screen_name}")
    return JSONResponse({"ok": True, "screen_name": screen_name})


@app.post("/api/blogger/crawl-now")
def api_blogger_crawl_now():
    if blogger_state["is_running"]:
        return JSONResponse({"ok": False, "message": "博主爬取任务正在运行中"})
    threading.Thread(target=run_blogger_crawl_job, daemon=True).start()
    append_log("[博主] 已触发手动爬取任务")
    return JSONResponse({"ok": True, "message": "博主爬取任务已启动"})


@app.post("/api/blogger/save-settings")
async def api_blogger_save_settings(request: Request):
    try:
        body = await request.json()
        with config_lock:
            cfg = load_config()
            if "twitter_cookie" in body:
                cfg["twitter_cookie"] = str(body["twitter_cookie"]).strip()
            if "twitter_blogger_enabled" in body:
                cfg["twitter_blogger_enabled"] = parse_bool(body["twitter_blogger_enabled"])
            if "twitter_blogger_cron" in body:
                cfg["twitter_blogger_cron"] = str(body["twitter_blogger_cron"]).strip()
            if "twitter_blogger_max_media" in body:
                val = int(body["twitter_blogger_max_media"])
                if val < -1:
                    val = -1
                cfg["twitter_blogger_max_media"] = val
            if "twitter_blogger_has_retweet" in body:
                cfg["twitter_blogger_has_retweet"] = parse_bool(body["twitter_blogger_has_retweet"])
            save_config(cfg)
            updated = load_config()
        update_schedule(updated)
        append_log("[博主] 博主设置已保存")
        return JSONResponse({"ok": True})
    except Exception as exc:
        append_log(f"[博主] 保存设置失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=2617)
