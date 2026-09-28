"""下载任务、失败重试与定时调度。"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import (
    REQUEST_TIMEOUT,
    VIDEO_EXTS,
    get_current_config,
    resolve_download_root,
)
from app.logging_utils import append_log
from app.media_api import fetch_pektino_media

MAX_DOWNLOAD_WORKERS = 3
DOWNLOAD_ATTEMPTS = 2
RETRY_INTERVAL_SECONDS = 1.5

scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

runtime_lock = threading.Lock()
runtime_state = {
    "is_running": False,
    "last_run_time": None,
    "last_result": "尚未执行",
}


def build_proxies(proxy: str) -> Optional[Dict[str, str]]:
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def get_file_ext_from_url(url: str, fallback: str) -> str:
    parsed = urlparse(url)
    ext = Path(parsed.path).suffix.lower()
    if ext:
        return ext
    return fallback


def download_binary(
    session: requests.Session,
    url: str,
    target_path: Path,
    proxies: Optional[Dict[str, str]],
    attempts: int = DOWNLOAD_ATTEMPTS,
) -> bool:
    """流式下载文件到本地，失败自动重试。"""
    last_error: Exception = ValueError("未尝试下载")
    for attempt in range(1, max(1, attempts) + 1):
        try:
            with session.get(url, stream=True, timeout=REQUEST_TIMEOUT, proxies=proxies) as resp:
                resp.raise_for_status()
                tmp_path = target_path.with_suffix(target_path.suffix + ".part")
                with tmp_path.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                tmp_path.replace(target_path)
            return True
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(RETRY_INTERVAL_SECONDS)
    append_log(f"下载失败 {url} -> {target_path.name}，错误：{last_error}")
    _cleanup_partial(target_path)
    return False


def _cleanup_partial(target_path: Path) -> None:
    for p in (target_path, target_path.with_suffix(target_path.suffix + ".part")):
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass


def _download_one(
    session: requests.Session,
    video_id: str,
    video_url: str,
    thumbnail_url: str,
    target_dir: Path,
    proxies: Optional[Dict[str, str]],
) -> bool:
    video_ext = get_file_ext_from_url(video_url, ".mp4")
    thumb_ext = get_file_ext_from_url(thumbnail_url, ".jpg") if thumbnail_url else ".jpg"
    video_path = target_dir / f"{video_id}{video_ext}"
    thumb_path = target_dir / f"{video_id}{thumb_ext}"

    ok_video = download_binary(session, video_url, video_path, proxies)
    ok_thumb = True
    if ok_video and thumbnail_url:
        ok_thumb = download_binary(session, thumbnail_url, thumb_path, proxies, attempts=1)

    if ok_video and ok_thumb:
        append_log(f"下载完成：{target_dir.name}/{video_path.name}")
        return True
    return False


def download_items(
    session: requests.Session,
    items: list,
    target_dir: Path,
    max_count: int,
    proxies: Optional[Dict[str, str]],
) -> Tuple[int, int, int]:
    """并行下载列表中的视频（先去重筛选，再并发下载），返回 (success, skip, fail)。"""
    success_count = 0
    skip_count = 0
    fail_count = 0

    candidates: List[Tuple[str, str, str]] = []
    for item in items:
        if len(candidates) >= max_count:
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

        candidates.append((video_id, video_url, thumbnail_url))

    if not candidates:
        return success_count, skip_count, fail_count

    workers = min(MAX_DOWNLOAD_WORKERS, len(candidates))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _download_one, session, video_id, video_url, thumbnail_url, target_dir, proxies
            ): video_id
            for video_id, video_url, thumbnail_url in candidates
        }
        for future in as_completed(futures):
            try:
                if future.result():
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as exc:
                fail_count += 1
                append_log(f"下载任务异常（id={futures[future]}）：{exc}")

    return success_count, skip_count, fail_count


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
            items, _has_next = fetch_pektino_media(
                proxies, ranking_range, per_page=max(30, max_downloads)
            )
            s, k, f = download_items(session, items, download_root, max_downloads, proxies)
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
