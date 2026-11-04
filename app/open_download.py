"""对外开放下载接口：key 校验、异步下载任务与状态查询。

外部调用方提交「下载链接 + 目标完整路径」，任务在后台线程执行，接口立即
返回 task_id，避免大文件下载把调用方拖到超时。
"""
import hmac
import threading
import time
import uuid
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse

import requests

from app.config import get_current_config, resolve_download_root
from app.downloader import build_proxies, download_binary
from app.logging_utils import append_log

TASK_TTL_SECONDS = 3600  # 已完成任务保留 1 小时，便于调用方回查结果
MAX_TASKS = 200

_tasks: Dict[str, dict] = {}
_tasks_lock = threading.Lock()


def verify_key(key: str) -> bool:
    """校验外部调用方传入的 key；配置为空时该接口视为未开启。"""
    expected = str(get_current_config().get("open_api_key", "") or "").strip()
    if not expected:
        return False
    return hmac.compare_digest(str(key or "").strip(), expected)


def normalize_url(url: str) -> Optional[str]:
    """只接受 http/https 直链，非法时返回 None。"""
    value = str(url or "").strip()
    if not value:
        return None
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return value


def resolve_target_path(raw_path: str) -> Path:
    """把传入的下载地址解析为绝对路径；相对路径基于下载根目录展开。"""
    value = str(raw_path or "").strip()
    if not value:
        raise ValueError("下载路径不能为空")
    target = Path(value).expanduser()
    if not target.is_absolute():
        target = resolve_download_root(get_current_config()["download_root"]) / target
    return target


def create_task(url: str, target: Path) -> dict:
    """登记任务并启动后台下载线程，返回任务当前状态。"""
    task_id = uuid.uuid4().hex
    task = {
        "task_id": task_id,
        "url": url,
        "path": str(target),
        "status": "pending",
        "error": None,
        "size": None,
        "created_at": _now_text(),
        "finished_at": None,
        "_created_ts": time.time(),
    }
    with _tasks_lock:
        _tasks[task_id] = task
        _prune_tasks()
        snapshot = _public(task)

    threading.Thread(target=_run_task, args=(task_id, url, target), daemon=True).start()
    return snapshot


def get_task(task_id: str) -> Optional[dict]:
    with _tasks_lock:
        task = _tasks.get(str(task_id or "").strip())
        return _public(task) if task else None


def _run_task(task_id: str, url: str, target: Path) -> None:
    _update(task_id, status="running")
    append_log(f"外部下载开始：{url} -> {target}")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        proxies = build_proxies(str(get_current_config().get("proxy", "")).strip())
        session = requests.Session()
        ok, error = download_binary(session, url, target, proxies)
        if ok:
            size = target.stat().st_size
            _update(
                task_id,
                status="success",
                size=size,
                finished_at=_now_text(),
                _finished_ts=time.time(),
            )
            append_log(f"外部下载完成：{target}（{size} 字节）")
        else:
            _update(
                task_id,
                status="failed",
                error=error or "下载失败",
                finished_at=_now_text(),
                _finished_ts=time.time(),
            )
    except Exception as exc:
        _update(
            task_id,
            status="failed",
            error=str(exc)[:300],
            finished_at=_now_text(),
            _finished_ts=time.time(),
        )
        append_log(f"外部下载异常：{exc}")


def _update(task_id: str, **fields) -> None:
    with _tasks_lock:
        task = _tasks.get(task_id)
        if task is not None:
            task.update(fields)


def _prune_tasks() -> None:
    """清理过期任务并限制总数，调用方需已持有 _tasks_lock。"""
    now = time.time()
    expired = [
        task_id
        for task_id, task in _tasks.items()
        if task.get("_finished_ts") and now - task["_finished_ts"] > TASK_TTL_SECONDS
    ]
    for task_id in expired:
        _tasks.pop(task_id, None)
    while len(_tasks) > MAX_TASKS:
        oldest = min(_tasks, key=lambda tid: _tasks[tid].get("_created_ts", 0))
        _tasks.pop(oldest, None)


def _public(task: dict) -> dict:
    """去掉内部时间戳字段后的对外视图。"""
    return {key: value for key, value in task.items() if not key.startswith("_")}


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")
