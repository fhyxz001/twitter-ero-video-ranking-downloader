"""封面刮削：遍历指定目录中的视频文件，用 ffmpeg 抽取首帧生成同名 jpg 封面。

作业模型：
- 同一时间仅允许一个刮削作业；路由层 POST 启动后台线程，GET 轮询状态。
- 已有同名图片（IMAGE_EXTS 内任意后缀）的视频直接跳过，不覆盖手动设置的封面。
- .strm 等引用型条目不参与刮削（无本地媒体帧可抽），只处理真实视频文件。
- 作业结束后失效本地视频扫描缓存，新封面立即可见。
"""
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional, Tuple

from app.config import IMAGE_EXTS, VIDEO_EXTS
from app.logging_utils import append_log
from app.poster import invalidate_scan_cache

FFMPEG_CMD: List[str] = ["ffmpeg"]
FFMPEG_TIMEOUT = 60  # 单文件抽帧超时（秒）
MAX_WORKERS = 2  # 并发抽帧线程数，照顾 NAS 机械盘
MAX_ERRORS = 20  # 状态中保留的最近错误条数

_scrape_lock = threading.Lock()
scrape_state: dict = {
    "running": False,
    "phase": "idle",  # idle / scanning / extracting / done / error
    "total": 0,
    "done": 0,
    "generated": 0,
    "skipped": 0,
    "failed": 0,
    "current": None,  # 正在处理的视频路径
    "errors": [],  # 最近错误，新错误在前
    "message": "",  # error 阶段的说明，如未找到 ffmpeg
}


def scrape_status() -> dict:
    with _scrape_lock:
        return {**scrape_state, "errors": list(scrape_state["errors"])}


def start_scrape_job(targets: List[Path]) -> bool:
    """启动刮削作业；已有作业在跑时返回 False，调用方据此提示用户。"""
    with _scrape_lock:
        if scrape_state["running"]:
            return False
        scrape_state.update(
            running=True,
            phase="scanning",
            total=0,
            done=0,
            generated=0,
            skipped=0,
            failed=0,
            current=None,
            errors=[],
            message="",
        )
    threading.Thread(target=_run_job, args=(targets,), daemon=True).start()
    return True


def _has_cover(video: Path) -> bool:
    return any(video.with_suffix(ext).is_file() for ext in IMAGE_EXTS)


def _collect_tasks(targets: List[Path]) -> Tuple[List[Tuple[Path, Path]], int]:
    """收集待刮削任务 (视频路径, 封面路径)，返回 (任务列表, 已有封面跳过数)。"""
    tasks: List[Tuple[Path, Path]] = []
    skipped = 0
    for directory in targets:
        if not directory.is_dir():
            continue
        for p in sorted(directory.iterdir()):
            if not p.is_file() or p.suffix.lower() not in VIDEO_EXTS:
                continue
            if _has_cover(p):
                skipped += 1
                continue
            tasks.append((p, p.with_suffix(".jpg")))
    return tasks, skipped


def _update_locked_fail(err: str) -> None:
    """记录一次失败（需持有 _scrape_lock），保留最近 MAX_ERRORS 条。"""
    scrape_state["failed"] += 1
    scrape_state["errors"] = [err] + scrape_state["errors"]
    scrape_state["errors"] = scrape_state["errors"][:MAX_ERRORS]


def _finish(phase: str, message: str = "") -> None:
    with _scrape_lock:
        scrape_state.update(running=False, phase=phase, current=None, message=message)


def extract_first_frame(video_path: Path, cover_path: Path) -> Tuple[bool, str]:
    """用 ffmpeg 抽取视频首帧保存为 jpg，返回 (是否成功, 失败原因)。"""
    cmd = [
        *FFMPEG_CMD,
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(video_path),
        "-map",
        "0:v:0",
        "-frames:v",
        "1",
        "-q:v",
        "2",
        "-y",
        str(cover_path),
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=FFMPEG_TIMEOUT,
        )
    except FileNotFoundError:
        return False, "未找到 ffmpeg"
    except subprocess.TimeoutExpired:
        return False, f"抽帧超时（>{FFMPEG_TIMEOUT}s）"
    if result.returncode != 0 or not cover_path.is_file():
        stderr_lines = (result.stderr or "").strip().splitlines()
        reason = stderr_lines[-1] if stderr_lines else f"ffmpeg 退出码 {result.returncode}"
        return False, reason
    return True, ""


def _run_job(targets: List[Path]) -> None:
    if shutil.which(FFMPEG_CMD[0]) is None:
        append_log("封面刮削失败：未找到 ffmpeg")
        _finish("error", "未找到 ffmpeg，请安装 ffmpeg 并加入 PATH 后重试")
        return

    tasks, skipped = _collect_tasks(targets)
    with _scrape_lock:
        scrape_state.update(phase="extracting", total=len(tasks), skipped=skipped)
    append_log(f"封面刮削开始：待生成 {len(tasks)} 个，已有封面跳过 {skipped} 个")

    def process_one(video: Path, cover: Path) -> None:
        with _scrape_lock:
            scrape_state["current"] = str(video)
        ok, err = extract_first_frame(video, cover)
        with _scrape_lock:
            scrape_state["done"] += 1
            if ok:
                scrape_state["generated"] += 1
            else:
                _update_locked_fail(f"{video.name}: {err}")
                cover.unlink(missing_ok=True)

    if tasks:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(process_one, v, c) for v, c in tasks]
            for f in futures:
                try:
                    f.result()
                except Exception as exc:
                    with _scrape_lock:
                        _update_locked_fail(f"内部错误: {exc}")

    invalidate_scan_cache()
    s = scrape_status()
    append_log(f"封面刮削完成：新增 {s['generated']}，跳过 {s['skipped']}，失败 {s['failed']}")
    _finish("done")
