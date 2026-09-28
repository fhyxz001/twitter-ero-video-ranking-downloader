"""应用日志：内存环形缓冲（供前端展示）+ 落盘滚动文件。"""
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from threading import Lock
from typing import List

from app.paths import APP_DIR

log_lock = Lock()
log_lines: List[str] = []

LOG_DIR = APP_DIR / "logs"
LOG_FILE = LOG_DIR / "app.log"

_logger = logging.getLogger("ero_downloader")
_logger.setLevel(logging.INFO)
_logger.propagate = False


def _setup_file_handler() -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            LOG_FILE, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        _logger.addHandler(handler)
    except OSError:
        # 日志目录不可写时降级为仅内存日志
        pass


_setup_file_handler()


def append_log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_lock:
        log_lines.append(f"[{timestamp}] {message}")
        if len(log_lines) > 300:
            del log_lines[:-300]
    _logger.info(message)


def get_logs() -> List[str]:
    with log_lock:
        return list(log_lines)
