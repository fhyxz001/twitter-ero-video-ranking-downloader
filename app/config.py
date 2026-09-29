"""应用配置：全局常量、默认配置、加载/校验/保存。

配置采用白名单制：只保留 DEFAULT_CONFIG 中登记的键，历史遗留键在
读取时即被丢弃，保证不再出现的功能（及其残留敏感信息）不会回写磁盘，
也不会通过接口对外泄露。
"""
import json
import os
import shutil
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.logging_utils import append_log
from app.paths import APP_DIR, CONFIG_PATH, STATIC_PATH, VUE_DIST  # noqa: F401  (VUE_DIST 供 server 使用)

# ── 数据源与全局常量 ──
MEDIA_API_URL = "https://pektino.com/api/media"
# 榜单档位（配置/UI 值）→ 接口 range 参数。站点的日榜取值是 timely，
# range 缺省会被接口当作无效档位返回空列表，因此必须始终携带。
RANKING_RANGE_PARAMS = {
    "daily": "timely",
    "weekly": "weekly",
    "monthly": "monthly",
    "all": "all",
}
RANKING_RANGE_OPTIONS = list(RANKING_RANGE_PARAMS.keys())
REQUEST_TIMEOUT = 30
ALLOWED_WATERFALL_PAGE_SIZES = {10, 20, 30, 50, 100}

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv"}
# 海报墙额外收录的“引用型”条目：.strm 是文本文件，内容为媒体 URL 或本地路径
POSTER_VIDEO_EXTS = VIDEO_EXTS | {".strm"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

DEFAULT_CONFIG: Dict[str, object] = {
    "download_root": "/data/downloads",
    # 海报墙媒体根目录：留空时回退到 download_root
    "poster_root": "",
    "proxy": "",
    "auto_download_enabled": True,
    "schedule_cron": "0 3 * * *",
    "max_daily_downloads": 10,
    "ranking_range": "daily",
    "waterfall_per_page": 10,
}
CONFIG_KEYS = tuple(DEFAULT_CONFIG.keys())

# 可重入锁：load/save 内部与路由层嵌套使用（with config_lock: load_config()）
config_lock = threading.RLock()
_config_cache: Optional[Tuple[int, Dict[str, object]]] = None


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def validate_config(raw: Dict[str, object]) -> Dict[str, object]:
    """白名单合并 + 逐项校验，返回只含合法键的配置。非法值抛 ValueError。"""
    source = raw or {}
    cfg: Dict[str, object] = {key: source.get(key, default) for key, default in DEFAULT_CONFIG.items()}

    download_root = str(cfg.get("download_root", "")).strip()
    if not download_root:
        raise ValueError("下载根目录不能为空")
    cfg["download_root"] = download_root

    cfg["poster_root"] = str(cfg.get("poster_root", "") or "").strip()

    cfg["proxy"] = str(cfg.get("proxy", "")).strip()
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
        waterfall_per_page = 10
    cfg["waterfall_per_page"] = waterfall_per_page

    return cfg


def public_config(cfg: Dict[str, object]) -> Dict[str, object]:
    """对外输出的配置视图：只含白名单键，防止遗留键里的敏感信息泄露。"""
    return {key: cfg.get(key, DEFAULT_CONFIG[key]) for key in CONFIG_KEYS}


def _config_file() -> Path:
    """实际生效的配置文件路径。

    Docker 文件级挂载（./config.json:/app/config.json）在宿主机文件不存在时，
    Docker 会创建一个同名目录挂进容器；该目录是挂载点，容器内既删不掉也无法
    替换成文件。此时把配置落到该目录内的 config.json，实现零干预自愈。
    """
    if CONFIG_PATH.is_dir():
        return CONFIG_PATH / "config.json"
    return CONFIG_PATH


def load_config() -> Dict[str, object]:
    """读取配置。带 mtime 缓存：文件未变化时直接返回缓存副本。"""
    global _config_cache

    path = _config_file()
    if path.is_dir():
        # 极端情况：配置路径本身又是个目录，清理后重建默认配置
        append_log(f"检测到 {path} 是目录而非文件，已自动移除并重建默认配置")
        shutil.rmtree(path, ignore_errors=True)
    if not path.exists():
        save_config(DEFAULT_CONFIG)
        return dict(DEFAULT_CONFIG)

    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        mtime = -1

    with config_lock:
        if _config_cache is not None and mtime >= 0 and _config_cache[0] == mtime:
            return dict(_config_cache[1])
        try:
            with path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            validated = validate_config(raw)
        except Exception as exc:
            append_log(f"读取配置失败，已回退默认配置：{exc}")
            save_config(DEFAULT_CONFIG)
            return dict(DEFAULT_CONFIG)
        _config_cache = (mtime, validated)
        return dict(validated)


def save_config(cfg: Dict[str, object]) -> None:
    """校验并写入配置（原子写），同时刷新进程内缓存。"""
    global _config_cache
    validated = validate_config(cfg)
    path = _config_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(validated, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)
    with config_lock:
        try:
            mtime = path.stat().st_mtime_ns
        except OSError:
            mtime = -1
        _config_cache = (mtime, validated)


def get_current_config() -> Dict[str, object]:
    return load_config()


def resolve_download_root(download_root: object) -> Path:
    """把配置中的下载根目录解析为绝对路径；相对路径基于程序目录。"""
    root = Path(str(download_root)).expanduser()
    if not root.is_absolute():
        root = APP_DIR / root
    return root.resolve()


def resolve_poster_root(cfg: Dict[str, object]) -> Path:
    """海报墙媒体根目录：优先用 poster_root，未配置时回退 download_root。"""
    poster_root = str(cfg.get("poster_root", "") or "").strip()
    if poster_root:
        return resolve_download_root(poster_root)
    return resolve_download_root(cfg["download_root"])
