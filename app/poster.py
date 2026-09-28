"""海报墙：本地媒体扫描、时长探测、缩略图生成与多级缓存。

缓存分三层：
- 文件夹/条目扫描缓存：内存 TTL 60s，任何写操作后仅清空本层（时长保留）。
- 时长缓存：持久化 JSON（.duration_cache.json）+ 进程内镜像，键为
  "folder::stem"，原子写入；时长几乎不变，只在对应视频被删除时移除对应键。
- 缩略图缓存：WebP 文件，按源图 mtime 哈希命名。
"""
import hashlib
import json
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

from PIL import Image

from app.config import IMAGE_EXTS, VIDEO_EXTS
from app.paths import APP_DIR

POSTER_SCAN_TTL = 60.0  # 秒
POSTER_MAX_PAGE_SIZE = 100

_poster_scan_cache: Dict[str, Tuple[float, dict]] = {}
_poster_folders_cache: Dict[str, Tuple[float, List[dict]]] = {}

THUMB_CACHE_DIR = APP_DIR / ".thumb_cache"
THUMB_SIZE = (320, 180)  # 16:9, 2x 显示密度

DURATION_CACHE_PATH = APP_DIR / ".duration_cache.json"


# ── 时长缓存（持久化 + 进程内镜像，原子写） ──

class DurationStore:
    """以 JSON 文件为唯一持久层，进程内保镜像；并发读写均走锁，写入用 os.replace 原子替换。"""

    def __init__(self, path: Path):
        self._path = path
        self._lock = threading.Lock()
        self._data: Dict[str, str] = {}
        self._mtime: Optional[int] = None
        self._loaded = False

    def _read_file(self) -> Dict[str, str]:
        try:
            with self._path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError):
            return {}

    def _reload_if_changed_locked(self) -> None:
        try:
            mtime = self._path.stat().st_mtime_ns
        except OSError:
            mtime = None
        if not self._loaded or mtime != self._mtime:
            self._data = self._read_file()
            self._mtime = mtime
            self._loaded = True

    def _write_file_locked(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._path.with_suffix(".json.tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(self._data, f)
            os.replace(tmp_path, self._path)
            self._mtime = self._path.stat().st_mtime_ns
        except OSError:
            pass

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            self._reload_if_changed_locked()
            return self._data.get(key)

    def snapshot(self) -> Dict[str, str]:
        """返回全量时长映射的副本（供 /api/poster-durations 过滤输出）。"""
        with self._lock:
            self._reload_if_changed_locked()
            return dict(self._data)

    def update(self, mapping: Dict[str, str]) -> None:
        if not mapping:
            return
        with self._lock:
            self._reload_if_changed_locked()
            self._data.update(mapping)
            self._write_file_locked()

    def remove_stems(self, folder: str, stems: List[str]) -> int:
        """删除指定文件夹下若干 stem 的时长记录，返回删除数量。"""
        if not stems:
            return 0
        prefixes = {f"{folder}::{stem}" for stem in stems}
        with self._lock:
            self._reload_if_changed_locked()
            before = len(self._data)
            self._data = {k: v for k, v in self._data.items() if k not in prefixes}
            removed = before - len(self._data)
            if removed:
                self._write_file_locked()
            return removed


duration_store = DurationStore(DURATION_CACHE_PATH)


# ── 扫描缓存 ──

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


def invalidate_scan_cache() -> None:
    """写操作后只清空扫描缓存；时长缓存按条目精确定向清理，不做全量失效。"""
    _poster_scan_cache.clear()
    _poster_folders_cache.clear()


# ── 文件夹与条目扫描 ──

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


def _duration_seconds(label: Optional[str]) -> int:
    """把 'MM:SS' / 'HH:MM:SS' 标签换算为秒；缺失返回 -1（排序时排最后）。"""
    if not label:
        return -1
    parts = label.split(":")
    if not parts or any(not p.isdigit() for p in parts):
        return -1
    if len(parts) == 2:
        return int(parts[0]) * 60 + int(parts[1])
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    return -1


def _get_or_create_thumb(source_path: Path) -> Path:
    """生成或返回缓存的 WebP 缩略图。"""
    THUMB_CACHE_DIR.mkdir(exist_ok=True)
    try:
        key = hashlib.md5(f"{source_path.stat().st_mtime_ns}::{source_path}".encode()).hexdigest()[:16]
    except OSError:
        return source_path
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


def _ffprobe_duration(video_path: Path) -> Optional[str]:
    """调用 ffprobe 探测时长并格式化标签，失败返回 None（不做缓存）。"""
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
    pending: List[tuple] = []  # (folder, stem, directory, video_path)
    for name in folder_names:
        directory = resolve_media_folder(download_root, name)
        if directory is None:
            continue
        for item in _list_folder_items(directory):
            dk = f"{name}::{item['stem']}"
            dur = duration_store.get(dk)
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
    """后台线程池并行探测时长，结果以 "folder::stem" 为键批量写入持久化缓存。"""
    def probe_one(folder: str, stem: str, vp: Path) -> Optional[Tuple[str, str]]:
        dur = _ffprobe_duration(vp)
        if dur:
            return (f"{folder}::{stem}", dur)
        return None

    results: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(probe_one, folder, stem, vp) for folder, stem, _, vp in pending]
        for f in futures:
            try:
                r = f.result()
            except Exception:
                continue
            if r:
                results[r[0]] = r[1]

    if results:
        duration_store.update(results)


SORT_OPTIONS = {
    "time_desc", "time_asc", "size_desc", "size_asc",
    "duration_desc", "duration_asc",
}


def sort_poster_items(items: List[dict], sort: str) -> List[dict]:
    """按前端同名的排序键排序；duration 缺失按 -1 处理（与旧前端行为一致）。"""
    if sort == "time_asc":
        return sorted(items, key=lambda x: x["stem"])
    if sort == "time_desc":
        return sorted(items, key=lambda x: x["stem"], reverse=True)
    if sort == "size_asc":
        return sorted(items, key=lambda x: x["size"])
    if sort == "size_desc":
        return sorted(items, key=lambda x: x["size"], reverse=True)
    if sort == "duration_asc":
        return sorted(items, key=lambda x: _duration_seconds(x.get("duration")))
    if sort == "duration_desc":
        return sorted(items, key=lambda x: _duration_seconds(x.get("duration")), reverse=True)
    return items
