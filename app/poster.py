"""本地视频：媒体扫描、缩略图生成与多级缓存。

缓存分两层：
- 文件夹/条目扫描缓存：内存 TTL 60s，任何写操作后整体清空。
- 缩略图缓存：WebP 文件，按源图 mtime 哈希命名。
"""
import hashlib
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

from PIL import Image

from app.config import IMAGE_EXTS, POSTER_VIDEO_EXTS
from app.paths import APP_DIR

POSTER_SCAN_TTL = 60.0  # 秒
POSTER_MAX_PAGE_SIZE = 100

_poster_scan_cache: Dict[str, Tuple[float, dict]] = {}
_poster_folders_cache: Dict[str, Tuple[float, List[dict]]] = {}

THUMB_CACHE_DIR = APP_DIR / ".thumb_cache"
THUMB_SIZE = (320, 180)  # 16:9, 2x 显示密度

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
        if p.is_file() and p.suffix.lower() in POSTER_VIDEO_EXTS
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
    videos = {p.stem: p for p in directory.iterdir() if p.is_file() and p.suffix.lower() in POSTER_VIDEO_EXTS}
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


def read_strm_target(strm_path: Path) -> Optional[str]:
    """读取 .strm 文本内容，解析出播放目标。

    内容取首个非空行：http(s) 链接原样返回；否则视为本地/UNC 路径，
    相对路径按 .strm 所在目录展开。内容为空或读不了时返回 None。
    """
    try:
        text = strm_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    line = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
    if not line:
        return None
    if line.lower().startswith(("http://", "https://")):
        return line
    p = Path(line).expanduser()
    if not p.is_absolute():
        p = strm_path.parent / p
    return str(p)


def is_streaming_url(target: str) -> bool:
    return target.lower().startswith(("http://", "https://"))


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
    for name in folder_names:
        directory = resolve_media_folder(download_root, name)
        if directory is None:
            continue
        for item in _list_folder_items(directory):
            items.append(_build_poster_item(name, item))

    items.sort(key=lambda x: (x["folder"], x["stem"]))
    _scan_cache_set(cache_key, {"items": items})
    return items


SORT_OPTIONS = {"time_desc", "time_asc", "size_desc", "size_asc"}


def sort_poster_items(items: List[dict], sort: str) -> List[dict]:
    """按前端同名的排序键排序。"""
    if sort == "time_asc":
        return sorted(items, key=lambda x: x["stem"])
    if sort == "time_desc":
        return sorted(items, key=lambda x: x["stem"], reverse=True)
    if sort == "size_asc":
        return sorted(items, key=lambda x: x["size"])
    if sort == "size_desc":
        return sorted(items, key=lambda x: x["size"], reverse=True)
    return items
