"""排行榜海报拼图：拉取榜单封面，生成 2 列 × 5 行一组、带序号的图片。

同一档位（day/week/month/year）的榜单快照会短时间缓存，图片接口与链接
接口共用同一份快照，保证图上序号与下载链接一一对应。
"""
import hashlib
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from curl_cffi import requests as curl_requests
from PIL import Image, ImageDraw, ImageFont, ImageOps

from app.logging_utils import append_log
from app.paths import APP_DIR

# 传入的查询类型 → 站点榜单档位。站点没有真正的年榜，year 归到总榜 all。
TYPE_TO_RANGE = {
    "day": "daily",
    "daily": "daily",
    "week": "weekly",
    "weekly": "weekly",
    "month": "monthly",
    "monthly": "monthly",
    "year": "all",
    "all": "all",
}
RANGE_LABELS = {"daily": "日榜", "weekly": "周榜", "monthly": "月榜", "all": "总榜"}

COLS = 2
ROWS = 5
GROUP_SIZE = COLS * ROWS
CELL_W = 288
CELL_H = 512
GROUP_GAP = 12
MAX_GROUPS = 10

SNAPSHOT_TTL = 600  # 榜单快照缓存秒数
THUMB_WORKERS = 6
THUMB_TIMEOUT = 15
CACHE_DIR = APP_DIR / ".rank_poster_cache"

_snapshot_cache: Dict[Tuple[str, int], Tuple[float, List[dict]]] = {}
_snapshot_lock = threading.Lock()
_image_lock = threading.Lock()
_font_cache: Dict[int, ImageFont.ImageFont] = {}
_placeholder: Optional[Image.Image] = None

_FONT_CANDIDATES = (
    "C:/Windows/Fonts/arialbd.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "DejaVuSans-Bold.ttf",
    "DejaVuSans.ttf",
)


def resolve_range(type_value: str) -> Optional[str]:
    """把外部查询类型解析为站点榜单档位，非法值返回 None。"""
    return TYPE_TO_RANGE.get(str(type_value or "").strip().lower())


def _build_proxies():
    # 延迟导入，避免仅做图片渲染时也拉起配置/调度器依赖
    from app.config import get_current_config
    from app.downloader import build_proxies

    return build_proxies(str(get_current_config().get("proxy", "")).strip())


def get_snapshot(range_key: str, groups: int) -> List[dict]:
    """取该档位前 groups*10 条，带短时缓存，保证图片与链接使用同一份数据。"""
    cache_key = (range_key, groups)
    now = time.monotonic()
    with _snapshot_lock:
        cached = _snapshot_cache.get(cache_key)
        if cached and cached[0] > now:
            return list(cached[1])

    from app.media_api import fetch_pektino_media

    proxies = _build_proxies()
    needed = groups * GROUP_SIZE
    per_page = 100 if needed > 50 else (50 if needed > 10 else 10)
    items: List[dict] = []
    page = 1
    while len(items) < needed:
        page_items, has_next = fetch_pektino_media(
            proxies, range_key, page=page, per_page=per_page
        )
        if not page_items:
            break
        items.extend(page_items)
        if not has_next:
            break
        page += 1

    items = items[:needed]
    with _snapshot_lock:
        _snapshot_cache[cache_key] = (time.monotonic() + SNAPSHOT_TTL, items)
    return list(items)


def build_links(items: List[dict]) -> List[dict]:
    """序号（从 1 开始，跨组连续）→ 视频地址。"""
    return [
        {
            "group": idx // GROUP_SIZE + 1,
            "index": idx + 1,
            "video_url": str(item.get("url", "")).strip(),
        }
        for idx, item in enumerate(items)
    ]


def get_or_create_collage(range_key: str, groups: int, items: List[dict]) -> Path:
    """生成并缓存拼图文件；缓存键包含榜单条目 ID，榜单变化后自动重绘。"""
    signature = hashlib.md5(
        "|".join(str(item.get("id", "")) for item in items).encode("utf-8")
    ).hexdigest()[:16]
    path = CACHE_DIR / f"{range_key}_{groups}_{signature}.jpg"
    if path.exists() and path.stat().st_size > 0:
        return path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with _image_lock:
        if path.exists() and path.stat().st_size > 0:
            return path
        image = render_collage(groups, items)
        tmp_path = path.with_name(path.name + ".tmp")
        image.save(tmp_path, "JPEG", quality=85, optimize=True)
        os.replace(tmp_path, path)
    return path


def render_collage(groups: int, items: List[dict]) -> Image.Image:
    """按 2 列 × 5 行一组纵向拼接，编号全局连续。"""
    group_height = ROWS * CELL_H
    height = groups * group_height + max(0, groups - 1) * GROUP_GAP
    canvas = Image.new("RGB", (COLS * CELL_W, height), (16, 16, 18))

    proxies = _build_proxies()
    urls = [str(item.get("thumbnail", "")).strip() for item in items]
    thumbnails = _download_thumbnails(urls, proxies)

    for idx in range(min(len(items), groups * GROUP_SIZE)):
        group = idx // GROUP_SIZE
        pos = idx % GROUP_SIZE
        row, col = divmod(pos, COLS)
        left = col * CELL_W
        top = group * (group_height + GROUP_GAP) + row * CELL_H
        thumbnail = thumbnails[idx]
        cell = (
            ImageOps.fit(thumbnail, (CELL_W, CELL_H), Image.LANCZOS)
            if thumbnail is not None
            else _get_placeholder()
        )
        canvas.paste(cell, (left, top))
        _draw_index(canvas, idx + 1, left, top)

    return canvas


def _download_thumbnails(urls: List[str], proxies) -> List[Optional[Image.Image]]:
    if not urls:
        return []
    with ThreadPoolExecutor(max_workers=min(THUMB_WORKERS, len(urls))) as pool:
        results = list(pool.map(lambda url: _download_one(url, proxies), urls))
    failed = sum(1 for url, image in zip(urls, results) if url and image is None)
    if failed:
        append_log(f"排行榜海报：{failed} 张封面下载失败，已使用占位图")
    return results


def _download_one(url: str, proxies) -> Optional[Image.Image]:
    if not url:
        return None
    try:
        resp = curl_requests.get(
            url,
            timeout=THUMB_TIMEOUT,
            proxies=proxies or None,
            impersonate="chrome",
        )
        if resp.status_code != 200 or not resp.content:
            return None
        image = Image.open(BytesIO(resp.content))
        image.load()
        return image.convert("RGB")
    except Exception:
        return None


def _get_placeholder() -> Image.Image:
    global _placeholder
    if _placeholder is None:
        _placeholder = Image.new("RGB", (CELL_W, CELL_H), (38, 38, 42))
    return _placeholder


def _get_font(size: int) -> ImageFont.ImageFont:
    font = _font_cache.get(size)
    if font is not None:
        return font
    for candidate in _FONT_CANDIDATES:
        try:
            font = ImageFont.truetype(candidate, size)
            break
        except OSError:
            continue
    else:
        try:
            font = ImageFont.load_default(size=size)
        except TypeError:  # 旧版 Pillow 不支持指定字号
            font = ImageFont.load_default()
    _font_cache[size] = font
    return font


def _draw_index(canvas: Image.Image, number: int, left: int, top: int) -> None:
    """在每张封面左上角画序号角标。"""
    draw = ImageDraw.Draw(canvas)
    text = str(number)
    font = _get_font(int(CELL_H * 0.06))
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    pad = max(6, int(CELL_W * 0.03))
    box_w = text_w + pad * 2
    box_h = text_h + pad * 2
    x = left + pad
    y = top + pad
    draw.rounded_rectangle(
        [x, y, x + box_w, y + box_h],
        radius=max(4, pad // 2),
        fill=(12, 12, 14),
    )
    draw.text(
        (x + pad - bbox[0], y + pad - bbox[1]),
        text,
        font=font,
        fill=(255, 255, 255),
    )
