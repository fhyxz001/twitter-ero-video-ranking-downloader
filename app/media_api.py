"""pektino.com /api/media 客户端与条目规整。"""
import hashlib
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from app.config import MEDIA_API_URL, RANKING_RANGE_OPTIONS, REQUEST_TIMEOUT

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)


def generate_video_id(url: str) -> str:
    """从视频 URL 提取稳定 ID：优先取路径里的 Twitter 数字 ID，其次文件名，最后 MD5。"""
    parsed = urlparse(url)
    path_parts = parsed.path.strip("/").split("/")
    for part in path_parts:
        if part.isdigit() and len(part) >= 15:
            return part
    filename = Path(parsed.path).stem
    if filename:
        return filename
    return hashlib.md5(url.encode()).hexdigest()[:16]


def normalize_media_item(raw: object) -> Optional[dict]:
    """把 /api/media 返回的单个条目规整为内部统一格式（id/url/thumbnail/...）。"""
    if not isinstance(raw, dict):
        return None
    video_id = str(raw.get("id", "")).strip()
    mp4_url = str(raw.get("mp4", "")).strip()
    thumb_url = str(raw.get("thumbnail", "")).strip()
    tweet_url = str(raw.get("tweet_url", "")).strip()
    url_cd = str(raw.get("url_cd", "")).strip()

    # 只认真实 http/https 下载地址；兼容 url / mp4 两种字段名。
    video_url = mp4_url if mp4_url.startswith("http") else ""
    if not video_url:
        for key in ("url", "video_url", "src", "source"):
            candidate = str(raw.get(key, "")).strip()
            if candidate.startswith("http"):
                video_url = candidate
                break
    if not video_url:
        return None

    if not video_id:
        video_id = generate_video_id(video_url)
        if not video_id:
            return None

    return {
        "id": video_id,
        "url": video_url,
        "thumbnail": thumb_url if thumb_url.startswith("http") else "",
        "title": str(raw.get("title", "")).strip() or video_id,
        "pv": raw.get("pv"),
        "favorite": raw.get("favorite"),
        "tweet_url": tweet_url,
        "url_cd": url_cd,
    }


def fetch_pektino_media(
    session: requests.Session,
    proxies,
    ranking_range: str = "daily",
    page: int = 1,
    per_page: int = 30,
) -> Tuple[List[dict], bool]:
    """调用 pektino.com 的 /api/media 接口获取排行榜视频列表，返回 (items, has_next)。"""
    params: Dict[str, object] = {
        "page": max(1, int(page)),
        "per_page": max(1, int(per_page)),
        "ids": "",
        "isAnimeOnly": 0,
        "sort": "pv",
    }
    range_value = str(ranking_range or "daily").strip()
    if range_value in RANKING_RANGE_OPTIONS and range_value != "daily":
        params["range"] = range_value
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    resp = session.get(
        MEDIA_API_URL,
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
        proxies=proxies,
    )
    resp.raise_for_status()
    raw_text = resp.text.strip()
    if not raw_text:
        raise ValueError(f"API 返回空响应（HTTP {resp.status_code}），请检查接口或代理设置")
    try:
        payload = resp.json()
    except json.JSONDecodeError as exc:
        preview = raw_text[:200]
        raise ValueError(f"API 返回非 JSON 内容（{exc}）：{preview}") from exc
    raw_items = payload.get("items", [])
    if not isinstance(raw_items, list):
        raise ValueError("API 返回的 items 不是数组")
    items = [
        normalized
        for normalized in (normalize_media_item(raw) for raw in raw_items)
        if normalized is not None
    ]
    current_page = payload.get("currentPage")
    last_page = payload.get("lastPage")
    if isinstance(current_page, int) and isinstance(last_page, int):
        has_next = current_page < last_page
    else:
        has_next = len(items) >= int(per_page)
    return items, has_next


def normalize_waterfall_item(item: dict) -> Optional[dict]:
    """把内部条目映射为瀑布流接口的输出格式。"""
    if not isinstance(item, dict):
        return None
    video_id = str(item.get("id", "")).strip()
    video_url = str(item.get("url", "")).strip()
    thumbnail_url = str(item.get("thumbnail", "")).strip()
    parsed = urlparse(video_url)
    if not video_id or parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    thumb_parsed = urlparse(thumbnail_url)
    return {
        "id": video_id,
        "url": video_url,
        "preview_url": video_url,
        "thumbnail": (
            thumbnail_url
            if thumb_parsed.scheme in {"http", "https"} and thumb_parsed.netloc
            else ""
        ),
        "title": str(item.get("title") or video_id),
        "pv": item.get("pv"),
        "favorite_count": item.get("favorite"),
        "tweet_url": str(item.get("tweet_url", "")).strip() or None,
    }
