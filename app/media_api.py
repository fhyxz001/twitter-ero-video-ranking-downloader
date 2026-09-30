"""pektino.com /api/media 客户端与条目规整。"""
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from curl_cffi import requests

from app.config import MEDIA_API_URL, RANKING_RANGE_PARAMS, REQUEST_TIMEOUT

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# 连通性探测用的轻量超时，避免设置页的检测按钮卡顿过久
PROBE_TIMEOUT = 10


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


def _friendly_curl_error(exc: Exception) -> str:
    """把 curl 连接类错误翻译成带排查提示的信息（curl: (35) TLS被重置 等）。"""
    msg = str(exc)[:300]
    match = re.search(r"curl: \((\d+)\)", msg)
    if match and match.group(1) in {"6", "7", "28", "35", "56"}:
        hint = (
            "无法连通 pektino.com：请检查设置里的 HTTP 代理是否为部署环境可访问的地址"
            "（容器内不要写 127.0.0.1），或当前网络到该站点的连接被阻断了"
        )
        return f"{msg}（{hint}）"
    return msg


def probe_source(proxies: Optional[Dict[str, str]], timeout: int = PROBE_TIMEOUT) -> dict:
    """对数据源做一次轻量连通性探测，供设置页"检测连接"使用。

    ok 表示网络链路可用（拿到 HTTP 200 且响应是合法 JSON），与榜单当前是否有数据无关。
    """
    params: Dict[str, object] = {
        "page": 1,
        "per_page": 1,
        "range": "all",
        "ids": "",
        "category": "",
        "isFilteredOnly": "0",
        "sort": "pv",
    }
    started = time.monotonic()
    try:
        resp = requests.get(
            MEDIA_API_URL,
            params=params,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            timeout=timeout,
            proxies=proxies or None,
            impersonate="chrome",
        )
    except Exception as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        return {"ok": False, "status": None, "elapsed_ms": elapsed, "items": None,
                "error": _friendly_curl_error(exc)}

    elapsed = int((time.monotonic() - started) * 1000)
    if resp.status_code != 200:
        return {"ok": False, "status": resp.status_code, "elapsed_ms": elapsed, "items": None,
                "error": f"HTTP {resp.status_code}"}
    try:
        items = resp.json().get("items", [])
    except (json.JSONDecodeError, ValueError):
        return {"ok": False, "status": resp.status_code, "elapsed_ms": elapsed, "items": None,
                "error": "响应不是合法 JSON"}
    return {"ok": True, "status": resp.status_code, "elapsed_ms": elapsed,
            "items": len(items) if isinstance(items, list) else None, "error": None}


def fetch_pektino_media(
    proxies,
    ranking_range: str = "daily",
    page: int = 1,
    per_page: int = 30,
) -> Tuple[List[dict], bool]:
    """调用 pektino.com 的 /api/media 接口获取排行榜视频列表，返回 (items, has_next)。

    请求参数与站点前端保持一致：range 必须始终携带（日榜的取值是 timely，
    缺省或不发会被当作无效档位返回空列表），排序沿用按播放量（pv）。
    """
    params: Dict[str, object] = {
        "page": max(1, int(page)),
        "per_page": max(1, int(per_page)),
        "range": RANKING_RANGE_PARAMS.get(str(ranking_range or "daily").strip(), "timely"),
        "ids": "",
        "category": "",
        "isFilteredOnly": "0",
        "sort": "pv",
    }
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    try:
        resp = requests.get(
            MEDIA_API_URL,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            proxies=proxies,
            impersonate="chrome",
        )
    except Exception as exc:
        raise ValueError(_friendly_curl_error(exc)) from exc
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
    """把内部条目映射为远程视频接口的输出格式。"""
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
