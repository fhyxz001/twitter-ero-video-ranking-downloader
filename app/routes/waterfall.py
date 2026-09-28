"""瀑布流路由：远端榜单浏览与选择下载。"""
from pathlib import Path
from typing import List, Optional

import requests
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.config import (
    ALLOWED_WATERFALL_PAGE_SIZES,
    RANKING_RANGE_OPTIONS,
    config_lock,
    get_current_config,
    load_config,
    resolve_download_root,
    save_config,
)
from app.downloader import build_proxies, download_items
from app.logging_utils import append_log
from app.media_api import fetch_pektino_media, normalize_waterfall_item

router = APIRouter()


class WaterfallSettingsRequest(BaseModel):
    per_page: int = 10


class WaterfallItemIn(BaseModel):
    id: str
    url: str
    thumbnail: str = ""
    title: str = ""
    pv: Optional[int] = None
    favorite: Optional[int] = None
    tweet_url: Optional[str] = None


class WaterfallDownloadRequest(BaseModel):
    items: List[WaterfallItemIn]


@router.get("/api/waterfall")
def api_waterfall(page: int = 1, range: str = ""):
    cfg = get_current_config()
    per_page = int(cfg.get("waterfall_per_page", 10))
    safe_page = max(1, int(page))
    ranking_range = str(range or cfg.get("ranking_range", "daily")).strip()
    if ranking_range not in RANKING_RANGE_OPTIONS:
        ranking_range = "daily"
    proxies = build_proxies(str(cfg.get("proxy", "")).strip())
    try:
        page_items, has_next = fetch_pektino_media(
            proxies, ranking_range, page=safe_page, per_page=per_page
        )
        items = [
            normalized
            for normalized in (normalize_waterfall_item(item) for item in page_items)
            if normalized is not None
        ]
        return JSONResponse({
            "ok": True,
            "items": items,
            "config": {"per_page": per_page},
            "pagination": {
                "page": safe_page,
                "per_page": per_page,
                "has_next": has_next,
            },
        })
    except Exception as exc:
        append_log(f"瀑布流预览加载失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)


@router.post("/api/waterfall/settings")
def save_waterfall_settings(body: WaterfallSettingsRequest):
    try:
        per_page = int(body.per_page)
        if per_page not in ALLOWED_WATERFALL_PAGE_SIZES:
            per_page = 10
        with config_lock:
            cfg = load_config()
            cfg["waterfall_per_page"] = per_page
            save_config(cfg)
        append_log("瀑布流配置已保存")
        return JSONResponse({"ok": True, "config": {"per_page": per_page}})
    except Exception as exc:
        append_log(f"瀑布流配置保存失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@router.post("/api/waterfall/download")
def api_waterfall_download(body: WaterfallDownloadRequest):
    cfg = get_current_config()

    items = [
        normalized
        for normalized in (normalize_waterfall_item(item.model_dump()) for item in body.items)
        if normalized is not None
    ]
    if not items:
        return JSONResponse({"ok": False, "error": "没有可下载的视频"}, status_code=400)

    target_dir = resolve_download_root(cfg["download_root"])
    target_dir.mkdir(parents=True, exist_ok=True)

    proxies = build_proxies(str(cfg.get("proxy", "")).strip())
    session = requests.Session()
    success_count, skip_count, fail_count = download_items(
        session, items, target_dir, len(items), proxies
    )
    return JSONResponse({
        "ok": True,
        "target_dir": str(target_dir),
        "success": success_count,
        "skipped": skip_count,
        "failed": fail_count,
    })
