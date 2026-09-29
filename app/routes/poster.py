"""海报墙路由：本地视频浏览（服务端分页/排序）、缩略图、预览、删除、换封面。"""
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

from app.config import (
    IMAGE_EXTS,
    POSTER_VIDEO_EXTS,
    get_current_config,
    resolve_poster_root,
)
from app.logging_utils import append_log
from app.poster import (
    POSTER_MAX_PAGE_SIZE,
    SORT_OPTIONS,
    duration_store,
    invalidate_scan_cache,
    is_streaming_url,
    read_strm_target,
    resolve_media_folder,
    sort_poster_items,
    _collect_poster_items,
    _get_or_create_thumb,
    _media_folders,
)
from app.scraper import scrape_status, start_scrape_job

router = APIRouter()


class PosterDeleteRequest(BaseModel):
    folder: str = ""
    stems: List[str] = []


class ScrapeCoversRequest(BaseModel):
    folder: str = ""


@router.get("/api/poster")
def api_poster_all(
    folder: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    sort: str = "time_desc",
):
    cfg = get_current_config()
    root = resolve_poster_root(cfg)
    if folder is not None and resolve_media_folder(root, folder) is None:
        return JSONResponse({"ok": False, "error": "无效文件夹"}, status_code=400)

    safe_page = max(1, int(page))
    safe_page_size = min(max(1, int(page_size)), POSTER_MAX_PAGE_SIZE)
    sort_key = sort if sort in SORT_OPTIONS else "time_desc"

    all_items = _collect_poster_items(root, folder=folder)
    ordered = sort_poster_items(all_items, sort_key)
    total = len(ordered)
    start = (safe_page - 1) * safe_page_size
    page_items = ordered[start:start + safe_page_size]

    return JSONResponse({
        "ok": True,
        "folder": folder,
        "folders": _media_folders(root),
        "items": page_items,
        "pagination": {
            "page": safe_page,
            "page_size": safe_page_size,
            "total": total,
            "has_next": start + safe_page_size < total,
        },
    })


@router.get("/api/poster-thumb")
def api_thumb(folder: str = "", name: str = ""):
    cfg = get_current_config()
    root = resolve_poster_root(cfg)
    directory = resolve_media_folder(root, folder)
    if directory is None or "/" in name or "\\" in name:
        return JSONResponse({"error": "无效路径"}, status_code=400)
    path = directory / name
    if not path.exists() or not path.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    resolved = path.resolve()
    if not resolved.is_relative_to(root.resolve()):
        return JSONResponse({"error": "禁止访问"}, status_code=403)
    # 生成 WebP 缩略图
    thumb_path = _get_or_create_thumb(resolved)
    mimetype = "image/webp" if thumb_path.suffix.lower() == ".webp" else None
    return FileResponse(
        str(thumb_path),
        media_type=mimetype,
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.get("/api/poster-durations")
def api_poster_durations(folder: str = ""):
    """返回已探测完成的视频时长，供前端轮询补充。"""
    cfg = get_current_config()
    root = resolve_poster_root(cfg)
    if folder and resolve_media_folder(root, folder) is None:
        return JSONResponse({"ok": False, "error": "无效文件夹"}, status_code=400)

    persistent_cache = duration_store.snapshot()
    matching = {}
    prefix = f"{folder}::" if folder else ""
    for k, v in persistent_cache.items():
        if not prefix or k.startswith(prefix):
            matching[k] = v
    return JSONResponse({"ok": True, "durations": matching})


@router.get("/api/poster-video")
def api_video(folder: str = "", name: str = ""):
    cfg = get_current_config()
    root = resolve_poster_root(cfg)
    directory = resolve_media_folder(root, folder)
    if directory is None or "/" in name or "\\" in name:
        return JSONResponse({"error": "无效路径"}, status_code=400)
    path = directory / name
    if not path.exists() or not path.is_file():
        return JSONResponse({"error": "文件不存在"}, status_code=404)
    if path.suffix.lower() not in POSTER_VIDEO_EXTS:
        return JSONResponse({"error": "不是视频文件"}, status_code=400)
    resolved = path.resolve()
    if not resolved.is_relative_to(root.resolve()):
        return JSONResponse({"error": "禁止访问"}, status_code=403)
    if resolved.suffix.lower() == ".strm":
        target = read_strm_target(resolved)
        if not target:
            return JSONResponse({"error": "strm 文件内容为空或无效"}, status_code=400)
        if is_streaming_url(target):
            return RedirectResponse(target, status_code=302)
        target_path = Path(target)
        if not target_path.exists() or not target_path.is_file():
            return JSONResponse({"error": "strm 指向的本地文件不存在"}, status_code=404)
        return FileResponse(str(target_path))
    return FileResponse(str(resolved))


@router.post("/api/poster/delete")
def api_delete(body: PosterDeleteRequest):
    folder = body.folder
    stems = body.stems
    cfg = get_current_config()
    root = resolve_poster_root(cfg)
    directory = resolve_media_folder(root, folder)
    if directory is None:
        return JSONResponse({"ok": False, "error": "无效文件夹"}, status_code=400)
    deleted = []
    for stem in stems:
        stem_str = str(stem)
        if "/" in stem_str or "\\" in stem_str:
            continue
        for p in list(directory.iterdir()):
            if p.stem == stem_str and p.suffix.lower() in POSTER_VIDEO_EXTS | IMAGE_EXTS:
                p.unlink(missing_ok=True)
                deleted.append(p.name)
    # 定向清理已删除条目的时长记录，避免影响其余视频的缓存
    duration_store.remove_stems(folder, stems)
    invalidate_scan_cache()
    if deleted:
        append_log(f"已删除 {len(deleted)} 个文件（{folder or '根目录'}）")
    return JSONResponse({"ok": True, "deleted": deleted})


@router.post("/api/poster/replace-cover")
def api_replace_cover(
    folder: str = Form(""),
    stem: str = Form(...),
    file: UploadFile = File(...),
):
    cfg = get_current_config()
    root = resolve_poster_root(cfg)
    directory = resolve_media_folder(root, folder)
    if directory is None or "/" in stem or "\\" in stem:
        return JSONResponse({"ok": False, "error": "无效路径"}, status_code=400)
    suffix = Path(file.filename or "cover.jpg").suffix.lower() or ".jpg"
    if suffix not in IMAGE_EXTS:
        return JSONResponse({"ok": False, "error": "不支持的图片格式"}, status_code=400)
    # Remove old thumb files for this stem
    for p in list(directory.iterdir()):
        if p.stem == stem and p.suffix.lower() in IMAGE_EXTS:
            p.unlink(missing_ok=True)
    new_path = directory / f"{stem}{suffix}"
    content = file.file.read()
    new_path.write_bytes(content)
    # 换封面不影响视频时长，只需刷新扫描缓存
    invalidate_scan_cache()
    append_log(f"已替换封面：{folder or '根目录'}/{stem}{suffix}")
    return JSONResponse({"ok": True, "thumb": new_path.name})


@router.post("/api/poster/scrape-covers")
def api_scrape_covers(body: ScrapeCoversRequest):
    """为缺失封面的视频抽取首帧生成 jpg；folder 空=根目录+全部一级子文件夹。"""
    cfg = get_current_config()
    root = resolve_poster_root(cfg)
    folder = body.folder.strip()
    if folder:
        directory = resolve_media_folder(root, folder)
        if directory is None:
            return JSONResponse({"ok": False, "error": "无效文件夹"}, status_code=400)
        targets = [directory]
    else:
        targets = [root] + [root / f["folder"] for f in _media_folders(root) if f["folder"]]
    started = start_scrape_job(targets)
    if started:
        append_log(f"已触发封面刮削：{folder or '全部文件夹'}")
    return JSONResponse({"ok": True, "started": started})


@router.get("/api/poster/scrape-covers/status")
def api_scrape_status():
    """返回封面刮削作业进度，供前端轮询。"""
    return JSONResponse({"ok": True, **scrape_status()})
