"""对外开放下载接口：凭 key 提交下载任务，并按 task_id 查询进度。"""
from pathlib import Path
from urllib.parse import urlparse

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.logging_utils import append_log
from app.open_download import (
    create_task,
    get_task,
    normalize_url,
    resolve_target_path,
    verify_key,
)

router = APIRouter()


class OpenDownloadRequest(BaseModel):
    url: str
    path: str
    key: str = ""


@router.post("/api/open/download")
def api_open_download(body: OpenDownloadRequest):
    """异步下载：校验 key 与参数后立即返回 task_id。"""
    if not verify_key(body.key):
        return JSONResponse(
            {"ok": False, "error": "key 无效或未在设置中启用"},
            status_code=401,
        )

    url = normalize_url(body.url)
    if not url:
        return JSONResponse(
            {"ok": False, "error": "下载链接必须是 http/https 直链"},
            status_code=400,
        )

    try:
        target = resolve_target_path(body.path)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    # 传的是已存在的目录时，用链接里的文件名补全
    if target.is_dir():
        target = target / (Path(urlparse(url).path).name or "download.mp4")

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return JSONResponse(
            {"ok": False, "error": f"无法创建目标目录：{exc}"},
            status_code=400,
        )

    append_log(f"收到外部下载请求：{url} -> {target}")
    task = create_task(url, target)
    return JSONResponse({"ok": True, "task": task})


@router.get("/api/open/download/{task_id}")
def api_open_download_status(task_id: str):
    task = get_task(task_id)
    if not task:
        return JSONResponse(
            {"ok": False, "error": "任务不存在或已过期"},
            status_code=404,
        )
    return JSONResponse({"ok": True, "task": task})
