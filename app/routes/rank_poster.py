"""排行榜海报路由：图片流 + 序号对应的下载链接列表。"""
from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse

from app.logging_utils import append_log
from app.rank_poster import (
    MAX_GROUPS,
    RANGE_LABELS,
    build_links,
    get_or_create_collage,
    get_snapshot,
    resolve_range,
)

router = APIRouter()


def _validate(type_value: str, groups: int):
    range_key = resolve_range(type_value)
    if range_key is None:
        return None, JSONResponse(
            {"ok": False, "error": "type 仅支持 day / week / month / year"},
            status_code=400,
        )
    if groups < 1 or groups > MAX_GROUPS:
        return None, JSONResponse(
            {"ok": False, "error": f"groups 取值范围为 1-{MAX_GROUPS}"},
            status_code=400,
        )
    return range_key, None


@router.get("/api/rank-poster/image")
def api_rank_poster_image(type: str = "day", groups: int = 1):
    """返回拼图图片流：每行 2 张、每 10 张一组，组数由 groups 决定。"""
    range_key, error = _validate(type, groups)
    if error is not None:
        return error
    try:
        items = get_snapshot(range_key, groups)
        if not items:
            return JSONResponse({"ok": False, "error": "该榜单暂无数据"}, status_code=404)
        path = get_or_create_collage(range_key, groups, items)
    except Exception as exc:
        append_log(f"排行榜海报生成失败（{range_key}，{groups} 组）：{exc}")
        return JSONResponse({"ok": False, "error": f"海报生成失败：{exc}"}, status_code=502)
    return FileResponse(
        str(path),
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=600"},
    )


@router.get("/api/rank-poster/links")
def api_rank_poster_links(type: str = "day", groups: int = 1):
    """返回与图片序号一一对应的视频下载地址列表。"""
    range_key, error = _validate(type, groups)
    if error is not None:
        return error
    try:
        items = get_snapshot(range_key, groups)
    except Exception as exc:
        append_log(f"排行榜下载链接获取失败（{range_key}，{groups} 组）：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=502)

    links = build_links(items)
    return JSONResponse({
        "ok": True,
        "type": range_key,
        "label": RANGE_LABELS.get(range_key, range_key),
        "groups": groups,
        "count": len(links),
        "items": links,
    })
