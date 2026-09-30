"""系统路由：健康检查、状态、配置读写、手动触发、目录校验。"""
import threading
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.config import (
    config_lock,
    get_current_config,
    load_config,
    public_config,
    save_config,
)
from app.downloader import (
    build_proxies,
    run_download_job,
    runtime_state,
    update_schedule,
)
from app.logging_utils import append_log, get_logs
from app.media_api import probe_source
from app.poster import invalidate_scan_cache

router = APIRouter()


class SaveConfigRequest(BaseModel):
    download_root: str
    poster_root: str = ""
    proxy: str = ""
    auto_download_enabled: bool = True
    schedule_cron: str = "0 3 * * *"
    max_daily_downloads: int = 10
    ranking_range: str = "daily"


class QuickSaveRequest(BaseModel):
    download_root: str


class CheckProxyRequest(BaseModel):
    proxy: str = ""


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/status")
def status():
    return JSONResponse(
        {
            "ok": True,
            "state": runtime_state,
            "logs": get_logs(),
            "config": public_config(get_current_config()),
        }
    )


@router.post("/save")
def save(body: SaveConfigRequest):
    try:
        with config_lock:
            cfg = load_config()
            roots_changed = (
                cfg.get("download_root") != body.download_root.strip()
                or cfg.get("poster_root", "") != body.poster_root.strip()
            )
            cfg.update(
                {
                    "download_root": body.download_root.strip(),
                    "poster_root": body.poster_root.strip(),
                    "proxy": body.proxy.strip(),
                    "auto_download_enabled": body.auto_download_enabled,
                    "schedule_cron": body.schedule_cron.strip(),
                    "max_daily_downloads": body.max_daily_downloads,
                    "ranking_range": body.ranking_range.strip(),
                }
            )
            save_config(cfg)
        update_schedule(get_current_config())
        if roots_changed:
            # 目录变化后立刻失效扫描缓存，本地视频页面无需等 TTL 过期
            invalidate_scan_cache()
        append_log("配置已保存")
        return JSONResponse({"ok": True})
    except Exception as exc:
        append_log(f"配置保存失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@router.post("/save-quick")
def save_quick(body: QuickSaveRequest):
    try:
        with config_lock:
            cfg = load_config()
            cfg["download_root"] = body.download_root.strip()
            save_config(cfg)
            updated = load_config()
        append_log("下载根目录已自动保存")
        return JSONResponse({"ok": True, "config": public_config(updated)})
    except Exception as exc:
        append_log(f"自动保存下载根目录失败：{exc}")
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)


@router.post("/run-now")
def run_now():
    messages = []
    if runtime_state["is_running"]:
        messages.append("下载任务正在运行中")
    else:
        threading.Thread(target=run_download_job, daemon=True).start()
        append_log("已触发手动执行下载任务")
        messages.append("下载任务已启动")

    return JSONResponse({"ok": True, "message": "；".join(messages)})


@router.get("/api/check-dir")
def api_check_dir(path: str = ""):
    """校验下载目录是否存在"""
    p = Path(path.strip()) if path else None
    if not p:
        return JSONResponse({"ok": False, "error": "路径不能为空"})
    if p.exists() and p.is_dir():
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": False, "error": "目录不存在"})


@router.post("/api/check-proxy")
def api_check_proxy(body: CheckProxyRequest):
    """用设置页正在填写的代理与直连各探测一次数据源，返回两条链路的结果供对照。"""
    proxy = body.proxy.strip()
    proxy_result = probe_source(build_proxies(proxy)) if proxy else None
    direct_result = probe_source(None)
    summary = "直连 " + ("正常" if direct_result["ok"] else "失败")
    if proxy_result is not None:
        summary = "代理 " + ("正常" if proxy_result["ok"] else "失败") + "；" + summary
    else:
        summary = "未填写代理，仅测试直连；" + summary
    append_log(f"代理连通性检测：{summary}")
    return JSONResponse({"ok": True, "proxy": proxy_result, "direct": direct_result})
