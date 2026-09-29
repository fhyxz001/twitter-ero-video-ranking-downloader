"""FastAPI 应用装配：中间件、路由注册、生命周期、SPA 兜底。"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

from app.config import VUE_DIST, get_current_config
from app.downloader import scheduler, update_schedule
from app.logging_utils import append_log
from app.paths import STATIC_PATH
from app.routes import poster, system, waterfall


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    cfg = get_current_config()
    if not scheduler.running:
        scheduler.start()
    update_schedule(cfg)
    append_log("服务启动完成")
    yield
    # shutdown
    if scheduler.running:
        scheduler.shutdown(wait=False)
    append_log("服务已停止")


app = FastAPI(title="twitter-ero-video-ranking-downloader", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_PATH)), name="static")
app.add_middleware(GZipMiddleware, minimum_size=500)

# 注意：不挂 CORSMiddleware。生产环境前后端同源（前端由本服务托管），
# 开发环境走 vite proxy，均无需跨域；放开 CORS 会让任意网页可跨域调用
# 删除文件、触发下载等无鉴权接口。


# ── 缓存策略中间件 ──
@app.middleware("http")
async def add_cache_headers(request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    elif path.startswith("/api/poster-thumb"):
        if "Cache-Control" not in response.headers:
            response.headers["Cache-Control"] = "public, max-age=86400"
        response.headers["Vary"] = "Accept-Encoding"
    elif path.startswith("/api/poster-video"):
        # strm 播放会 302 到 strm 内的目标地址，重定向不可缓存，否则改写 strm 后不生效
        if response.status_code == 200:
            response.headers["Cache-Control"] = "public, max-age=3600"
    return response


app.include_router(system.router)
app.include_router(waterfall.router)
app.include_router(poster.router)


# ── SPA 兜底路由（必须放在所有 API 路由之后） ──
@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    # 未知 API 路径返回 404 JSON，而不是把 index.html 吞给前端造成解析错误
    if full_path == "api" or full_path.startswith("api/"):
        return JSONResponse({"error": "接口不存在"}, status_code=404)
    index = VUE_DIST / "index.html"
    if not index.exists():
        return JSONResponse(
            {"error": "前端未构建，请在 frontend/ 目录运行 npm run build"},
            status_code=500,
        )
    return FileResponse(str(index))
