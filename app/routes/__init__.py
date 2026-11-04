"""路由包：按域拆分的 APIRouter 集合。"""
from app.routes import open_download, poster, rank_poster, system, waterfall  # noqa: F401

__all__ = ["open_download", "poster", "rank_poster", "system", "waterfall"]
