"""路由包：按域拆分的 APIRouter 集合。"""
from app.routes import poster, system, waterfall  # noqa: F401

__all__ = ["poster", "system", "waterfall"]
