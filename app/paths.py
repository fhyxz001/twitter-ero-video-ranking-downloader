"""路径解析。"""
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = APP_DIR / "config.json"
STATIC_PATH = APP_DIR / "static"
VUE_DIST = STATIC_PATH / "dist"
