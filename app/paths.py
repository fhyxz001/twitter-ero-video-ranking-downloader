"""路径解析：适配开发环境与 PyInstaller 打包环境。"""
import sys
from pathlib import Path


def get_resource_path(relative_path: str) -> Path:
    """获取资源文件的绝对路径，适配开发环境和 PyInstaller 编译环境"""
    if getattr(sys, "frozen", False):
        base_path = Path(sys._MEIPASS)
    else:
        base_path = Path(__file__).resolve().parent.parent
    return base_path / relative_path


def get_exe_dir() -> Path:
    """获取程序执行文件所在的目录，用于存放配置文件和运行时缓存"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parent.parent


APP_DIR = get_exe_dir()
CONFIG_PATH = APP_DIR / "config.json"
STATIC_PATH = get_resource_path("static")
VUE_DIST = STATIC_PATH / "dist"
