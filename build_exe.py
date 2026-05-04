import os
import sys
import subprocess
import importlib.util


def ensure_pyinstaller():
    """Ensure PyInstaller is available in the current Python environment."""
    if importlib.util.find_spec("PyInstaller") is not None:
        return

    print("未发现 PyInstaller，正在尝试安装到当前 Python 环境中...")
    # Use the current interpreter to avoid installing into the wrong Python.
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])
    except Exception as e:
        print("自动安装 PyInstaller 失败：", e)
        print("")
        print("建议使用虚拟环境安装依赖后再打包（Windows PowerShell）：")
        print(r"  py -m venv .venv")
        print(r"  .\.venv\Scripts\python -m pip install -U pip")
        print(r"  .\.venv\Scripts\python -m pip install -r requirements.txt pyinstaller")
        print(r"  .\.venv\Scripts\python build_exe.py")
        raise

def build():
    # 确保在当前目录下运行
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(current_dir)

    # 定义资源文件 (Windows 使用 ; 分隔)
    # 格式: "源路径;目标路径"
    add_data = "templates;templates"

    # PyInstaller 参数
    params = [
        'main.py',              # 入口文件
        '--onefile',            # 打包成单个 exe 文件
        '--name', 'twitter-downloader', # 生成的可执行文件名
        '--add-data', add_data, # 包含模板文件夹
        '--clean',              # 清理临时文件
        # '--noconsole',        # 如果需要隐藏控制台窗口，可以取消注释
        '--collect-all', 'uvicorn', # 收集 uvicorn 的所有依赖
    ]

    print(f"正在开始打包 Twitter 下载器...")
    import PyInstaller.__main__
    PyInstaller.__main__.run(params)
    print(f"打包完成！可执行文件位于 dist 目录中。")

if __name__ == "__main__":
    ensure_pyinstaller()
    build()
