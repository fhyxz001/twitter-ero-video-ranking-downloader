@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
title Twitter Ero Video Ranking Downloader - 本地调试

set "PY=venv\Scripts\python.exe"

echo.
echo  ============================================================
echo     Twitter Ero Video Ranking Downloader - Windows 本地调试
echo  ============================================================
echo    后端地址 : http://127.0.0.1:2617     前端 : http://127.0.0.1:5173
echo  ============================================================
echo.

if not exist main.py (
    echo  [错误] 请在项目根目录运行本脚本。
    pause
    exit /b 1
)

netstat -ano | findstr /r ":2617 .*LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo  [提示] 端口 2617 已被占用，可能已有实例在运行，建议先关闭旧进程。
    echo.
)

rem ===== 1. venv =====
if exist "%PY%" goto :have_venv
echo  [环境] 未找到 venv，正在创建虚拟环境 ...
python -m venv venv
if errorlevel 1 (
    echo  [错误] 创建虚拟环境失败，请先安装 Python 3.11 并勾选 Add to PATH。
    pause
    exit /b 1
)
"%PY%" -m pip install --upgrade pip -q

:have_venv
rem ===== 2. Python 依赖 =====
"%PY%" -c "import fastapi,uvicorn,requests,curl_cffi,PIL,apscheduler,multipart" >nul 2>&1
if errorlevel 1 (
    echo  [环境] 安装 Python 依赖（首次较慢）...
    "%PY%" -m pip install -r requirements.txt
    if errorlevel 1 (
        echo  [错误] 依赖安装失败，请检查网络后重试。
        pause
        exit /b 1
    )
)
"%PY%" -c "import watchfiles" >nul 2>&1
if errorlevel 1 (
    echo  [环境] 安装 watchfiles（后端热重载依赖）...
    "%PY%" -m pip install watchfiles -q
)

rem ===== 3. ffmpeg 检查 =====
set "FFMPEG_OK=1"
where ffmpeg >nul 2>&1 || set "FFMPEG_OK=0"
where ffprobe >nul 2>&1 || set "FFMPEG_OK=0"
if "%FFMPEG_OK%"=="0" (
    echo  [提示] 未检测到 ffmpeg / ffprobe。
    echo         视频时长会显示为 --:--，封面刮削、时长探测不可用。
    echo         如需要：到 ffmpeg.org 下载，解压后把 bin 目录加入 PATH，重开本脚本。
    echo.
)

rem ===== 4. 配置检查 =====
if not exist config.json goto :cfg_missing
"%PY%" -c "import json,sys,pathlib; p=pathlib.Path('config.json'); c=json.loads(p.read_text(encoding='utf-8')); sys.exit(0 if str(c.get('download_root',''))=='/data/downloads' else 1)" >nul 2>&1
if errorlevel 1 goto :menu
echo  [配置] config.json 仍是 Docker/NAS 路径（download_root=/data/downloads），本机调试会写到错误目录。
set /p "YN=  是否切换为本地调试配置（下载目录 downloads\，清空代理）? [Y/N] "
if /i not "%YN%"=="y" (
    echo  [配置] 保留原配置不变。
    goto :menu
)
goto :cfg_reset

:cfg_missing
echo  [配置] 未找到 config.json，将生成本地调试配置。
goto :cfg_reset

:cfg_reset
"%PY%" -c "import json,pathlib,shutil; p=pathlib.Path('config.json'); c=json.loads(p.read_text(encoding='utf-8')) if p.exists() else {}; shutil.copy(p,'config.json.nas.bak') if p.exists() else None; pathlib.Path('downloads').mkdir(exist_ok=True); out={'download_root':str(pathlib.Path('downloads').resolve()).replace('\\','/'),'poster_root':'','proxy':'','auto_download_enabled':c.get('auto_download_enabled',True),'schedule_cron':c.get('schedule_cron','0 3 * * *'),'max_daily_downloads':c.get('max_daily_downloads',2),'ranking_range':c.get('ranking_range','daily'),'waterfall_per_page':c.get('waterfall_per_page',10)}; p.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8'); print(' [配置] 已写入本地调试配置，download_root = ' + out['download_root'])"
echo.

rem ===== 5. 启动菜单 =====
:menu
set "MODE=%1"
if not "%MODE%"=="" goto :dispatch
echo  ------------------------------------------------------------
echo   请选择启动方式：
echo     [1] 仅后端    访问已构建前端  http://127.0.0.1:2617  (默认)
echo     [2] 后端+前端 Vite 热更新      http://127.0.0.1:5173  (改前端用)
echo     [3] 构建前端  npm run build，产物输出到 static\dist
echo     [4] 退出
echo  ------------------------------------------------------------
set "MODE="
set /p "MODE=  输入数字后回车（直接回车=1）: "
if "%MODE%"=="" set "MODE=1"

:dispatch
if "%MODE%"=="1" goto :mode1
if "%MODE%"=="2" goto :mode2
if "%MODE%"=="3" goto :mode3
if "%MODE%"=="4" exit /b 0
echo  无效输入 %MODE%，请重试。
echo.
goto :menu

:mode1
echo.
echo  [启动] 后端在本窗口运行（热重载已开启），浏览器稍后自动打开，按 Ctrl+C 停止 ...
start /min "" cmd /c "timeout /t 3 /nobreak >nul & start http://127.0.0.1:2617"
"%PY%" -m uvicorn app.server:app --host 127.0.0.1 --port 2617 --reload
echo.
if errorlevel 1 (
    echo  [错误] 后端退出异常，请查看上方日志。
) else (
    echo  [已退出] 后端已停止，可关闭本窗口。
)
pause
exit /b 0

:mode2
echo.
echo  [启动] 后端与前端将在两个新窗口运行，浏览器稍后自动打开前端 ...
start /min "" cmd /c "timeout /t 4 /nobreak >nul & start http://127.0.0.1:5173"
start "后端 2617 (Ctrl+C 停止)" /d "%~dp0" cmd /k "%PY% -m uvicorn app.server:app --host 127.0.0.1 --port 2617 --reload"
start "前端 5173 (Ctrl+C 停止)" /d "%~dp0frontend" cmd /k "npm run dev"
echo  两个新窗口已打开。若前端窗口报错，请先在本窗口选择模式 3 构建，或在前端目录执行 npm install。
pause
exit /b 0

:mode3
where node >nul 2>&1
if errorlevel 1 (
    echo  [错误] 未检测到 node/npm，请先安装 Node.js LTS。
    pause
    exit /b 1
)
echo  [构建] 正在构建前端，产物输出到 static\dist ...
cd /d "%~dp0frontend"
npm run build
if errorlevel 1 (
    echo  [错误] 前端构建失败。
    cd /d "%~dp0"
    pause
    exit /b 1
)
cd /d "%~dp0"
echo  [构建] 完成。
pause
goto :menu
