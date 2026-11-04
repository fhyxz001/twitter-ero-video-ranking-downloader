"""程序入口：Docker / python 直接运行统一从这里启动。

业务代码按职责拆分在 app/ 包内：
  paths.py        路径解析
  config.py       配置常量与读写（白名单制 + mtime 缓存）
  logging_utils.py 日志（内存环形缓冲 + 落盘滚动文件）
  media_api.py    pektino.com /api/media 客户端与条目规整
  downloader.py   下载任务（并行 + 重试）与定时调度
  poster.py       本地视频扫描与缩略图缓存
  rank_poster.py  排行榜海报拼图与下载链接
  open_download.py 对外开放下载接口（key 校验 + 异步任务）
  routes/         路由（system / waterfall / poster / rank_poster / open_download）
  server.py       FastAPI 应用装配
"""
import uvicorn

from app.server import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=2617)
