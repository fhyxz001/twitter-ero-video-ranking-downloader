import axios from 'axios'
import { ElMessage } from 'element-plus'

const http = axios.create({
  timeout: 30000,
})

http.interceptors.response.use(
  (response) => response,
  (error) => {
    const msg = error?.response?.data?.error || error?.message || '网络错误'
    ElMessage.error(msg)
    return Promise.reject(error)
  }
)

export default http

// ===== Config / Status =====

export interface AppConfig {
  download_root: string
  poster_root: string
  proxy: string
  auto_download_enabled: boolean
  schedule_cron: string
  max_daily_downloads: number
  ranking_range: string
  waterfall_per_page: number
  open_api_key: string
}

export in