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
  proxy: string
  auto_download_enabled: boolean
  schedule_cron: string
  max_daily_downloads: number
  ranking_range: string
  waterfall_per_page: number
  twitter_cookie: string
  twitter_blogger_list: string[]
  twitter_blogger_enabled: boolean
  twitter_blogger_cron: string
  twitter_blogger_max_media: number
  twitter_blogger_has_retweet: boolean
}

export interface RuntimeState {
  is_running: boolean
  last_run_time: string | null
  last_result: string
}

export interface BloggerState {
  is_running: boolean
  last_run_time: string | null
  last_result: string
}

export interface StatusResponse {
  ok: boolean
  state: RuntimeState
  blogger_state: BloggerState
  logs: string[]
  config: AppConfig
}

export const api = {
  getStatus: () => http.get<StatusResponse>('/status').then((r) => r.data),
  runNow: () => http.post<{ ok: boolean; message?: string }>('/run-now').then((r) => r.data),
  saveConfig: (form: FormData) =>
    http.post<{ ok: boolean; error?: string }>('/save', form).then((r) => r.data),
  saveQuickConfig: (form: FormData) =>
    http.post<{ ok: boolean; config?: AppConfig; error?: string }>('/save-quick', form).then((r) => r.data),
  checkDir: (path: string) =>
    http.get<{ ok: boolean; error?: string }>('/api/check-dir', { params: { path } }).then((r) => r.data),
  saveWaterfallSettings: (per_page: number) =>
    http
      .post<{ ok: boolean; config?: { per_page: number }; error?: string }>('/api/waterfall/settings', {
        per_page,
      })
      .then((r) => r.data),
}

// ===== Blogger =====

export interface Blogger {
  screen_name: string
  name: string
  profile_image_url: string
  description: string
  file_count: number
}

export interface BloggerListResponse {
  ok: boolean
  bloggers: Blogger[]
  state: BloggerState
  settings: {
    twitter_blogger_enabled: boolean
    twitter_blogger_cron: string
    twitter_blogger_max_media: number
    twitter_blogger_has_retweet: boolean
    twitter_cookie_set: boolean
  }
}

export const bloggerApi = {
  list: () => http.get<BloggerListResponse>('/api/blogger/list').then((r) => r.data),
  add: (screen_name: string) =>
    http.post<{ ok: boolean; error?: string }>('/api/blogger/add', { screen_name }).then((r) => r.data),
  remove: (screen_name: string) =>
    http.post<{ ok: boolean; error?: string }>('/api/blogger/remove', { screen_name }).then((r) => r.data),
  crawlNow: () =>
    http.post<{ ok: boolean; message?: string }>('/api/blogger/crawl-now').then((r) => r.data),
  saveSettings: (body: Record<string, unknown>) =>
    http.post<{ ok: boolean; error?: string }>('/api/blogger/save-settings', body).then((r) => r.data),
}

// ===== Poster =====

export interface PosterFolder {
  folder: string
  count: number
}

export interface PosterItem {
  stem: string
  video: string
  thumb: string | null
  size: number
  folder: string
  thumbnail_url: string | null
  video_url: string
  duration: string | null
}

export interface PosterResponse {
  ok: boolean
  folder: string | null
  folders: PosterFolder[]
  items: PosterItem[]
}

export interface PosterDurationsResponse {
  ok: boolean
  durations: Record<string, string>
}

export const posterApi = {
  list: (folder?: string) =>
    http
      .get<PosterResponse>('/api/poster', { params: folder ? { folder } : {} })
      .then((r) => r.data),
  durations: (folder?: string) =>
    http
      .get<PosterDurationsResponse>('/api/poster-durations', { params: { folder: folder || '' } })
      .then((r) => r.data),
  delete: (folder: string, stems: string[]) =>
    http.post<{ ok: boolean; deleted: string[] }>('/api/poster/delete', { folder, stems }).then((r) => r.data),
  replaceCover: (folder: string, stem: string, file: File) => {
    const fd = new FormData()
    fd.append('folder', folder)
    fd.append('stem', stem)
    fd.append('file', file)
    return http
      .post<{ ok: boolean; thumb?: string; error?: string }>('/api/poster/replace-cover', fd)
      .then((r) => r.data)
  },
}

// ===== Waterfall =====

export interface WaterfallItem {
  id: string
  url: string
  preview_url: string
  thumbnail: string
  title: string
  pv: number | null
  favorite_count: number | null
  tweet_url: string | null
}

export interface WaterfallResponse {
  ok: boolean
  items: WaterfallItem[]
  config: { per_page: number }
  pagination: { page: number; per_page: number; has_next: boolean }
}

export const waterfallApi = {
  list: (params: { page?: number; range?: string }) =>
    http.get<WaterfallResponse>('/api/waterfall', { params }).then((r) => r.data),
  download: (items: WaterfallItem[]) =>
    http
      .post<{ ok: boolean; success: number; skipped: number; failed: number; error?: string }>(
        '/api/waterfall/download',
        { items }
      )
      .then((r) => r.data),
}
