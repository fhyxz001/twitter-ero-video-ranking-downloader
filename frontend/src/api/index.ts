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
}

export interface SaveConfigPayload {
  download_root: string
  poster_root: string
  proxy: string
  auto_download_enabled: boolean
  schedule_cron: string
  max_daily_downloads: number
  ranking_range: string
}

export interface RuntimeState {
  is_running: boolean
  last_run_time: string | null
  last_result: string
}

export interface StatusResponse {
  ok: boolean
  state: RuntimeState
  logs: string[]
  config: AppConfig
}

export interface ProbeResult {
  ok: boolean
  status: number | null
  elapsed_ms: number | null
  items: number | null
  error: string | null
}

export interface CheckProxyResponse {
  ok: boolean
  proxy: ProbeResult | null
  direct: ProbeResult
}

export const api = {
  getStatus: () => http.get<StatusResponse>('/status').then((r) => r.data),
  runNow: () => http.post<{ ok: boolean; message?: string }>('/run-now').then((r) => r.data),
  saveConfig: (payload: SaveConfigPayload) =>
    http.post<{ ok: boolean; error?: string }>('/save', payload).then((r) => r.data),
  saveQuickConfig: (payload: { download_root: string }) =>
    http.post<{ ok: boolean; config?: AppConfig; error?: string }>('/save-quick', payload).then((r) => r.data),
  checkDir: (path: string) =>
    http.get<{ ok: boolean; error?: string }>('/api/check-dir', { params: { path } }).then((r) => r.data),
  checkProxy: (proxy: string) =>
    http.post<CheckProxyResponse>('/api/check-proxy', { proxy }).then((r) => r.data),
  saveWaterfallSettings: (per_page: number) =>
    http
      .post<{ ok: boolean; config?: { per_page: number }; error?: string }>('/api/waterfall/settings', {
        per_page,
      })
      .then((r) => r.data),
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

export type PosterSort =
  | 'time_desc'
  | 'time_asc'
  | 'size_desc'
  | 'size_asc'
  | 'duration_desc'
  | 'duration_asc'

export interface PosterPagination {
  page: number
  page_size: number
  total: number
  has_next: boolean
}

export interface PosterResponse {
  ok: boolean
  folder: string | null
  folders: PosterFolder[]
  items: PosterItem[]
  pagination: PosterPagination
}

export interface PosterDurationsResponse {
  ok: boolean
  durations: Record<string, string>
}

export const posterApi = {
  list: (params: { folder?: string; page?: number; page_size?: number; sort?: PosterSort } = {}) =>
    http
      .get<PosterResponse>('/api/poster', {
        params: {
          folder: params.folder || undefined,
          page: params.page ?? 1,
          page_size: params.page_size ?? 20,
          sort: params.sort ?? 'time_desc',
        },
      })
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
