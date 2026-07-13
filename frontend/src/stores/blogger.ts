import { defineStore } from 'pinia'
import { ref } from 'vue'
import { bloggerApi, type Blogger, type BloggerState } from '@/api'

export const useBloggerStore = defineStore('blogger', () => {
  const bloggers = ref<Blogger[]>([])
  const state = ref<BloggerState>({ is_running: false, last_run_time: null, last_result: '尚未执行' })
  const settings = ref<{
    twitter_blogger_enabled: boolean
    twitter_blogger_cron: string
    twitter_blogger_max_media: number
    twitter_blogger_has_retweet: boolean
    twitter_cookie_set: boolean
  }>({
    twitter_blogger_enabled: true,
    twitter_blogger_cron: '0 4 * * *',
    twitter_blogger_max_media: -1,
    twitter_blogger_has_retweet: false,
    twitter_cookie_set: false,
  })
  let timer: number | null = null

  async function refresh() {
    try {
      const data = await bloggerApi.list()
      bloggers.value = data.bloggers || []
      state.value = data.state
      settings.value = data.settings
      return data
    } catch {
      return null
    }
  }

  async function add(screen_name: string) {
    const r = await bloggerApi.add(screen_name)
    if (r.ok) await refresh()
    return r
  }

  async function remove(screen_name: string) {
    const r = await bloggerApi.remove(screen_name)
    if (r.ok) await refresh()
    return r
  }

  async function crawlNow() {
    const r = await bloggerApi.crawlNow()
    await refresh()
    return r
  }

  async function saveSettings(body: Record<string, unknown>) {
    const r = await bloggerApi.saveSettings(body)
    if (r.ok) await refresh()
    return r
  }

  function startPolling(intervalMs = 10000) {
    stopPolling()
    timer = window.setInterval(refresh, intervalMs)
  }

  function stopPolling() {
    if (timer !== null) {
      window.clearInterval(timer)
      timer = null
    }
  }

  return { bloggers, state, settings, refresh, add, remove, crawlNow, saveSettings, startPolling, stopPolling }
})
