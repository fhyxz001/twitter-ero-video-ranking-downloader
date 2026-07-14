import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api, type AppConfig, type RuntimeState } from '@/api'

export const useStatusStore = defineStore('status', () => {
  const runtimeState = ref<RuntimeState>({ is_running: false, last_run_time: null, last_result: '尚未执行' })
  const logs = ref<string[]>([])
  const configPermalink = ref<AppConfig | null>(null)
  let timer: number | null = null

  async function refresh() {
    try {
      const data = await api.getStatus()
      runtimeState.value = data.state
      logs.value = data.logs || []
      if (data.config) {
        configPermalink.value = data.config
      }
      return data
    } catch {
      return null
    }
  }

  function startPolling(intervalMs = 5000) {
    stopPolling()
    timer = window.setInterval(refresh, intervalMs)
  }

  function stopPolling() {
    if (timer !== null) {
      window.clearInterval(timer)
      timer = null
    }
  }

  return { runtimeState, logs, configPermalink, refresh, startPolling, stopPolling }
})
