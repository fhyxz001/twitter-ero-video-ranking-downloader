import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api, type AppConfig, type SaveConfigPayload } from '@/api'

export const useConfigStore = defineStore('config', () => {
  const config = ref<AppConfig | null>(null)
  const loading = ref(false)

  async function load() {
    loading.value = true
    try {
      const data = await api.getStatus()
      config.value = data.config
      return data
    } finally {
      loading.value = false
    }
  }

  async function save(payload: SaveConfigPayload) {
    const r = await api.saveConfig(payload)
    if (r.ok) {
      await load()
    }
    return r
  }

  async function saveQuickDownloadRoot(download_root: string) {
    const r = await api.saveQuickConfig({ download_root })
    if (r.ok && r.config) {
      config.value = r.config
    }
    return r
  }

  return { config, loading, load, save, saveQuickDownloadRoot }
})
