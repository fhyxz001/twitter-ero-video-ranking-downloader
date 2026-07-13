import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api, type AppConfig } from '@/api'

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

  async function save(form: Record<string, string | boolean | number>) {
    const fd = new FormData()
    for (const [k, v] of Object.entries(form)) {
      fd.append(k, String(v))
    }
    const r = await api.saveConfig(fd)
    if (r.ok) {
      await load()
    }
    return r
  }

  async function saveQuickDownloadRoot(download_root: string) {
    const fd = new FormData()
    fd.append('download_root', download_root)
    const r = await api.saveQuickConfig(fd)
    if (r.ok && r.config) {
      config.value = r.config
    }
    return r
  }

  return { config, loading, load, save, saveQuickDownloadRoot }
})
