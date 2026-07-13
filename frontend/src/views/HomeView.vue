<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { useConfigStore } from '@/stores/config'
import { useStatusStore } from '@/stores/status'
import { api } from '@/api'
import SettingsDrawer from '@/components/SettingsDrawer.vue'
import BloggerList from '@/components/BloggerList.vue'
import LogPanel from '@/components/LogPanel.vue'

const configStore = useConfigStore()
const statusStore = useStatusStore()

const settingsOpen = ref(false)
const downloadRoot = ref('')
const downloadRootDirty = ref(false)
const maxDailyDownloads = ref(10)
const dirValid = ref<boolean | null>(null)
const dirError = ref('')
let quickSaveTimer: number | null = null

const bloggerCount = computed(() => configStore.config?.twitter_blogger_list?.length || 0)
const autoEnabled = computed(() => configStore.config?.auto_download_enabled !== false)
const isRunning = computed(() => statusStore.runtimeState.is_running)
const nextRun = computed(() =>
  autoEnabled.value ? configStore.config?.schedule_cron || '未知' : '未开启'
)

async function validateDir() {
  const dir = downloadRoot.value.trim()
  if (!dir) {
    dirValid.value = false
    dirError.value = '目录路径不能为空'
    return
  }
  try {
    const r = await api.checkDir(dir)
    dirValid.value = r.ok
    dirError.value = r.ok ? '' : r.error || '目录无效'
  } catch {
    dirValid.value = false
    dirError.value = '无法验证'
  }
}

function onDownloadRootInput() {
  downloadRootDirty.value = true
  scheduleQuickSave(500)
  validateDir()
}

function scheduleQuickSave(delay = 0) {
  if (quickSaveTimer !== null) {
    window.clearTimeout(quickSaveTimer)
  }
  quickSaveTimer = window.setTimeout(async () => {
    if (!downloadRootDirty.value) return
    const snap = downloadRoot.value
    const r = await configStore.saveQuickDownloadRoot(snap)
    if (r.ok && downloadRoot.value === snap) {
      downloadRootDirty.value = false
    }
  }, delay)
}

async function flushQuickSave() {
  if (quickSaveTimer !== null) {
    window.clearTimeout(quickSaveTimer)
    quickSaveTimer = null
  }
  if (!downloadRootDirty.value) return true
  const r = await configStore.saveQuickDownloadRoot(downloadRoot.value)
  if (r.ok) {
    downloadRootDirty.value = false
    return true
  }
  return false
}

async function saveMaxDaily() {
  const v = Number(maxDailyDownloads.value)
  if (!Number.isInteger(v) || v < 1) {
    ElMessage.error('每次下载数量上限必须大于等于 1')
    return
  }
  const c = configStore.config!
  const payload: Record<string, string | boolean | number> = {
    download_root: downloadRoot.value,
    proxy: c.proxy || '',
    auto_download_enabled: c.auto_download_enabled ? '1' : '0',
    schedule_cron: c.schedule_cron || '0 3 * * *',
    max_daily_downloads: String(v),
    ranking_range: c.ranking_range || 'daily',
    twitter_cookie: c.twitter_cookie || '',
    twitter_blogger_enabled: c.twitter_blogger_enabled ? '1' : '0',
    twitter_blogger_cron: c.twitter_blogger_cron || '0 4 * * *',
    twitter_blogger_max_media: String(c.twitter_blogger_max_media ?? -1),
    twitter_blogger_has_retweet: c.twitter_blogger_has_retweet ? '1' : '0',
  }
  try {
    await configStore.save(payload)
    await statusStore.refresh()
    ElMessage.success('保存成功')
  } catch {
    // handled by interceptor
  }
}

async function runNow() {
  const ok = await flushQuickSave()
  if (!ok) {
    ElMessage.error('配置自动保存失败，请检查后重试')
    return
  }
  try {
    const r = await api.runNow()
    if (!r.ok) {
      ElMessage.error(r.message || '启动失败')
    } else if (r.message) {
      ElMessage.info(r.message)
    }
    await statusStore.refresh()
  } catch {
    // handled
  }
}

function browseDirectory() {
  const i = document.createElement('input')
  i.type = 'file'
  i.webkitdirectory = true
  ;(i as any).directory = true
  i.onchange = (e: Event) => {
    const target = e.target as HTMLInputElement
    const files = target.files
    if (files && files.length > 0) {
      const f = files[0] as File & { path?: string }
      if (f.path) {
        downloadRoot.value = f.path.replace(/[\\/][^\\/]+$/, '')
      } else if (f.webkitRelativePath) {
        downloadRoot.value = f.webkitRelativePath.split('/')[0]
      }
      onDownloadRootInput()
    }
  }
  i.click()
}

onMounted(async () => {
  await statusStore.refresh()
  if (configStore.config) {
    downloadRoot.value = configStore.config.download_root || ''
    maxDailyDownloads.value = configStore.config.max_daily_downloads || 10
  }
  validateDir()
  statusStore.startPolling(5000)
})

onUnmounted(() => {
  statusStore.stopPolling()
})
</script>

<template>
  <div>
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
      <h1 class="page-title" style="margin: 0;">任务</h1>
      <div>
        <el-button @click="settingsOpen = true">设置</el-button>
        <el-button type="primary" :loading="isRunning" @click="runNow">
          <template v-if="!isRunning">▶</template>
          立即执行
        </el-button>
      </div>
    </div>

    <!-- 状态条 -->
    <div class="status-strip">
      <div class="item">
        <div class="label">定时服务</div>
        <div class="value">{{ autoEnabled ? '已开启' : '已关闭' }}</div>
      </div>
      <div class="item">
        <div class="label">运行状态</div>
        <div class="value" :style="{ color: isRunning ? 'var(--el-color-success)' : 'inherit' }">
          {{ isRunning ? '运行中' : '空闲' }}
        </div>
      </div>
      <div class="item">
        <div class="label">博主</div>
        <div class="value">{{ bloggerCount }}</div>
      </div>
      <div class="item">
        <div class="label">上次执行</div>
        <div class="value">{{ statusStore.runtimeState.last_run_time || '暂无' }}</div>
      </div>
      <div class="item">
        <div class="label">下次计划</div>
        <div class="value muted">{{ nextRun }}</div>
      </div>
      <div class="item">
        <div class="label">下载目录</div>
        <div class="value" :style="{ color: dirValid === false ? 'var(--el-color-danger)' : dirValid ? 'var(--el-color-success)' : 'inherit' }">
          {{ dirValid === null ? '检测中…' : dirValid ? '有效' : '无效' }}
        </div>
        <div class="muted" style="margin-top: 2px; font-size: 11px;">{{ downloadRoot }}</div>
      </div>
    </div>

    <!-- 博主管理 + 下载设置 -->
    <el-row :gutter="16">
      <el-col :xs="24" :md="14">
        <BloggerList />
      </el-col>
      <el-col :xs="24" :md="10">
        <el-card>
          <template #header>
            <span style="font-weight: 600;">下载设置</span>
          </template>
          <el-form label-width="0">
            <el-form-item>
              <template #label>
                <span style="font-size: 13px;">视频下载根目录</span>
              </template>
              <div style="display: flex; gap: 8px; width: 100%;">
                <el-input v-model="downloadRoot" @input="onDownloadRootInput" />
                <el-button @click="browseDirectory">浏览</el-button>
              </div>
              <div v-if="dirError" class="muted" style="color: var(--el-color-danger); margin-top: 4px;">
                {{ dirError }}
              </div>
            </el-form-item>
            <el-form-item>
              <template #label>
                <span style="font-size: 13px;">每次下载数量上限</span>
              </template>
              <div style="display: flex; gap: 8px; width: 100%;">
                <el-input-number v-model="maxDailyDownloads" :min="1" />
                <el-button type="primary" @click="saveMaxDaily">保存</el-button>
              </div>
            </el-form-item>
          </el-form>
        </el-card>
      </el-col>
    </el-row>

    <!-- 日志面板 -->
    <div style="margin-top: 16px;">
      <LogPanel />
    </div>

    <SettingsDrawer v-model="settingsOpen" />
  </div>
</template>
