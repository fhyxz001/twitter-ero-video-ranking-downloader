<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useConfigStore } from '@/stores/config'
import { useStatusStore } from '@/stores/status'
import { api, type CheckProxyResponse } from '@/api'
import LogPanel from '@/components/LogPanel.vue'

const configStore = useConfigStore()
const statusStore = useStatusStore()

const downloadRoot = ref('')
const downloadRootDirty = ref(false)
const maxDailyDownloads = ref(10)
const posterRoot = ref('')
const proxy = ref('')
const autoDownloadEnabled = ref(true)
const scheduleCron = ref('0 3 * * *')
const rankingRange = ref('daily')
const openApiKey = ref('')

const dirValid = ref<boolean | null>(null)
const dirError = ref('')
const saving = ref(false)
const checking = ref(false)
const checkResult = ref<CheckProxyResponse | null>(null)
let quickSaveTimer: number | null = null

const isRunning = computed(() => statusStore.runtimeState.is_running)

const dirStatusText = computed(() => {
  if (dirValid.value === null) return '检测中…'
  return dirValid.value ? '有效' : '无效'
})

const dirStatusClass = computed(() => {
  if (dirValid.value === null) return ''
  return dirValid.value ? 'is-ok' : 'is-fail'
})

const proxyStatusText = computed(() => {
  const result = checkResult.value
  if (!result) return ''
  return result.proxy.ok ? `正常 ${result.proxy.elapsed_ms ?? 0}ms` : '失败'
})

const proxyStatusTitle = computed(() => {
  const result = checkResult.value
  if (!result) return ''
  return result.proxy.ok ? `HTTP ${result.proxy.status ?? 200}` : result.proxy.error || '未知错误'
})

function populateForm() {
  const config = configStore.config
  if (!config) return
  downloadRoot.value = config.download_root || ''
  maxDailyDownloads.value = config.max_daily_downloads || 10
  posterRoot.value = config.poster_root || ''
  proxy.value = config.proxy || ''
  autoDownloadEnabled.value = config.auto_download_enabled !== false
  scheduleCron.value = config.schedule_cron || '0 3 * * *'
  rankingRange.value = config.ranking_range || 'daily'
  openApiKey.value = config.open_api_key || ''
}

async function validateDir() {
  const dir = downloadRoot.value.trim()
  if (!dir) {
    dirValid.value = false
    dirError.value = '目录路径不能为空'
    return
  }
  try {
    const result = await api.checkDir(dir)
    dirValid.value = result.ok
    dirError.value = result.ok ? '' : result.error || '目录无效'
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
    const snapshot = downloadRoot.value
    const result = await configStore.saveQuickDownloadRoot(snapshot.trim())
    if (result.ok && downloadRoot.value === snapshot) {
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
  const result = await configStore.saveQuickDownloadRoot(downloadRoot.value.trim())
  if (result.ok) {
    downloadRootDirty.value = false
    return true
  }
  return false
}

async function saveAll() {
  const count = Number(maxDailyDownloads.value)
  if (!Number.isInteger(count) || count < 1) {
    ElMessage.error('每次下载数量上限必须大于等于 1')
    return
  }
  const config = configStore.config
  if (!config) {
    ElMessage.warning('配置尚未加载，请稍后重试')
    return
  }

  saving.value = true
  try {
    await configStore.save({
      download_root: downloadRoot.value.trim(),
      poster_root: posterRoot.value.trim(),
      proxy: proxy.value.trim(),
      auto_download_enabled: autoDownloadEnabled.value,
      schedule_cron: scheduleCron.value.trim() || '0 3 * * *',
      max_daily_downloads: count,
      ranking_range: rankingRange.value || 'daily',
      open_api_key: openApiKey.value.trim(),
    })
    if (quickSaveTimer !== null) {
      window.clearTimeout(quickSaveTimer)
      quickSaveTimer = null
    }
    downloadRootDirty.value = false
    await statusStore.refresh()
    ElMessage.success('设置已保存')
  } catch {
    // axios interceptor already shows error
  } finally {
    saving.value = false
  }
}

async function runNow() {
  const ok = await flushQuickSave()
  if (!ok) {
    ElMessage.error('配置自动保存失败，请检查后重试')
    return
  }
  try {
    const result = await api.runNow()
    if (!result.ok) {
      ElMessage.error(result.message || '启动失败')
    } else if (result.message) {
      ElMessage.info(result.message)
    }
    await statusStore.refresh()
  } catch {
    // axios interceptor already shows error
  }
}

async function checkProxy() {
  checking.value = true
  checkResult.value = null
  try {
    checkResult.value = await api.checkProxy(proxy.value.trim())
  } catch {
    // axios interceptor already shows error
  } finally {
    checking.value = false
  }
}

watch(proxy, () => {
  checkResult.value = null
})

onMounted(async () => {
  await configStore.load()
  populateForm()
  validateDir()
})

onBeforeUnmount(() => {
  if (quickSaveTimer !== null) {
    window.clearTimeout(quickSaveTimer)
  }
})
</script>

<template>
  <div class="task-page">
    <div class="task-header">
      <h1 class="page-title">任务</h1>
      <div class="header-actions">
        <el-button :loading="saving" @click="saveAll">保存设置</el-button>
        <el-button type="primary" :loading="isRunning" @click="runNow">
          <template v-if="!isRunning">▶</template>
          立即执行
        </el-button>
      </div>
    </div>

    <el-card class="settings-card" shadow="never">
      <template #header>
        <div class="card-header">
          <span class="card-title">任务设置</span>
          <span class="muted">修改后点击右上角“保存设置”生效</span>
        </div>
      </template>

      <div class="settings-grid">
        <div class="field field-span-2">
          <div class="field-label">
            <span>视频下载根目录</span>
            <span class="field-status" :class="dirStatusClass" :title="dirError">{{ dirStatusText }}</span>
          </div>
          <el-input
            v-model="downloadRoot"
            placeholder="如 D:\Downloads 或 /data/downloads"
            @input="onDownloadRootInput"
          />
        </div>

        <div class="field">
          <div class="field-label">每次下载数量上限</div>
          <el-input-number
            v-model="maxDailyDownloads"
            :min="1"
            :step="1"
            controls-position="right"
            style="width: 100%"
          />
        </div>

        <div class="field field-span-2">
          <div class="field-label">HTTP 代理</div>
          <div class="inline-control">
            <el-input v-model="proxy" placeholder="http://127.0.0.1:7890（留空直连）" />
            <el-button :loading="checking" @click="checkProxy">检测连接</el-button>
            <span
              v-if="checkResult"
              class="probe-status"
              :class="checkResult.proxy.ok ? 'is-ok' : 'is-fail'"
              :title="proxyStatusTitle"
            >
              {{ proxyStatusText }}
            </span>
          </div>
        </div>

        <div class="field">
          <div class="field-label">排行榜范围</div>
          <el-select v-model="rankingRange" style="width: 100%">
            <el-option label="日榜" value="daily" />
            <el-option label="周榜" value="weekly" />
            <el-option label="月榜" value="monthly" />
            <el-option label="总榜" value="all" />
          </el-select>
        </div>

        <div class="field field-span-2">
          <div class="field-label">
            <span>媒体根目录</span>
            <span
              class="field-hint"
              title="本地视频从该目录扫描视频与 .strm 引用；留空时使用视频下载根目录。同名图片文件将作为对应条目的封面。"
            >?</span>
          </div>
          <el-input v-model="posterRoot" placeholder="留空则使用视频下载根目录" />
        </div>

        <div class="field">
          <div class="field-label">
            <span>定时下载</span>
            <span class="field-hint" title="标准 5 位 cron，如 0 3 * * * 表示每天 3:00">?</span>
          </div>
          <div class="inline-control">
            <el-switch v-model="autoDownloadEnabled" />
            <el-input
              v-model="scheduleCron"
              class="cron-input"
              placeholder="0 3 * * *"
              :disabled="!autoDownloadEnabled"
            />
          </div>
        </div>
        <div class="field field-span-3">
          <div class="field-label">
            <span>开放下载 Key</span>
            <span
              class="field-hint"
              title="外部调用 POST /api/open/download 时需携带该 key；留空则关闭该接口。"
            >?</span>
          </div>
          <el-input v-model="openApiKey" placeholder="留空则关闭对外开放下载接口" />
        </div>
      </div>
    </el-card>

    <div class="log-area">
      <LogPanel />
    </div>
  </div>
</template>

<style scoped>
.task-page {
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow: hidden;
}

.task-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-shrink: 0;
}

.task-header .page-title {
  margin: 0;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.settings-card {
  flex-shrink: 0;
}

.settings-card :deep(.el-card__header) {
  padding: 10px 16px;
}

.settings-card :deep(.el-card__body) {
  padding: 14px 16px;
}

.card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.card-title {
  font-size: 14px;
  font-weight: 600;
}

.settings-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px 16px;
}

.field {
  min-width: 0;
}

.field-span-2 {
  grid-column: span 2;
}

.field-span-3 {
  grid-column: span 3;
}

.field-label {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 5px;
  font-size: 12px;
  line-height: 1.4;
  color: var(--el-text-color-secondary);
}

.field-status {
  font-size: 11px;
}

.field-status.is-ok {
  color: var(--el-color-success);
}

.field-status.is-fail {
  color: var(--el-color-danger);
}

.field-hint {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 14px;
  height: 14px;
  border: 1px solid var(--el-border-color);
  border-radius: 50%;
  font-size: 11px;
  line-height: 1;
  cursor: help;
}

.inline-control {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.inline-control .el-input {
  flex: 1;
  min-width: 0;
}

.cron-input {
  flex: 1;
}

.probe-status {
  flex-shrink: 0;
  font-size: 12px;
  white-space: nowrap;
}

.probe-status.is-ok {
  color: var(--el-color-success);
}

.probe-status.is-fail {
  color: var(--el-color-danger);
}

.log-area {
  flex: 1;
  min-height: 0;
  display: flex;
}

.log-area > * {
  width: 100%;
}

@media (max-width: 900px) {
  .task-page {
    height: auto;
    overflow: visible;
  }

  .settings-grid {
    grid-template-columns: 1fr;
  }

  .field-span-2 {
    grid-column: span 1;
  }

  .card-header .muted {
    display: none;
  }

  .log-area {
    flex: none;
    height: 360px;
  }
}
</style>
