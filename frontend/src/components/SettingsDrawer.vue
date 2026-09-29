<script setup lang="ts">
import { ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useConfigStore } from '@/stores/config'
import { useStatusStore } from '@/stores/status'
import { api, type CheckProxyResponse } from '@/api'

const props = defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{ (e: 'update:modelValue', value: boolean): void }>()

const configStore = useConfigStore()
const statusStore = useStatusStore()

const form = ref({
  poster_root: '',
  proxy: '',
  auto_download_enabled: true,
  schedule_cron: '0 3 * * *',
  ranking_range: 'daily',
})
const saving = ref(false)
const checking = ref(false)
const checkResult = ref<CheckProxyResponse | null>(null)

watch(
  () => props.modelValue,
  (open) => {
    if (open) {
      populateForm()
    }
  }
)

watch(
  () => configStore.config,
  (c) => {
    if (c && props.modelValue) {
      populateForm()
    }
  }
)

watch(
  () => form.value.proxy,
  () => {
    // 代理地址改动后，旧检测结果不再可信
    checkResult.value = null
  }
)

function populateForm() {
  const c = configStore.config
  if (!c) return
  form.value = {
    poster_root: c.poster_root || '',
    proxy: c.proxy || '',
    auto_download_enabled: c.auto_download_enabled !== false,
    schedule_cron: c.schedule_cron || '0 3 * * *',
    ranking_range: c.ranking_range || 'daily',
  }
}

function close() {
  emit('update:modelValue', false)
}

async function save() {
  saving.value = true
  try {
    const c = configStore.config
    if (!c) {
      ElMessage.warning('配置尚未加载，请稍后重试')
      return
    }
    await configStore.save({
      download_root: c.download_root,
      poster_root: form.value.poster_root.trim(),
      proxy: form.value.proxy,
      auto_download_enabled: form.value.auto_download_enabled,
      schedule_cron: form.value.schedule_cron,
      max_daily_downloads: c.max_daily_downloads,
      ranking_range: form.value.ranking_range,
    })
    await statusStore.refresh()
    ElMessage.success('设置已保存')
    close()
  } catch (e) {
    // axios interceptor already shows error
  } finally {
    saving.value = false
  }
}

function describeProbe(label: string, ok: boolean, status: number | null, elapsedMs: number | null, error: string | null): string {
  if (ok) {
    return `${label}：正常（HTTP ${status ?? 200}，${elapsedMs ?? 0}ms）`
  }
  return `${label}：失败 —— ${error || '未知错误'}`
}

async function checkProxy() {
  checking.value = true
  checkResult.value = null
  try {
    checkResult.value = await api.checkProxy(form.value.proxy.trim())
  } catch (e) {
    // axios interceptor already shows error
  } finally {
    checking.value = false
  }
}
</script>

<template>
  <el-drawer
    :model-value="modelValue"
    title="设置"
    direction="rtl"
    size="480px"
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <el-form label-width="140px" label-position="left">
      <div class="section-title">下载设置</div>
      <el-form-item label="开启自动下载">
        <el-switch v-model="form.auto_download_enabled" />
        <div class="muted" style="margin-top: 4px;">关闭后"立即执行"仍可手动触发。</div>
      </el-form-item>
      <el-form-item label="HTTP 代理">
        <el-input v-model="form.proxy" placeholder="http://127.0.0.1:7890" />
        <div style="margin-top: 6px;">
          <el-button size="small" :loading="checking" @click="checkProxy">检测连接</el-button>
          <span class="muted" style="margin-left: 8px;">对数据源 pektino.com 分别经代理与直连探测</span>
        </div>
        <div v-if="checkResult" class="check-result">
          <div v-if="checkResult.proxy" :class="checkResult.proxy.ok ? 'is-ok' : 'is-fail'">
            {{ describeProbe('代理连接', checkResult.proxy.ok, checkResult.proxy.status, checkResult.proxy.elapsed_ms, checkResult.proxy.error) }}
          </div>
          <div v-else class="muted">未填写代理，已跳过代理测试</div>
          <div :class="checkResult.direct.ok ? 'is-ok' : 'is-fail'">
            {{ describeProbe('直连对照', checkResult.direct.ok, checkResult.direct.status, checkResult.direct.elapsed_ms, checkResult.direct.error) }}
          </div>
          <div v-if="checkResult.proxy && !checkResult.proxy.ok && checkResult.direct.ok" class="muted">
            直连可用而代理失败：代理地址不可达或其分流规则未放行该站点；反之若两者都失败，需更换可用代理。
          </div>
        </div>
      </el-form-item>
      <el-form-item label="定时 Cron">
        <el-input v-model="form.schedule_cron" placeholder="0 3 * * *" />
        <div class="muted" style="margin-top: 4px;">标准 5 位 cron，如 0 3 * * * 每天 3:00</div>
      </el-form-item>
      <el-form-item label="排行榜范围">
        <el-select v-model="form.ranking_range">
          <el-option label="日榜" value="daily" />
          <el-option label="周榜" value="weekly" />
          <el-option label="月榜" value="monthly" />
          <el-option label="总榜" value="all" />
        </el-select>
      </el-form-item>

      <div class="section-title">海报墙</div>
      <el-form-item label="媒体根目录">
        <el-input v-model="form.poster_root" placeholder="如 D:\Media 或 /data/media" />
        <div class="muted" style="margin-top: 4px;">
          海报墙从该目录扫描视频与 .strm 引用；留空时使用"视频下载根目录"。
          同名图片文件（如 Movie.jpg）将作为对应条目的封面。
        </div>
      </el-form-item>
    </el-form>

    <template #footer>
      <div style="text-align: right;">
        <el-button @click="close">取消</el-button>
        <el-button type="primary" :loading="saving" @click="save">保存</el-button>
      </div>
    </template>
  </el-drawer>
</template>

<style scoped>
.section-title {
  font-size: 13px;
  font-weight: 600;
  color: var(--el-text-color-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin: 16px 0 12px;
  padding-bottom: 6px;
  border-bottom: 1px solid var(--el-border-color-lighter);
}
.section-title:first-child {
  margin-top: 0;
}
.muted {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.check-result {
  margin-top: 8px;
  font-size: 12px;
  line-height: 1.7;
  word-break: break-all;
  padding: 8px 10px;
  border-radius: 4px;
  background: var(--el-fill-color-light);
}
.check-result .is-ok {
  color: var(--el-color-success);
}
.check-result .is-fail {
  color: var(--el-color-danger);
}
</style>
