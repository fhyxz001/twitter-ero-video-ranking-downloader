<script setup lang="ts">
import { ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useConfigStore } from '@/stores/config'
import { useStatusStore } from '@/stores/status'
import { api } from '@/api'

const props = defineProps<{ modelValue: boolean }>()
const emit = defineEmits<{ (e: 'update:modelValue', value: boolean): void }>()

const configStore = useConfigStore()
const statusStore = useStatusStore()

const form = ref({
  proxy: '',
  auto_download_enabled: true,
  schedule_cron: '0 3 * * *',
  ranking_range: 'daily',
})
const saving = ref(false)

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

function populateForm() {
  const c = configStore.config
  if (!c) return
  form.value = {
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
    const payload: Record<string, string | boolean | number> = {
      download_root: c.download_root,
      proxy: form.value.proxy,
      auto_download_enabled: form.value.auto_download_enabled ? '1' : '0',
      schedule_cron: form.value.schedule_cron,
      max_daily_downloads: c.max_daily_downloads,
      ranking_range: form.value.ranking_range,
    }
    await configStore.save(payload)
    await statusStore.refresh()
    ElMessage.success('设置已保存')
    close()
  } catch (e) {
    // axios interceptor already shows error
  } finally {
    saving.value = false
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
</style>
