<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { useStatusStore } from '@/stores/status'

const statusStore = useStatusStore()
const collapsed = ref(false)
const logEl = ref<HTMLPreElement | null>(null)

const logsText = computed(() => (statusStore.logs || []).join('\n'))
const isRunning = computed(() => statusStore.runtimeState.is_running)

watch(logsText, async () => {
  await nextTick()
  if (logEl.value) {
    logEl.value.scrollTop = logEl.value.scrollHeight
  }
})

async function copyLogs() {
  try {
    await navigator.clipboard.writeText(logsText.value)
    ElMessage.success('已复制日志')
  } catch {
    const ta = document.createElement('textarea')
    ta.value = logsText.value
    document.body.appendChild(ta)
    ta.select()
    document.execCommand('copy')
    document.body.removeChild(ta)
    ElMessage.success('已复制日志')
  }
}
</script>

<template>
  <el-card>
    <template #header>
      <div
        style="display: flex; justify-content: space-between; align-items: center; cursor: pointer;"
        @click="collapsed = !collapsed"
      >
        <div style="display: flex; align-items: center; gap: 8px;">
          <span style="font-weight: 600;">运行日志</span>
          <span
            :style="{
              display: 'inline-block',
              width: '8px',
              height: '8px',
              borderRadius: '50%',
              background: isRunning ? 'var(--el-color-success)' : 'var(--el-text-color-disabled)',
            }"
          />
        </div>
        <div @click.stop>
          <el-button size="small" @click="copyLogs">复制</el-button>
          <el-button size="small" @click="collapsed = !collapsed">
            {{ collapsed ? '展开' : '收起' }}
          </el-button>
        </div>
      </div>
    </template>
    <div v-show="!collapsed" class="log-panel">
      <pre ref="logEl">{{ logsText }}</pre>
    </div>
  </el-card>
</template>
