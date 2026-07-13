<script setup lang="ts">
import { ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { useBloggerStore } from '@/stores/blogger'

const bloggerStore = useBloggerStore()
const addInput = ref('')

function initial(name: string): string {
  return (name || '?').replace(/^@/, '').charAt(0).toUpperCase()
}

async function add() {
  const v = addInput.value.trim().replace(/^@/, '').toLowerCase()
  if (!v) return
  if (!/^[a-zA-Z0-9_]+$/.test(v)) {
    ElMessage.error('用户名只能包含字母、数字和下划线')
    return
  }
  try {
    const r = await bloggerStore.add(v)
    if (r.ok) {
      addInput.value = ''
    }
  } catch {
    // handled by interceptor
  }
}

async function remove(name: string) {
  try {
    await ElMessageBox.confirm(`确定移除 @${name} 吗？`, '提示', { type: 'warning' })
  } catch {
    return
  }
  await bloggerStore.remove(name)
}

async function crawlNow() {
  const r = await bloggerStore.crawlNow()
  if (!r.ok) {
    ElMessage.info(r.message || '启动失败')
  }
}
</script>

<template>
  <el-card>
    <template #header>
      <div style="display: flex; justify-content: space-between; align-items: center;">
        <div style="display: flex; align-items: center; gap: 8px;">
          <span style="font-weight: 600;">博主管理</span>
          <el-badge :value="bloggerStore.bloggers.length" type="primary" />
        </div>
        <div>
          <el-button size="small" type="primary" @click="crawlNow">立即爬取</el-button>
          <el-button size="small" @click="bloggerStore.refresh()">刷新</el-button>
        </div>
      </div>
    </template>

    <div v-if="bloggerStore.bloggers.length === 0" class="muted" style="padding: 12px 0;">
      暂无博主，请在下方添加
    </div>
    <div v-else style="display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px;">
      <div v-for="b in bloggerStore.bloggers" :key="b.screen_name" class="blogger-chip">
        <el-avatar :size="36" :src="b.profile_image_url || undefined">
          {{ initial(b.screen_name) }}
        </el-avatar>
        <div class="info">
          <div class="display-name">{{ b.name && b.name !== b.screen_name ? b.name : '@' + b.screen_name }}</div>
          <div class="screen-name">@{{ b.screen_name }}</div>
          <div v-if="b.description" class="description">{{ b.description }}</div>
          <div v-if="b.file_count > 0" class="file-count">已下载 {{ b.file_count }} 个</div>
        </div>
        <el-button size="small" text type="danger" @click="remove(b.screen_name)">×</el-button>
      </div>
    </div>

    <div style="display: flex; gap: 8px; margin-top: 12px;">
      <el-input
        v-model="addInput"
        placeholder="输入用户名（不含 @），回车添加"
        @keydown.enter="add"
      />
      <el-button type="primary" @click="add">添加</el-button>
    </div>

    <div class="muted" style="margin-top: 8px;">
      爬取 {{ bloggerStore.state.is_running ? '运行中' : '空闲' }} ·
      最近 {{ bloggerStore.state.last_run_time || '暂无' }} ·
      {{ bloggerStore.state.last_result }}
    </div>
  </el-card>
</template>
