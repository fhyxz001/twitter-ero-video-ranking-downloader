<script setup lang="ts">
import { computed, onMounted, onUnmounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useStatusStore } from '@/stores/status'

const route = useRoute()
const router = useRouter()
const statusStore = useStatusStore()

const activeIndex = computed(() => route.path)

async function handleSelect(index: string) {
  if (index !== route.path) {
    await router.push(index)
  }
}

const isRunning = computed(() => statusStore.runtimeState.is_running)

// 运行状态轮询在全局外壳统一管理，保证任意页面下顶部标签都能实时更新
onMounted(() => {
  statusStore.refresh()
  statusStore.startPolling(5000)
})

onUnmounted(() => {
  statusStore.stopPolling()
})
</script>

<template>
  <el-container :class="{ 'layout-task': route.path === '/' }">
    <el-header style="background: var(--el-bg-color); border-bottom: 1px solid var(--el-border-color); padding: 0 24px; display: flex; align-items: center;">
      <div style="font-size: 18px; font-weight: 700; margin-right: 32px;">视频下载器</div>
      <el-menu
        :default-active="activeIndex"
        mode="horizontal"
        :ellipsis="false"
        @select="handleSelect"
        style="border-bottom: none; flex: 1;"
      >
        <el-menu-item index="/">任务</el-menu-item>
        <el-menu-item index="/poster">本地视频</el-menu-item>
        <el-menu-item index="/waterfall">远程视频</el-menu-item>
      </el-menu>
      <el-tag :type="isRunning ? 'success' : 'info'" effect="plain">
        <span style="display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: currentColor; margin-right: 4px; vertical-align: middle;" />
        {{ isRunning ? '运行中' : '空闲' }}
      </el-tag>
    </el-header>
    <el-main :class="{ 'task-main': route.path === '/' }">
      <div class="app-container" :class="{ 'task-container': route.path === '/' }">
        <slot />
      </div>
    </el-main>
  </el-container>
</template>
