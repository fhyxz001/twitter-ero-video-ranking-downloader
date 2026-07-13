<script setup lang="ts">
import { computed } from 'vue'
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
</script>

<template>
  <el-container>
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
        <el-menu-item index="/poster">海报墙</el-menu-item>
        <el-menu-item index="/waterfall">瀑布流</el-menu-item>
      </el-menu>
      <el-tag :type="isRunning ? 'success' : 'info'" effect="plain" style="margin-right: 12px;">
        <span style="display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: currentColor; margin-right: 4px; vertical-align: middle;" />
        {{ isRunning ? '运行中' : '空闲' }}
      </el-tag>
    </el-header>
    <el-main>
      <div class="app-container">
        <slot />
      </div>
    </el-main>
  </el-container>
</template>
