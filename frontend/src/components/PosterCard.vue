<script setup lang="ts">
import type { PosterItem } from '@/api'

const props = defineProps<{
  item: PosterItem
  selected: boolean
  durationLabel: string
}>()

const emit = defineEmits<{
  (e: 'toggle-select'): void
  (e: 'preview'): void
  (e: 'replace-cover'): void
  (e: 'delete'): void
}>()

function fmtSize(b: number): string {
  if (b >= 1073741824) return (b / 1073741824).toFixed(2) + ' GB'
  if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB'
  if (b >= 1024) return (b / 1024).toFixed(0) + ' KB'
  return b + ' B'
}
</script>

<template>
  <article class="poster-card" :class="{ selected }">
    <div class="poster-card-cover" @click="emit('preview')">
      <div class="checkbox" @click.stop>
        <el-checkbox
          :model-value="selected"
          @update:model-value="emit('toggle-select')"
        />
      </div>
      <img v-if="item.thumbnail_url" :src="item.thumbnail_url" :alt="item.video" loading="lazy" />
      <div v-else class="placeholder">MV</div>
      <div class="play-overlay">▶</div>
      <div v-if="durationLabel" class="duration">{{ durationLabel }}</div>
    </div>
    <div class="poster-card-body">
      <div class="poster-card-title" :title="item.video">{{ item.video }}</div>
      <div class="poster-card-meta">
        <el-tag size="small" type="info">{{ fmtSize(item.size) }}</el-tag>
        <el-tag size="small">{{ item.folder || '根目录' }}</el-tag>
        <el-tag v-if="durationLabel" size="small" type="success">时长 {{ durationLabel }}</el-tag>
      </div>
      <div style="display: flex; gap: 4px;">
        <el-button size="small" @click="emit('replace-cover')">替换封面</el-button>
        <el-button size="small" type="danger" @click="emit('delete')">删除</el-button>
      </div>
    </div>
  </article>
</template>
