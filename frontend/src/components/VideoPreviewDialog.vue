<script setup lang="ts">
import { ref, watch, nextTick } from 'vue'

const props = defineProps<{
  modelValue: boolean
  title: string
  meta?: string
  videoUrl: string
  posterUrl?: string
}>()
const emit = defineEmits<{ (e: 'update:modelValue', v: boolean): void }>()

const videoEl = ref<HTMLVideoElement | null>(null)

watch(
  () => props.modelValue,
  async (open) => {
    if (open) {
      await nextTick()
      if (videoEl.value) {
        videoEl.value.play().catch(() => {})
      }
    } else {
      if (videoEl.value) {
        videoEl.value.pause()
        videoEl.value.src = ''
        videoEl.value.poster = ''
      }
    }
  }
)

function close() {
  emit('update:modelValue', false)
}
</script>

<template>
  <el-dialog
    :model-value="modelValue"
    :title="title"
    width="min(860px, 95vw)"
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
    @close="close"
  >
    <div v-if="meta" class="muted" style="margin-bottom: 8px;">{{ meta }}</div>
    <video
      ref="videoEl"
      controls
      preload="metadata"
      :poster="posterUrl"
      :src="videoUrl"
      style="width: 100%; max-height: 76vh; display: block; background: #000;"
    />
  </el-dialog>
</template>
