<script setup lang="ts">
import { ref, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { posterApi, type PosterItem } from '@/api'

const props = defineProps<{
  modelValue: boolean
  item: PosterItem | null
}>()
const emit = defineEmits<{
  (e: 'update:modelValue', v: boolean): void
  (e: 'replaced', thumb: string): void
}>()

const file = ref<File | null>(null)
const saving = ref(false)

watch(
  () => props.modelValue,
  (open) => {
    if (!open) {
      file.value = null
      saving.value = false
    }
  }
)

function onFileChange(uploadFile: { raw: File }) {
  file.value = uploadFile.raw
}

async function submit() {
  if (!props.item || !file.value) {
    ElMessage.warning('请选择图片文件')
    return
  }
  saving.value = true
  try {
    const r = await posterApi.replaceCover(props.item.folder, props.item.stem, file.value)
    if (r.ok && r.thumb) {
      ElMessage.success('封面已替换')
      emit('replaced', r.thumb)
      emit('update:modelValue', false)
    } else {
      ElMessage.error(r.error || '替换失败')
    }
  } catch {
    // handled
  } finally {
    saving.value = false
  }
}

function close() {
  emit('update:modelValue', false)
}
</script>

<template>
  <el-dialog
    :model-value="modelValue"
    title="替换封面"
    width="420px"
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <div v-if="item" class="muted" style="margin-bottom: 12px;">
      {{ item.folder || '根目录' }} · {{ item.video }}
    </div>
    <el-upload
      :auto-upload="false"
      :show-file-list="true"
      :limit="1"
      accept="image/*"
      :on-change="onFileChange"
    >
      <el-button>选择图片</el-button>
    </el-upload>
    <template #footer>
      <div style="text-align: right;">
        <el-button @click="close">取消</el-button>
        <el-button type="primary" :loading="saving" @click="submit">确认替换</el-button>
      </div>
    </template>
  </el-dialog>
</template>
