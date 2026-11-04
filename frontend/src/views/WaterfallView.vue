<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { api, waterfallApi, type WaterfallItem } from '@/api'
import VideoPreviewDialog from '@/components/VideoPreviewDialog.vue'

const route = useRoute()
const router = useRouter()

const items = ref<WaterfallItem[]>([])
const loading = ref(false)
const loadError = ref('')
const currentRange = ref<string>((route.query.range as string) || 'daily')
const currentPage = ref<number>(Number(route.query.page as string) || 1)
const pagination = ref({ page: 1, per_page: 10, has_next: false })
const perPage = ref<number>(10)
const selectedIds = ref<Set<string>>(new Set())

const settingsOpen = ref(false)
const settingsPerPage = ref<number>(10)

const previewOpen = ref(false)
const previewItem = ref<WaterfallItem | null>(null)

const ranges = [
  { code: 'daily', name: '日榜' },
  { code: 'weekly', name: '周榜' },
  { code: 'monthly', name: '月榜' },
  { code: 'all', name: '总榜' },
]

const allSelected = computed(
  () => items.value.length > 0 && items.value.every((i) => selectedIds.value.has(i.id))
)

function updateURL() {
  const q: Record<string, string> = {}
  if (currentRange.value && currentRange.value !== 'daily') q.range = currentRange.value
  if (currentPage.value > 1) q.page = String(currentPage.value)
  router.replace({ path: '/waterfall', query: q })
}

async function loadData() {
  loading.value = true
  loadError.value = ''
  selectedIds.value = new Set()
  try {
    const data = await waterfallApi.list({
      page: currentPage.value,
      range: currentRange.value,
    })
    items.value = data.items || []
    if (data.config?.per_page) {
      perPage.value = data.config.per_page
      settingsPerPage.value = data.config.per_page
    }
    if (data.pagination) {
      pagination.value = {
        page: data.pagination.page || currentPage.value,
        per_page: data.pagination.per_page || perPage.value,
        has_next: Boolean(data.pagination.has_next),
      }
      currentPage.value = pagination.value.page
    }
    updateURL()
  } catch (e: any) {
    items.value = []
    loadError.value = e?.message || '加载失败'
  } finally {
    loading.value = false
  }
}

function switchRange(r: string) {
  if (r === currentRange.value || loading.value) return
  currentRange.value = r
  currentPage.value = 1
  loadData()
}

function changePage(d: number) {
  if (loading.value) return
  const n = currentPage.value + d
  if (n < 1 || (d > 0 && !pagination.value.has_next)) return
  currentPage.value = n
  loadData()
}

function toggleSelect(item: WaterfallItem) {
  if (selectedIds.value.has(item.id)) selectedIds.value.delete(item.id)
  else selectedIds.value.add(item.id)
  selectedIds.value = new Set(selectedIds.value)
}

function toggleSelectAll() {
  if (allSelected.value) {
    for (const i of items.value) selectedIds.value.delete(i.id)
  } else {
    for (const i of items.value) selectedIds.value.add(i.id)
  }
  selectedIds.value = new Set(selectedIds.value)
}

function openPreview(item: WaterfallItem) {
  previewItem.value = item
  previewOpen.value = true
}

async function downloadSelected() {
  const selected = items.value.filter((i) => selectedIds.value.has(i.id))
  if (!selected.length || loading.value) return
  try {
    const r = await waterfallApi.download(selected)
    if (r.ok) {
      ElMessage.success(`成功 ${r.success}，跳过 ${r.skipped}，失败 ${r.failed}`)
      selectedIds.value = new Set()
    } else {
      ElMessage.error(r.error || '下载失败')
    }
  } catch {
    // handled
  }
}

async function saveSettings() {
  try {
    const r = await api.saveWaterfallSettings(settingsPerPage.value)
    if (r.ok) {
      perPage.value = r.config?.per_page || settingsPerPage.value
      settingsOpen.value = false
      currentPage.value = 1
      await loadData()
    }
  } catch {
    // handled
  }
}

onMounted(loadData)
</script>

<template>
  <div>
    <h1 class="page-title">远程视频</h1>
    <div class="muted" style="margin-bottom: 16px;">每页 {{ perPage }} 个</div>

    <!-- 工具栏 -->
    <div style="display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 16px;">
      <el-button
        v-for="r in ranges"
        :key="r.code"
        :type="r.code === currentRange ? 'primary' : 'default'"
        @click="switchRange(r.code)"
      >
        {{ r.name }}
      </el-button>
      <div style="flex: 1;"></div>
      <el-button @click="settingsOpen = true">设置</el-button>
      <el-button @click="loadData">刷新</el-button>
      <el-button @click="toggleSelectAll" :dis