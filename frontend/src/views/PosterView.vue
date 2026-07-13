<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { posterApi, type PosterFolder, type PosterItem } from '@/api'
import PosterCard from '@/components/PosterCard.vue'
import VideoPreviewDialog from '@/components/VideoPreviewDialog.vue'
import ReplaceCoverDialog from '@/components/ReplaceCoverDialog.vue'

const route = useRoute()
const router = useRouter()

const allItems = ref<PosterItem[]>([])
const folders = ref<PosterFolder[]>([])
const folderFilter = ref<string>((route.query.folder as string) || '')
const sortSelect = ref<'time_desc' | 'time_asc' | 'size_desc' | 'size_asc' | 'duration_desc' | 'duration_asc'>('time_desc')
const pageSize = ref<number>(Number(localStorage.getItem('poster_page_size')) || 20)
const currentPage = ref(1)
const selectedKeys = ref<Set<string>>(new Set())
const durationCache = ref<Map<string, string>>(new Map())
const loading = ref(false)
const loadError = ref('')

const previewOpen = ref(false)
const previewItem = ref<PosterItem | null>(null)
const replaceOpen = ref(false)
const replaceItem = ref<PosterItem | null>(null)

function itemKey(i: PosterItem): string {
  return i.folder + '::' + i.stem
}

function durationLabel(i: PosterItem): string {
  return i.duration || durationCache.value.get(itemKey(i)) || '--:--'
}

function durationSeconds(i: PosterItem): number {
  const r = i.duration || durationCache.value.get(itemKey(i)) || ''
  const p = r.split(':').map(Number)
  if (!p.length || p.some(isNaN)) return -1
  if (p.length === 2) return p[0] * 60 + p[1]
  if (p.length === 3) return p[0] * 3600 + p[1] * 60 + p[2]
  return -1
}

const visibleItems = computed(() => {
  let list = allItems.value.filter((i) => !folderFilter.value || i.folder === folderFilter.value)
  const sv = sortSelect.value
  list = [...list].sort((a, b) => {
    if (sv === 'time_asc') return a.stem.localeCompare(b.stem)
    if (sv === 'time_desc') return b.stem.localeCompare(a.stem)
    if (sv === 'size_asc') return a.size - b.size
    if (sv === 'size_desc') return b.size - a.size
    if (sv === 'duration_asc') return durationSeconds(a) - durationSeconds(b)
    if (sv === 'duration_desc') return durationSeconds(b) - durationSeconds(a)
    return 0
  })
  return list
})

const totalPages = computed(() => Math.max(1, Math.ceil(visibleItems.value.length / pageSize.value)))

const currentPageItems = computed(() => {
  const start = (currentPage.value - 1) * pageSize.value
  return visibleItems.value.slice(start, start + pageSize.value)
})

const allSelected = computed(
  () => visibleItems.value.length > 0 && visibleItems.value.every((i) => selectedKeys.value.has(itemKey(i)))
)

function syncURL() {
  const q: Record<string, string> = {}
  if (folderFilter.value) q.folder = folderFilter.value
  router.replace({ path: '/poster', query: q })
}

async function loadItems() {
  loading.value = true
  loadError.value = ''
  try {
    const data = await posterApi.list(folderFilter.value || undefined)
    allItems.value = data.items || []
    folders.value = data.folders || []
    for (const i of allItems.value) {
      if (i.duration) durationCache.value.set(itemKey(i), i.duration)
    }
    currentPage.value = 1
  } catch (e: any) {
    loadError.value = e?.message || '加载失败'
    allItems.value = []
  } finally {
    loading.value = false
  }
}

async function pollDurations() {
  try {
    const data = await posterApi.durations(folderFilter.value)
    if (data.ok && data.durations) {
      let changed = false
      for (const [k, v] of Object.entries(data.durations)) {
        if (!durationCache.value.has(k)) {
          durationCache.value.set(k, v)
          changed = true
        }
      }
      if (changed) {
        // trigger reactivity
        durationCache.value = new Map(durationCache.value)
      }
    }
  } catch {
    // silent
  }
}

function onFolderChange() {
  syncURL()
  loadItems()
}

function onSortChange() {
  currentPage.value = 1
}

function onPageSizeChange() {
  localStorage.setItem('poster_page_size', String(pageSize.value))
  currentPage.value = 1
}

function onPageChange(p: number) {
  currentPage.value = p
}

function toggleSelect(item: PosterItem) {
  const k = itemKey(item)
  if (selectedKeys.value.has(k)) selectedKeys.value.delete(k)
  else selectedKeys.value.add(k)
  selectedKeys.value = new Set(selectedKeys.value)
}

function toggleSelectAll() {
  if (allSelected.value) {
    for (const i of visibleItems.value) selectedKeys.value.delete(itemKey(i))
  } else {
    for (const i of visibleItems.value) selectedKeys.value.add(itemKey(i))
  }
  selectedKeys.value = new Set(selectedKeys.value)
}

function openPreview(item: PosterItem) {
  previewItem.value = item
  previewOpen.value = true
}

function openReplace(item: PosterItem) {
  replaceItem.value = item
  replaceOpen.value = true
}

async function deleteItem(item: PosterItem) {
  try {
    await ElMessageBox.confirm(`确认删除 ${item.video} 吗？`, '提示', { type: 'warning' })
  } catch {
    return
  }
  try {
    const r = await posterApi.delete(item.folder, [item.stem])
    if (r.ok) {
      allItems.value = allItems.value.filter((c) => itemKey(c) !== itemKey(item))
      selectedKeys.value.delete(itemKey(item))
      selectedKeys.value = new Set(selectedKeys.value)
      ElMessage.success('已删除')
    }
  } catch {
    // handled
  }
}

async function batchDelete() {
  if (selectedKeys.value.size === 0) return
  try {
    await ElMessageBox.confirm(`确认删除选中的 ${selectedKeys.value.size} 个视频吗？`, '提示', { type: 'warning' })
  } catch {
    return
  }
  const groups = new Map<string, string[]>()
  for (const i of visibleItems.value) {
    if (selectedKeys.value.has(itemKey(i))) {
      const arr = groups.get(i.folder) || []
      arr.push(i.stem)
      groups.set(i.folder, arr)
    }
  }
  try {
    for (const [folder, stems] of groups) {
      const r = await posterApi.delete(folder, stems)
      if (!r.ok) {
        ElMessage.error('删除失败')
        return
      }
    }
    allItems.value = allItems.value.filter((i) => !selectedKeys.value.has(itemKey(i)))
    selectedKeys.value.clear()
    selectedKeys.value = new Set(selectedKeys.value)
    ElMessage.success('批量删除完成')
  } catch {
    // handled
  }
}

function onCoverReplaced(newThumb: string) {
  if (replaceItem.value) {
    const item = replaceItem.value
    const updated: PosterItem = {
      ...item,
      thumb: newThumb,
      thumbnail_url: `/api/poster-thumb?folder=${encodeURIComponent(item.folder)}&name=${encodeURIComponent(newThumb)}&t=${Date.now()}`,
    }
    allItems.value = allItems.value.map((c) => (itemKey(c) === itemKey(item) ? updated : c))
  }
}

onMounted(async () => {
  await loadItems()
  setTimeout(pollDurations, 2000)
  setTimeout(pollDurations, 5000)
  setTimeout(pollDurations, 12000)
})
</script>

<template>
  <div>
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 16px;">
      <el-button text @click="router.push('/')">← 返回</el-button>
      <h1 class="page-title" style="margin: 0;">
        {{ folderFilter ? folderFilter + ' · 海报墙' : '视频海报墙' }}
      </h1>
    </div>

    <div class="muted" style="margin-bottom: 12px;">
      点击海报即可预览视频，可按文件夹筛选、排序和批量操作。
    </div>

    <!-- 工具栏 -->
    <div style="display: flex; align-items: flex-end; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid var(--el-border-color);">
      <div>
        <div class="muted" style="font-size: 11px; margin-bottom: 4px;">文件夹筛选</div>
        <el-select v-model="folderFilter" placeholder="全部文件夹" style="width: 200px;" @change="onFolderChange">
          <el-option label="全部文件夹" value="" />
          <el-option
            v-for="f in folders"
            :key="f.folder"
            :label="`${f.folder || '根目录'} (${f.count})`"
            :value="f.folder"
          />
        </el-select>
      </div>
      <div>
        <div class="muted" style="font-size: 11px; margin-bottom: 4px;">排序</div>
        <el-select v-model="sortSelect" style="width: 160px;" @change="onSortChange">
          <el-option label="按名称降序" value="time_desc" />
          <el-option label="按名称升序" value="time_asc" />
          <el-option label="大小从大到小" value="size_desc" />
          <el-option label="大小从小到大" value="size_asc" />
          <el-option label="时长从长到短" value="duration_desc" />
          <el-option label="时长从短到长" value="duration_asc" />
        </el-select>
      </div>
      <div>
        <div class="muted" style="font-size: 11px; margin-bottom: 4px;">每页</div>
        <el-select v-model="pageSize" style="width: 100px;" @change="onPageSizeChange">
          <el-option :value="10" label="10" />
          <el-option :value="15" label="15" />
          <el-option :value="20" label="20" />
          <el-option :value="25" label="25" />
          <el-option :value="30" label="30" />
        </el-select>
      </div>
      <div style="flex: 1;"></div>
      <el-button @click="loadItems">刷新</el-button>
      <el-button @click="toggleSelectAll">{{ allSelected ? '取消全选' : '全选' }}</el-button>
      <el-button type="danger" :disabled="selectedKeys.size === 0" @click="batchDelete">
        批量删除
      </el-button>
    </div>

    <!-- 状态条 -->
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
      <div class="muted">
        <span v-if="loading">正在加载…</span>
        <span v-else-if="loadError" style="color: var(--el-color-danger);">{{ loadError }}</span>
        <span v-else>
          {{ folderFilter ? folderFilter + ' · ' : '全部 · ' }}
          共 <strong>{{ visibleItems.length }}</strong> 个，第 <strong>{{ currentPage }}</strong>/<strong>{{ totalPages }}</strong> 页
        </span>
      </div>
      <div class="muted">
        已选 <strong style="color: var(--el-color-primary);">{{ selectedKeys.size }}</strong> 个
      </div>
    </div>

    <!-- 网格 -->
    <div v-loading="loading">
      <div v-if="visibleItems.length === 0 && !loading" class="muted" style="padding: 40px 0; text-align: center;">
        当前筛选条件下暂无视频。
      </div>
      <div v-else class="poster-grid">
        <PosterCard
          v-for="item in currentPageItems"
          :key="itemKey(item)"
          :item="item"
          :selected="selectedKeys.has(itemKey(item))"
          :duration-label="durationLabel(item)"
          @toggle-select="toggleSelect(item)"
          @preview="openPreview(item)"
          @replace-cover="openReplace(item)"
          @delete="deleteItem(item)"
        />
      </div>
    </div>

    <!-- 分页 -->
    <div v-if="visibleItems.length > 0" style="margin-top: 16px; display: flex; justify-content: center;">
      <el-pagination
        v-model:current-page="currentPage"
        :page-size="pageSize"
        :total="visibleItems.length"
        layout="prev, pager, next, jumper, total"
        @current-change="onPageChange"
      />
    </div>

    <!-- 预览弹窗 -->
    <VideoPreviewDialog
      v-model="previewOpen"
      :title="previewItem?.video || '视频预览'"
      :meta="previewItem ? `${previewItem.folder || '根目录'} · 时长 ${durationLabel(previewItem)}` : ''"
      :video-url="previewItem?.video_url || ''"
      :poster-url="previewItem?.thumbnail_url || ''"
    />

    <!-- 替换封面弹窗 -->
    <ReplaceCoverDialog
      v-model="replaceOpen"
      :item="replaceItem"
      @replaced="onCoverReplaced"
    />
  </div>
</template>
