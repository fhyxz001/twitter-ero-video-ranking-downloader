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
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 8px;">
      <el-button text @click="router.push('/')">← 返回</el-button>
      <h1 class="page-title" style="margin: 0;">瀑布流</h1>
    </div>
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
      <el-button @click="toggleSelectAll" :disabled="items.length === 0">
        {{ allSelected ? '取消全选' : '全选' }}
      </el-button>
      <el-button type="primary" :disabled="selectedIds.size === 0 || loading" @click="downloadSelected">
        下载选中
      </el-button>
    </div>

    <!-- 状态条 -->
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
      <div class="muted">
        <span v-if="loading">正在加载第 {{ currentPage }} 页…</span>
        <span v-else-if="loadError" style="color: var(--el-color-danger);">{{ loadError }}</span>
        <span v-else>当前页共 {{ items.length }} 个视频</span>
      </div>
      <div style="display: flex; gap: 12px; align-items: center;">
        <div class="muted">已选 <strong style="color: var(--el-color-primary);">{{ selectedIds.size }}</strong> 个</div>
        <div style="display: flex; gap: 4px;">
          <el-button size="small" :disabled="loading || currentPage <= 1" @click="changePage(-1)">上一页</el-button>
          <span class="muted" style="line-height: 28px;">第 {{ currentPage }} 页</span>
          <el-button size="small" :disabled="loading || !pagination.has_next" @click="changePage(1)">下一页</el-button>
        </div>
      </div>
    </div>

    <!-- 瀑布流 -->
    <div v-loading="loading">
      <div v-if="items.length === 0 && !loading" class="muted" style="padding: 40px 0; text-align: center;">
        当前配置下没有可预览的视频。
      </div>
      <div v-else class="waterfall-masonry">
        <div
          v-for="item in items"
          :key="item.id"
          class="waterfall-card"
          :class="{ selected: selectedIds.has(item.id) }"
        >
          <div class="waterfall-card-cover" @click="openPreview(item)">
            <div class="checkbox" @click.stop>
              <el-checkbox
                :model-value="selectedIds.has(item.id)"
                @update:model-value="toggleSelect(item)"
              />
            </div>
            <img v-if="item.thumbnail" :src="item.thumbnail" :alt="item.title || item.id" loading="lazy" />
            <div v-else class="placeholder">视频</div>
            <div class="play-overlay">▶</div>
          </div>
          <div class="waterfall-card-body">
            <div class="waterfall-card-title" :title="item.title || item.id">
              {{ item.title || item.id }}
            </div>
            <div class="waterfall-card-meta">
              <el-tag size="small">{{ item.id }}</el-tag>
              <el-tag v-if="item.pv != null" size="small" type="info">{{ item.pv }} 播放</el-tag>
              <el-tag v-if="item.favorite_count != null" size="small" type="warning">
                {{ item.favorite_count }} 收藏
              </el-tag>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 设置弹窗 -->
    <el-dialog v-model="settingsOpen" title="瀑布流设置" width="420px">
      <el-form label-width="100px">
        <el-form-item label="每页数量">
          <el-select v-model="settingsPerPage" style="width: 200px;">
            <el-option :value="10" label="10" />
            <el-option :value="20" label="20" />
            <el-option :value="30" label="30" />
            <el-option :value="50" label="50" />
            <el-option :value="100" label="100" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <div style="text-align: right;">
          <el-button @click="settingsOpen = false">取消</el-button>
          <el-button type="primary" @click="saveSettings">保存</el-button>
        </div>
      </template>
    </el-dialog>

    <!-- 预览弹窗 -->
    <VideoPreviewDialog
      v-model="previewOpen"
      :title="previewItem?.title || previewItem?.id || '视频预览'"
      :video-url="previewItem?.preview_url || previewItem?.url || ''"
      :poster-url="previewItem?.thumbnail || ''"
    />
  </div>
</template>
