<template>
  <div class="panel">
    <h4>📜 执行日志</h4>
    <div ref="listEl" class="log-list" @scroll="onScroll">
      <div v-for="(l,i) in logs" :key="i" class="log-row" :class="l.status.toLowerCase()">
        <span class="l-status">{{ statusText(l.status) }}</span>
        <span class="l-task">{{ l.taskId }}</span>
        <span class="l-msg">{{ l.message }}</span>
      </div>
      <div v-if="!logs.length" class="empty">等待执行...</div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch, nextTick, onMounted } from 'vue'
import { useDAGStore } from '../store/dag'
const store = useDAGStore()
const logs = computed(() => store.execution?.logs || [])
const listEl = ref<HTMLElement>()

const LS_LOG_SCROLL = 'dag.logScroll'
const STATUS: Record<string, string> = {
  RUNNING: '执行中', SUCCESS: '成功', FAILED: '失败',
  BLOCKED: '跳过', PENDING: '待执行', CIRCUIT_OPEN: '熔断'
}
function statusText(s: string) { return STATUS[s] ?? s }

function atBottom() {
  const el = listEl.value
  if (!el) return true
  return el.scrollHeight - el.scrollTop - el.clientHeight < 24
}

// 执行中有新日志时自动滚到底部；用户手动翻看历史时不打断
watch(() => logs.value.length, async () => {
  if (store.workflow?.status !== 'RUNNING') return
  await nextTick()
  const el = listEl.value
  if (el) el.scrollTop = el.scrollHeight
})

// 页面重开：恢复到最后看到的滚动位置（最后看到的进展位置）
watch(() => store.workflow?.id, async () => {
  await nextTick()
  const el = listEl.value
  if (!el) return
  const saved = Number(localStorage.getItem(LS_LOG_SCROLL) || '0')
  el.scrollTop = Math.max(0, Math.min(saved, el.scrollHeight - el.clientHeight))
}, { immediate: false })

let saveTimer = 0
function onScroll() {
  const el = listEl.value
  if (!el) return
  // 停在底部时无需记忆精确值；否则保留位置，重开页面后回到该处
  if (!atBottom()) {
    window.clearTimeout(saveTimer)
    saveTimer = window.setTimeout(() => localStorage.setItem(LS_LOG_SCROLL, String(el.scrollTop)), 150)
  }
}

onMounted(async () => {
  await nextTick()
  const el = listEl.value
  if (el) {
    const saved = Number(localStorage.getItem(LS_LOG_SCROLL) || '0')
    if (saved > 0) el.scrollTop = saved
  }
})
</script>
<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a;flex:1;display:flex;flex-direction:column;min-height:0}
.panel h4{color:#bb86fc;font-size:12px;margin-bottom:6px}
.log-list{max-height:280px;overflow-y:auto;font-size:10px;font-family:monospace;flex:1}
.log-row{display:flex;gap:6px;padding:2px 4px;border-radius:2px;margin:1px 0}
.log-row.running{background:#3182ce15}.log-row.success{color:#38a169}.log-row.failed{color:#e53e3e;background:#e53e3e10}
.log-row.blocked{color:#d97706;background:#d9770610}.log-row.circuit_open{color:#fbbf24;background:#fbbf2410}
.log-row.pending{color:#9ca3af}
.l-status{font-weight:700;min-width:44px}.l-task{color:#888;min-width:70px}.l-msg{color:#ccc}.empty{color:#4a5568}
</style>
