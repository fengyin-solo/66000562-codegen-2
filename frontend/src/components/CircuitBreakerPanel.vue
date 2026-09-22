<template>
  <div class="panel">
    <h4>⚡ 熔断器状态</h4>
    <div v-for="cb in breakers" :key="cb.taskId" class="cb-row" :class="cb.state.toLowerCase()">
      <span class="cb-task">{{ cb.taskId }}</span>
      <span class="cb-state">{{ stateText(cb.state) }}</span>
      <span class="cb-count">{{ cb.failureCount }} 次失败</span>
    </div>
    <div v-if="!breakers.length" class="empty">无熔断保护激活</div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useDAGStore } from '../store/dag'

const store = useDAGStore()
const breakers = computed(() => store.execution?.circuitBreakers || [])
const labels: Record<string, string> = { OPEN: '熔断打开', CLOSED: '正常闭合', HALF_OPEN: '半开探测' }
function stateText(state: string) {
  return labels[state] || state
}
</script>
<style scoped>
.panel{background:#1a1a2e;border-radius:8px;padding:10px;border:1px solid #2a2a4a}
.panel h4{color:#f87171;font-size:12px;margin-bottom:6px}
.cb-row{display:flex;gap:8px;padding:4px 6px;border-radius:4px;font-size:11px;margin:2px 0}
.cb-row.open{background:#ef444415}
.cb-task{color:#ccc;font-weight:600}.cb-state{font-weight:700}.cb-count{color:#888;font-size:10px}
.open .cb-state{color:#ef4444}.closed .cb-state{color:#22c55e}.half_open .cb-state{color:#fbbf24}
.empty{color:#4a5568;font-size:11px}
</style>
