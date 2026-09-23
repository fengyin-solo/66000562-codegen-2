<template>
  <div class="app-root">
    <header class="top-bar">
      <h1>🔀 分布式任务工作流DAG编排与执行引擎</h1>
      <div class="tools">
        <el-input v-model="wfName" placeholder="工作流名称" size="small" style="width:150px"
          :class="{ 'input-error': nameError }" />
        <el-select v-model="tpl" size="small" style="width:130px">
          <el-option value="dag" label="标准流水线" />
          <el-option value="cyclic" label="含循环等待" />
        </el-select>
        <el-select v-model="store.workers" size="small" style="width:100px" :disabled="hasWorkflow">
          <el-option :value="1" label="1 Worker"/><el-option :value="3" label="3 Workers"/><el-option :value="5" label="5 Workers"/>
        </el-select>
        <el-select v-model="store.strategy" size="small" style="width:100px" :disabled="hasWorkflow">
          <el-option value="fifo" label="FIFO"/><el-option value="priority" label="优先级"/><el-option value="max_concurrent" label="最大并发"/>
        </el-select>
        <el-button size="small" @click="create" :loading="store.loading">创建DAG</el-button>
        <el-button type="success" size="small" @click="run"
          :disabled="!store.workflow || isRunning || isFinished" :loading="store.loading">▶ 执行</el-button>
        <el-button type="warning" size="small" @click="stop"
          :disabled="!isRunning">⏸ 打断</el-button>
        <el-button size="small" @click="reset"
          :disabled="!store.workflow || isRunning">↺ 重置</el-button>
        <el-tag v-if="store.workflow" size="small" :type="statusTag" effect="dark" class="wf-status">
          {{ statusText }}
        </el-tag>
        <span class="ws-dot" :class="{on:store.wsConnected}" :title="store.wsConnected ? '实时连接' : '未连接'"></span>
      </div>
    </header>

    <div v-if="nameError" class="error-banner">
      <el-alert :title="`${nameError.field === 'name' ? '' : '【' + nameError.field + '】'}${nameError.message}`"
        type="error" show-icon :closable="false" />
    </div>
    <div v-for="(e, i) in otherErrors" :key="i" class="error-banner">
      <el-alert :title="`【${e.field}】${e.message}`" type="error" show-icon :closable="false" />
    </div>
    <div v-if="store.globalError" class="error-banner">
      <el-alert :title="store.globalError" type="error" show-icon :closable="false" />
    </div>
    <div v-if="cycleWarning" class="warn-banner">
      <el-alert :title="cycleWarning.message" type="warning" show-icon :closable="false" />
    </div>
    <div v-if="hasWorkflow" class="config-hint">
      当前工作流：{{ store.workflow!.name }} ｜ 并发上限 {{ store.workflow!.workers }} ｜
      策略 {{ strategyText(store.workflow!.strategy) }}（创建后绑定，多次执行保持一致）
    </div>

    <div class="main-grid">
      <div class="dag-area">
        <DAGCanvas />
      </div>
      <div class="side-area">
        <LogPanel />
        <CircuitBreakerPanel />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import DAGCanvas from './components/DAGCanvas.vue'
import LogPanel from './components/LogPanel.vue'
import CircuitBreakerPanel from './components/CircuitBreakerPanel.vue'
import { useDAGStore } from './store/dag'

const store = useDAGStore()
const wfName = ref('data-pipeline')
const tpl = ref('dag')

const hasWorkflow = computed(() => !!store.workflow)
const isRunning = computed(() => store.workflow?.status === 'RUNNING')
const isFinished = computed(() => store.workflow?.status === 'FINISHED')

const nameError = computed(() => store.fieldErrors.find(e => e.field === 'name'))
const otherErrors = computed(() => store.fieldErrors.filter(e => e.field !== 'name'))
const cycleWarning = computed(() => store.workflow?.warnings?.find(w => w.type === 'CYCLE'))

const STATUS_MAP: Record<string, { text: string; tag: 'info' | 'warning' | 'success' }> = {
  PENDING: { text: '排队待执行', tag: 'info' },
  RUNNING: { text: '执行中', tag: 'warning' },
  FINISHED: { text: '结束', tag: 'success' },
}
const statusText = computed(() => STATUS_MAP[store.workflow?.status ?? 'PENDING']?.text ?? '')
const statusTag = computed(() => STATUS_MAP[store.workflow?.status ?? 'PENDING']?.tag ?? 'info')

function strategyText(s: string) {
  return { fifo: 'FIFO', priority: '优先级', max_concurrent: '最大并发' }[s] ?? s
}

async function create() {
  await store.createWorkflow(wfName.value, tpl.value)
}
function run() { store.run() }
function stop() { store.stop() }
function reset() { store.reset() }

onMounted(() => {
  store.connectWS()
  // 页面重开：恢复到最后查看的工作流及其进展
  store.restoreLast()
})
onUnmounted(() => store.disconnectWS())
</script>

<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:system-ui,sans-serif;background:#0c0c1d;color:#e0e0e0}
.app-root{height:100vh;display:flex;flex-direction:column}
.top-bar{display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:#1a1a2e;border-bottom:1px solid #2a2a4a;flex-wrap:wrap;gap:8px}
.top-bar h1{font-size:1rem;color:#bb86fc}
.tools{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.ws-dot{width:8px;height:8px;border-radius:50%;background:#ef4444}.ws-dot.on{background:#22c55e}
.wf-status{margin-left:4px}
.input-error :deep(.el-input__wrapper){box-shadow:0 0 0 1px #ef4444 inset !important}
.error-banner,.warn-banner{padding:6px 20px 0}
.config-hint{padding:6px 20px;color:#9ca3af;font-size:11px}
.main-grid{display:grid;grid-template-columns:1fr 320px;flex:1;overflow:hidden}
.dag-area{background:#0f0f23;position:relative;overflow:hidden}
.side-area{display:flex;flex-direction:column;gap:8px;padding:8px;overflow-y:auto;background:#14142b}
</style>
