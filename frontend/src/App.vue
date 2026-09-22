<template>
  <div class="app-root">
    <header class="top-bar">
      <div>
        <h1>🔀 分布式任务工作流DAG编排与执行引擎</h1>
        <div v-if="store.workflow" class="workflow-meta">
          <span>{{ store.workflow.name }}</span>
          <el-tag size="small" :type="statusTag">{{ statusText }}</el-tag>
          <span>{{ store.workflow.executionConfig.workers }} Workers</span>
          <span>{{ strategyText }}</span>
        </div>
      </div>
      <div class="tools">
        <el-select v-model="store.workers" size="small" style="width:110px" :disabled="store.configLocked">
          <el-option :value="1" label="1 Worker" />
          <el-option :value="2" label="2 Workers" />
          <el-option :value="3" label="3 Workers" />
          <el-option :value="5" label="5 Workers" />
        </el-select>
        <el-select v-model="store.strategy" size="small" style="width:112px" :disabled="store.configLocked">
          <el-option value="fifo" label="FIFO" />
          <el-option value="priority" label="优先级" />
          <el-option value="max_concurrent" label="最大并发" />
        </el-select>
        <el-button size="small" @click="openCreate">新建编排</el-button>
        <el-button
          type="success"
          size="small"
          @click="run"
          :disabled="!store.workflow || store.status === 'RUNNING' || store.status === 'FINISHED'"
          :loading="store.loading"
        >{{ runButtonText }}</el-button>
        <el-button
          type="warning"
          size="small"
          @click="interrupt"
          :disabled="store.status !== 'RUNNING'"
          :loading="store.loading"
        >打断</el-button>
        <span class="ws-dot" :class="{ on: store.wsConnected }"></span>
      </div>
    </header>

    <div v-for="warning in warnings" :key="warning.nodeIds.join('-')" class="cycle-banner">
      ⚠️ {{ warning.message }}；未受影响的环节仍可执行，受影响环节已标记为“阻塞”。
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

    <el-dialog v-model="createVisible" title="创建工作流编排" width="680px">
      <el-form label-position="top">
        <el-form-item required label="工作流名称">
          <el-input v-model="wfName" placeholder="请输入唯一的工作流名称" />
        </el-form-item>
        <div class="dialog-row">
          <el-form-item label="并发上限">
            <el-select v-model="store.workers" style="width:130px">
              <el-option :value="1" label="1 Worker" />
              <el-option :value="2" label="2 Workers" />
              <el-option :value="3" label="3 Workers" />
              <el-option :value="5" label="5 Workers" />
            </el-select>
          </el-form-item>
          <el-form-item label="优先级策略">
            <el-select v-model="store.strategy" style="width:150px">
              <el-option value="fifo" label="FIFO" />
              <el-option value="priority" label="优先级" />
              <el-option value="max_concurrent" label="最大并发" />
            </el-select>
          </el-form-item>
        </div>
        <el-form-item label="环节编排（JSON，可留空使用默认 DAG）">
          <el-input
            v-model="draftJson"
            type="textarea"
            :rows="12"
            placeholder='[{"id":"a","name":"提取","deps":[],"duration":1,"priority":5}]'
          />
        </el-form-item>
        <div class="dialog-actions">
          <el-button size="small" @click="useDefaultNodes">填入默认 DAG</el-button>
          <el-button size="small" @click="useCycleExample">填入循环依赖示例</el-button>
        </div>
      </el-form>
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" :loading="store.loading" @click="create">创建</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import axios from 'axios'
import DAGCanvas from './components/DAGCanvas.vue'
import LogPanel from './components/LogPanel.vue'
import CircuitBreakerPanel from './components/CircuitBreakerPanel.vue'
import { useDAGStore } from './store/dag'
import type { WorkflowDraftNode } from './types'

const store = useDAGStore()
const createVisible = ref(false)
const wfName = ref('')
const draftJson = ref('')

const defaultNodes: WorkflowDraftNode[] = [
  { id: 'extract', name: '数据提取', deps: [], duration: 1.5, priority: 5 },
  { id: 'validate', name: '数据校验', deps: ['extract'], duration: 1, priority: 5 },
  { id: 'clean', name: '数据清洗', deps: ['validate'], duration: 1.5, priority: 4 },
  { id: 'report', name: '生成报表', deps: ['clean'], duration: 1, priority: 3 },
]

const cycleNodes: WorkflowDraftNode[] = [
  { id: 'prepare', name: '准备数据', deps: [], duration: 0.8, priority: 5 },
  { id: 'train', name: '模型训练', deps: ['prepare', 'evaluate'], duration: 1, priority: 4 },
  { id: 'evaluate', name: '模型评估', deps: ['train'], duration: 0.8, priority: 4 },
  { id: 'report', name: '独立报表', deps: ['prepare'], duration: 0.8, priority: 3 },
]

const warnings = computed(() => store.execution?.warnings || store.workflow?.warnings || [])
const statusText = computed(() => ({ QUEUED: '排队待执行', RUNNING: '执行中', FINISHED: '结束' }[store.status]))
const statusTag = computed(() => ({ QUEUED: 'info', RUNNING: 'primary', FINISHED: 'success' } as const))
const strategyText = computed(() => ({ fifo: 'FIFO', priority: '优先级', max_concurrent: '最大并发' }[store.strategy]))
const runButtonText = computed(() => {
  if (!store.execution) return '▶ 开始执行'
  return store.status === 'QUEUED' ? '▶ 恢复执行' : '▶ 执行'
})

function openCreate() {
  wfName.value = ''
  draftJson.value = ''
  createVisible.value = true
}

function useDefaultNodes() {
  draftJson.value = JSON.stringify(defaultNodes, null, 2)
}

function useCycleExample() {
  draftJson.value = JSON.stringify(cycleNodes, null, 2)
}

async function create() {
  const name = wfName.value.trim()
  if (!name) {
    ElMessage.error('工作流名称为空，不能继续创建')
    return
  }

  let nodes: WorkflowDraftNode[] | undefined
  if (draftJson.value.trim()) {
    try {
      const parsed = JSON.parse(draftJson.value)
      nodes = Array.isArray(parsed) ? parsed : parsed.nodes
      if (!Array.isArray(nodes)) throw new Error('nodes must be array')
    } catch {
      ElMessage.error('环节编排不是合法 JSON，不能继续创建')
      return
    }
  }

  try {
    await store.createWorkflow(name, nodes)
    createVisible.value = false
    ElMessage.success('工作流已创建')
  } catch (error) {
    if (axios.isAxiosError(error)) {
      const detail = error.response?.data?.detail
      ElMessage.error(detail?.message || detail || '工作流创建失败')
    } else {
      ElMessage.error('工作流创建失败')
    }
  }
}

async function run() {
  try {
    await store.run()
  } catch (error) {
    if (axios.isAxiosError(error)) {
      ElMessage.error(error.response?.data?.detail?.message || '执行失败')
    }
  }
}

async function interrupt() {
  try {
    await store.interrupt()
    ElMessage.info('已打断，工作流回到排队待执行')
  } catch (error) {
    if (axios.isAxiosError(error)) {
      ElMessage.error(error.response?.data?.detail?.message || '打断失败')
    }
  }
}

onMounted(() => {
  store.connectWS()
  store.restoreLatest()
})
onUnmounted(() => store.disconnectWS())
</script>

<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:system-ui,sans-serif;background:#0c0c1d;color:#e0e0e0}
.app-root{height:100vh;display:flex;flex-direction:column}
.top-bar{display:flex;justify-content:space-between;align-items:center;padding:10px 20px;background:#1a1a2e;border-bottom:1px solid #2a2a4a;gap:16px}
.top-bar h1{font-size:1rem;color:#bb86fc}
.workflow-meta{display:flex;align-items:center;gap:10px;margin-top:4px;font-size:11px;color:#9ca3af}
.tools{display:flex;gap:6px;align-items:center}
.ws-dot{width:8px;height:8px;border-radius:50%;background:#ef4444}.ws-dot.on{background:#22c55e}
.cycle-banner{padding:8px 20px;background:#78350f;color:#ffedd5;font-size:12px;border-bottom:1px solid #92400e}
.main-grid{display:grid;grid-template-columns:1fr 320px;flex:1;overflow:hidden}
.dag-area{background:#0f0f23;position:relative;overflow:hidden}
.side-area{display:flex;flex-direction:column;gap:8px;padding:8px;overflow-y:auto;background:#14142b}
.dialog-row{display:flex;gap:16px}
.dialog-actions{display:flex;gap:8px;margin-top:-6px}
:deep(.el-select.is-disabled){opacity:.55}
</style>
