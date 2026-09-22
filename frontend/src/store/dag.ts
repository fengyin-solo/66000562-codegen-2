import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import axios from 'axios'
import type { DAGWorkflow, ExecutionConfig, ExecutionInfo, WorkflowDraftNode } from '@/types'

const LAST_WORKFLOW_KEY = 'dag-workflow:last-id'
const LAST_EXECUTION_KEY = 'dag-workflow:last-execution'

export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const wsConnected = ref(false)
  const workers = ref(3)
  const strategy = ref<'fifo' | 'priority' | 'max_concurrent'>('fifo')

  let ws: WebSocket | null = null

  const configLocked = computed(() => Boolean(workflow.value))
  const status = computed(() => execution.value?.workflow.status || workflow.value?.status || 'QUEUED')

  function connectWS() {
    if (ws) ws.close()
    ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`)
    ws.onopen = () => { wsConnected.value = true }
    ws.onclose = () => { wsConnected.value = false }
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as ExecutionInfo
        if (!workflow.value || data.workflowId === workflow.value.id) {
          applyExecution(data)
        }
      } catch {
        // Ignore malformed realtime messages.
      }
    }
  }

  function applyWorkflow(data: DAGWorkflow) {
    workflow.value = data
    applyConfig(data.executionConfig)
    localStorage.setItem(LAST_WORKFLOW_KEY, String(data.id))
  }

  function applyConfig(config: ExecutionConfig) {
    workers.value = config.workers
    strategy.value = config.strategy
  }

  function applyExecution(data: ExecutionInfo) {
    execution.value = data
    workflow.value = data.workflow
    applyConfig(data.executionConfig)
    localStorage.setItem(LAST_WORKFLOW_KEY, String(data.workflowId))
    localStorage.setItem(LAST_EXECUTION_KEY, JSON.stringify(data))
  }

  async function createWorkflow(name: string, nodes?: WorkflowDraftNode[]) {
    loading.value = true
    try {
      const { data } = await axios.post<DAGWorkflow>('/api/workflow', {
        name,
        nodes,
        workers: workers.value,
        strategy: strategy.value,
      })
      execution.value = null
      localStorage.removeItem(LAST_EXECUTION_KEY)
      applyWorkflow(data)
    } finally {
      loading.value = false
    }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    try {
      const { data } = await axios.post<ExecutionInfo>('/api/run', { workflowId: workflow.value.id })
      applyExecution(data)
    } finally {
      loading.value = false
    }
  }

  async function interrupt() {
    if (!execution.value) return
    loading.value = true
    try {
      const { data } = await axios.post<ExecutionInfo>(`/api/interrupt/${execution.value.id}`)
      applyExecution(data)
    } finally {
      loading.value = false
    }
  }

  async function restoreLatest() {
    const lastId = localStorage.getItem(LAST_WORKFLOW_KEY)
    if (!lastId) return
    try {
      const { data } = await axios.get<DAGWorkflow>(`/api/workflow/${lastId}`)
      applyWorkflow(data)
      try {
        const execResp = await axios.get<ExecutionInfo>(`/api/workflow/${lastId}/execution`)
        applyExecution(execResp.data)
      } catch {
        // A workflow can exist before its first execution; use the latest local snapshot if present.
        const cachedExecution = localStorage.getItem(LAST_EXECUTION_KEY)
        if (cachedExecution) {
          try {
            const cached = JSON.parse(cachedExecution) as ExecutionInfo
            if (cached.workflowId === Number(lastId)) applyExecution(cached)
          } catch {
            // Keep the workflow restored from the server.
          }
        }
      }
    } catch {
      localStorage.removeItem(LAST_WORKFLOW_KEY)
    }
  }

  function disconnectWS() {
    ws?.close()
    ws = null
  }

  return {
    loading,
    workflow,
    execution,
    wsConnected,
    workers,
    strategy,
    configLocked,
    status,
    connectWS,
    createWorkflow,
    run,
    interrupt,
    restoreLatest,
    disconnectWS,
  }
})
