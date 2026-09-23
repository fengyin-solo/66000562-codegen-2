import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios, { AxiosError } from 'axios'
import type { DAGWorkflow, ExecutionInfo, FieldError } from '@/types'

// 页面重开后恢复到最后看到的位置
const LS_WORKFLOW = 'dag.lastWorkflowId'

export const useDAGStore = defineStore('dag', () => {
  const loading = ref(false)
  const workflow = ref<DAGWorkflow | null>(null)
  const execution = ref<ExecutionInfo | null>(null)
  const wsConnected = ref(false)
  // 并发上限与优先级策略绑定在工作流上，创建后不可更改
  const workers = ref(3)
  const strategy = ref('fifo')
  const fieldErrors = ref<FieldError[]>([])
  const globalError = ref('')

  let ws: WebSocket | null = null
  let reconnectTimer: number | null = null

  function connectWS() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    ws = new WebSocket(`${proto}://${location.host}/ws`)
    ws.onopen = () => {
      wsConnected.value = true
      if (workflow.value) {
        ws?.send(JSON.stringify({ type: 'subscribe', workflowId: workflow.value.id }))
      }
    }
    ws.onmessage = (e) => {
      try {
        const d = JSON.parse(e.data) as ExecutionInfo
        if (workflow.value && d.workflow.id !== workflow.value.id) return
        execution.value = d
        workflow.value = d.workflow
      } catch { /* 忽略非法消息 */ }
    }
    ws.onclose = () => {
      wsConnected.value = false
      // 断线后自动重连，重连成功后按最后查看的工作流恢复进展
      if (reconnectTimer === null) {
        reconnectTimer = window.setTimeout(() => {
          reconnectTimer = null
          connectWS()
        }, 1500)
      }
    }
  }

  function subscribe(wfId: number) {
    localStorage.setItem(LS_WORKFLOW, String(wfId))
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'subscribe', workflowId: wfId }))
    }
  }

  function applySnapshot(data: ExecutionInfo) {
    execution.value = data
    workflow.value = data.workflow
    workers.value = data.workflow.workers
    strategy.value = data.workflow.strategy
  }

  async function restoreLast() {
    const raw = localStorage.getItem(LS_WORKFLOW)
    if (!raw) return
    try {
      const { data } = await axios.get<ExecutionInfo>(`/api/workflow/${Number(raw)}`)
      applySnapshot(data)
      subscribe(data.workflow.id)
    } catch {
      localStorage.removeItem(LS_WORKFLOW)
    }
  }

  function extractErrors(err: unknown): FieldError[] {
    const ax = err as AxiosError<{ detail: FieldError[] | string }>
    const detail = ax.response?.data?.detail
    if (Array.isArray(detail)) return detail
    if (typeof detail === 'string') return [{ field: '_', message: detail }]
    return [{ field: '_', message: '请求失败，请稍后重试' }]
  }

  async function createWorkflow(name: string, template: string) {
    loading.value = true
    fieldErrors.value = []
    globalError.value = ''
    try {
      const { data } = await axios.post<ExecutionInfo>('/api/workflow', {
        name,
        workers: workers.value,
        strategy: strategy.value,
        template,
      })
      applySnapshot(data)
      subscribe(data.workflow.id)
      return true
    } catch (err) {
      // 明确指出是哪一项不合理（名称为空 / 名称重复 / 编排字段问题）
      fieldErrors.value = extractErrors(err)
      return false
    } finally {
      loading.value = false
    }
  }

  async function run() {
    if (!workflow.value) return
    loading.value = true
    globalError.value = ''
    try {
      // 不传 workers/strategy：并发上限与优先级以创建时绑定的为准，保证多次执行一致
      const { data } = await axios.post<ExecutionInfo>('/api/run', { workflowId: workflow.value.id })
      applySnapshot(data)
      subscribe(data.workflow.id)
    } catch (err) {
      globalError.value = extractErrors(err)[0].message
    } finally {
      loading.value = false
    }
  }

  async function stop() {
    if (!workflow.value) return
    try {
      await axios.post(`/api/workflow/${workflow.value.id}/stop`)
    } catch (err) {
      globalError.value = extractErrors(err)[0].message
    }
  }

  async function reset() {
    if (!workflow.value) return
    loading.value = true
    globalError.value = ''
    try {
      const { data } = await axios.put(`/api/workflow/${workflow.value.id}/reset`)
      applySnapshot(data)
    } catch (err) {
      globalError.value = extractErrors(err)[0].message
    } finally {
      loading.value = false
    }
  }

  function disconnectWS() {
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
    ws?.close()
    ws = null
  }

  return {
    loading, workflow, execution, wsConnected, workers, strategy,
    fieldErrors, globalError,
    connectWS, restoreLast, createWorkflow, run, stop, reset, disconnectWS,
  }
})
