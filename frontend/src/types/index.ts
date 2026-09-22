export type WorkflowStatus = 'QUEUED' | 'RUNNING' | 'FINISHED'
export type TaskStatus = 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'TIMEOUT' | 'BLOCKED'
export type Strategy = 'fifo' | 'priority' | 'max_concurrent'

export interface WorkflowWarning {
  type: 'CYCLE'
  nodeIds: string[]
  message: string
}

export interface TaskNode {
  id: string
  name: string
  deps: string[]
  duration?: number
  priority?: number
  x: number
  y: number
  status: TaskStatus
  startTime?: number | null
  endTime?: number | null
  retries: number
  blockedByCycle?: boolean
  blockedReason?: string | null
}

export interface ExecutionConfig {
  workers: number
  strategy: Strategy
}

export interface DAGWorkflow {
  id: number
  name: string
  status: WorkflowStatus
  nodes: TaskNode[]
  edges: [string, string][]
  warnings?: WorkflowWarning[]
  executionConfig: ExecutionConfig
  createdAt?: number
  updatedAt?: number
}

export interface ExecutionLog {
  taskId: string
  status: string
  timestamp: number
  message: string
}

export interface CircuitBreaker {
  taskId: string
  failureCount: number
  state: string
  cooldownUntil: number
}

export interface ExecutionInfo {
  id: number
  workflowId: number
  workflow: DAGWorkflow
  logs: ExecutionLog[]
  circuitBreakers: CircuitBreaker[]
  completed: boolean
  interrupted: boolean
  status: WorkflowStatus
  warnings?: WorkflowWarning[]
  executionConfig: ExecutionConfig
}

export interface WorkflowDraftNode {
  id: string
  name: string
  deps: string[]
  duration: number
  priority: number
  x?: number
  y?: number
}
