export type NodeStatus = 'PENDING' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'TIMEOUT' | 'BLOCKED'
export type WorkflowStatus = 'PENDING' | 'RUNNING' | 'FINISHED'

export interface CycleWarning {
  type: 'CYCLE'
  cycles: string[][]
  blockedNodeIds: string[]
  message: string
}

export interface TaskNode {
  id: string
  name: string
  deps: string[]
  x: number
  y: number
  status: NodeStatus
  startTime?: number | null
  endTime?: number | null
  retries: number
  duration?: number
  priority?: number
}

export interface DAGWorkflow {
  id: number
  name: string
  nodes: TaskNode[]
  edges: [string, string][]
  status: WorkflowStatus
  workers: number
  strategy: string
  warnings: CycleWarning[]
  cycleNodeIds: string[]
  blockedNodeIds: string[]
}

export interface ExecutionLog { taskId: string; status: string; timestamp: number; message: string }
export interface CircuitBreaker { taskId: string; failureCount: number; state: string; cooldownUntil: number }
export interface ExecutionInfo { workflow: DAGWorkflow; logs: ExecutionLog[]; circuitBreakers: CircuitBreaker[]; completed: boolean }

export interface FieldError { field: string; message: string }
