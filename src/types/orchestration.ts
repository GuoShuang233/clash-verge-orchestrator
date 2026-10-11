export type GroupMode = 'upstream' | 'hybrid' | 'custom'

export interface OrchestrationNodeRef {
  source_id: string
  node_id: string
  name: string
}

interface OrchestrationGroupBinding {
  mode?: GroupMode
  upstream_name?: string
  nodes: OrchestrationNodeRef[]
  custom?: Record<string, unknown> | null
}

export interface OrchestrationSource {
  source_id: string
  name: string
  url: string
  enabled: boolean
  user_agent?: string
}

export interface OrchestrationState {
  sources: OrchestrationSource[]
  active_mode: GroupMode
  default_mode: GroupMode
  groups: Record<string, OrchestrationGroupBinding>
}

export interface OrchestrationNodeSummary {
  source_id: string
  node_id: string
  name: string
  proxy_type: string
}
