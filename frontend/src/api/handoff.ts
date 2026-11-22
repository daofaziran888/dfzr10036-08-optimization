import { request } from './client'

const BASE = '/api/compaction-handoff'

export type HandoffBatch = {
  id: number
  批次编号: string
  任务编号: string
  输入指纹: string
  目标表: string
  基准快照: string
  计划文件: string[]
  实际替换文件: string[]
  替换文件数: number
  统计状态: string
  预演状态: string
  重送次数: number
  创建时间: string
}

export type Rehearsal = {
  id: number
  预演编号: string
  批次id: number
  批次编号: string
  任务编号: string
  目标表: string
  基准快照: string
  可复核路径: string[]
  快照数: number
  领取状态: string
  领取人: string | null
  领取时间: string | null
  重排次数: number
  创建时间: string
}

export type TableStat = {
  目标表: string
  累计替换文件数: number
  已确认批次数: number
  待领取预演数: number
}

export type Workbench = {
  table_stats: TableStat[]
  batches: HandoffBatch[]
  rehearsals: Rehearsal[]
  progress: Record<string, unknown>[]
}

export type ClaimResult = {
  ok: boolean
  message: string
  rehearsal: Rehearsal
  batch: HandoffBatch
  other_claimable: Rehearsal[]
}

async function post<T>(path: string, body: unknown = {}): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  return (await response.json()) as T
}

export const handoffApi = {
  workbench: () => request(`${BASE}/workbench`).then((r) => r.json()) as Promise<Workbench>,
  recover: () => request(`${BASE}/recover`).then((r) => r.json()),
  ensureRehearsal: (batchId: number) => post(`${BASE}/batches/${batchId}/rehearsal`),
  claim: (rehearsalId: number, operator = '值班员') =>
    post<ClaimResult>(`${BASE}/rehearsals/${rehearsalId}/claim`, { operator }),
  reschedule: (rehearsalId: number) => post(`${BASE}/rehearsals/${rehearsalId}/reschedule`),
}
