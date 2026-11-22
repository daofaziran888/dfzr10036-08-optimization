<template>
  <section class="page" data-module="compaction-handoff">
    <header class="page-head">
      <div>
        <h2>合并批次交接工作台</h2>
        <p class="page-desc">
          按已完成合并批次持久化确认记录：统计按实际替换文件数每批只记一次，重送幂等不扩大范围；
          清理预演一批一份，中断可恢复、未领取可重排、已领取范围冻结。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新工作台</button>
      </div>
    </header>

    <p v-if="message" class="status-legend" data-testid="message">{{ message }}</p>

    <div class="stat-row">
      <article v-for="item in workbench.table_stats" :key="item.目标表" class="stat-card">
        <span class="stat-label">{{ item.目标表 }} · 累计替换文件数</span>
        <strong class="stat-value" data-testid="cumulative">{{ item.累计替换文件数 }}</strong>
        <span class="stat-label">已确认批次 {{ item.已确认批次数 }} · 待领取预演 {{ item.待领取预演数 }}</span>
      </article>
      <article v-if="!workbench.table_stats.length" class="stat-card">
        <span class="stat-label">暂无已确认合并批次</span>
      </article>
    </div>

    <h3>合并批次确认记录</h3>
    <table class="data-table" data-testid="batch-table">
      <thead>
        <tr>
          <th>批次编号</th><th>任务编号</th><th>输入指纹</th><th>目标表</th><th>基准快照</th>
          <th>计划文件</th><th>实际替换文件</th><th>替换数</th><th>统计状态</th><th>预演状态</th><th>重送</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in workbench.batches" :key="batch.id" :data-testid="`batch-${batch.id}`">
          <td>{{ batch.批次编号 }}</td>
          <td>{{ batch.任务编号 }}</td>
          <td>{{ batch.输入指纹 }}</td>
          <td>{{ batch.目标表 }}</td>
          <td>{{ batch.基准快照 }}</td>
          <td>{{ batch.计划文件.join('、') }}</td>
          <td data-testid="actual-files">{{ batch.实际替换文件.join('、') }}</td>
          <td>{{ batch.替换文件数 }}</td>
          <td>{{ batch.统计状态 }}</td>
          <td>{{ batch.预演状态 }}</td>
          <td>{{ batch.重送次数 }}</td>
          <td class="row-actions">
            <button
              v-if="batch.预演状态 === '未生成'"
              class="link"
              type="button"
              :data-testid="`recover-rehearsal-${batch.id}`"
              @click="recoverRehearsal(batch.id)"
            >
              恢复预演
            </button>
            <span v-else>已交接</span>
          </td>
        </tr>
        <tr v-if="!workbench.batches.length">
          <td colspan="12" class="empty-state">暂无批次确认记录</td>
        </tr>
      </tbody>
    </table>

    <h3>清理预演领取队列</h3>
    <table class="data-table" data-testid="rehearsal-table">
      <thead>
        <tr>
          <th>预演编号</th><th>所属批次</th><th>任务编号</th><th>目标表</th><th>基准快照</th>
          <th>本次可复核路径</th><th>快照数</th><th>领取状态</th><th>领取人</th><th>重排</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="rehearsal in workbench.rehearsals" :key="rehearsal.id"
            :data-testid="`rehearsal-${rehearsal.id}`" :data-status="rehearsal.领取状态">
          <td>{{ rehearsal.预演编号 }}</td>
          <td>{{ rehearsal.批次编号 }}</td>
          <td>{{ rehearsal.任务编号 }}</td>
          <td>{{ rehearsal.目标表 }}</td>
          <td>{{ rehearsal.基准快照 }}</td>
          <td data-testid="review-paths">{{ rehearsal.可复核路径.join('、') }}</td>
          <td>{{ rehearsal.快照数 }}</td>
          <td>{{ rehearsal.领取状态 }}</td>
          <td>{{ rehearsal.领取人 ?? '—' }}</td>
          <td>{{ rehearsal.重排次数 }}</td>
          <td class="row-actions">
            <button
              v-if="rehearsal.领取状态 === '待领取'"
              class="link"
              type="button"
              :data-testid="`claim-${rehearsal.id}`"
              @click="claim(rehearsal.id)"
            >
              领取
            </button>
            <button
              v-if="rehearsal.领取状态 === '待领取'"
              class="link"
              type="button"
              :data-testid="`reschedule-${rehearsal.id}`"
              @click="reschedule(rehearsal.id)"
            >
              重新安排
            </button>
            <span v-else>已冻结</span>
          </td>
        </tr>
        <tr v-if="!workbench.rehearsals.length">
          <td colspan="11" class="empty-state">暂无清理预演</td>
        </tr>
      </tbody>
    </table>

    <section v-if="lastClaim" class="modal" data-testid="claim-detail">
      <h3>领取详情 · {{ lastClaim.rehearsal.预演编号 }}</h3>
      <p>所属批次：{{ lastClaim.batch.批次编号 }}（任务 {{ lastClaim.batch.任务编号 }}）</p>
      <p>本次复核路径：{{ lastClaim.rehearsal.可复核路径.join('、') }}</p>
      <p>
        还可领取：
        <template v-if="lastClaim.other_claimable.length">
          <span v-for="item in lastClaim.other_claimable" :key="item.预演编号">
            {{ item.预演编号 }}（{{ item.批次编号 }}）
          </span>
        </template>
        <span v-else>无</span>
      </p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { handoffApi, type ClaimResult, type Workbench } from '@/api/handoff'

const empty: Workbench = { table_stats: [], batches: [], rehearsals: [], progress: [] }
const workbench = ref<Workbench>(empty)
const message = ref('')
const lastClaim = ref<ClaimResult | null>(null)

async function reload() {
  workbench.value = await handoffApi.workbench()
}

async function recoverRehearsal(batchId: number) {
  const result = await handoffApi.ensureRehearsal(batchId) as { ok: boolean; message: string }
  message.value = result.message
  await reload()
}

async function claim(rehearsalId: number) {
  lastClaim.value = await handoffApi.claim(rehearsalId)
  message.value = lastClaim.value.message
  await reload()
}

async function reschedule(rehearsalId: number) {
  const result = await handoffApi.reschedule(rehearsalId) as { ok: boolean; message: string }
  message.value = result.message
  await reload()
}

onMounted(reload)
</script>
