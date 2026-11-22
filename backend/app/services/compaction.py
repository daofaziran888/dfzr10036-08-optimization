"""小文件合并服务：合并完成后按合并批次确认记录驱动快照过期与流聚合。

历史实现的问题：
- 重送合并时直接给已有任务的「文件数量」累加，统计被重复累计；
- 过期预演按任务的文件数量 * 3 推算快照数，与实际替换文件无关；
- 同一张表上两笔合并的预演互相覆盖，交接中断无法恢复。

现在合并任务完成后只负责把「实际替换的文件」提交给 handoff 服务，由其
生成可恢复的合并批次确认记录，统计受理与预演交接分别持久化。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.handoff import REHEARSAL_NONE, STAT_CONFIRMED, handoff_service
from app.store import store


class CompactionService(BaseModuleService):
    MODULE = "compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "合并"
    LIST_FIELDS = ["任务编号", "目标表", "文件数量", "合并策略", "触发阈值", "执行内存", "合并耗时", "释放空间"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "文件数量"]
    STATUS_ORDER = ["待执行", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "合并中", "取消合并": "已完成", "标记失败": "合并失败"}
    NEGATIVE_ACTIONS = []

    def submit_compaction_batch(
        self,
        entry_id: int,
        payload: dict[str, Any],
        *,
        interrupted: bool = False,
    ) -> dict[str, Any]:
        """把一次已完成合并的实际替换文件按批次交接。

        payload 支持：基准快照、计划文件、实际替换文件（缺省时由文件数量生成
        占位文件名，仅用于演示）。interrupted=True 模拟「统计成功后交接中断」：
        统计确认照常持久化，但不生成清理预演，后续可恢复。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已完成":
            raise ValueError("合并任务不存在或尚未完成")

        target_table = str(entry.get("目标表", "unknown"))
        task_code = str(entry.get("任务编号", "unknown"))
        baseline = str(payload.get("基准快照") or f"snap-baseline-{entry_id}")
        planned = list(payload.get("计划文件") or [])
        actual = list(payload.get("实际替换文件") or [])
        if not planned:
            planned = [f"part-{idx:04d}" for idx in range(1, int(entry.get("文件数量", 0)) + 1)]
        if not actual:
            raise ValueError("必须提供实际替换文件，统计按实际替换数受理")

        batch, created = handoff_service.submit_batch(
            {
                "任务编号": task_code,
                "目标表": target_table,
                "基准快照": baseline,
                "计划文件": planned,
                "实际替换文件": actual,
            }
        )
        # 统计始终先确认并持久化（每批一次、幂等）。
        confirmation = handoff_service.confirm_statistics(batch["id"])

        # 重送是纯幂等通道：不新增统计、不补预演，中断批次只能走恢复接口继续交付。
        rehearsal = None
        if created and not interrupted:
            rehearsal = handoff_service.ensure_rehearsal(batch["id"])

        return {
            "batch": batch,
            "created": created,
            "resubmitted": not created,
            "progress": confirmation.get("progress"),
            "metric": confirmation.get("metric"),
            "rehearsal": rehearsal,
            "interrupted": interrupted,
        }

    def trigger_expire_and_metric(self, entry_id: int) -> dict[str, Any] | None:
        """兼容既有编排链路：从该任务关联的批次确认记录恢复 expire/metric 结果。

        不再按文件数量推算快照，也不再创建重复的聚合窗口；数据全部来自
        handoff 持久化的批次确认记录与清理预演。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已完成":
            return None

        table = str(entry.get("目标表", "unknown"))
        task_code = str(entry.get("任务编号", ""))
        results: dict[str, Any] = {"expire_snap": None, "metric_aggr": None}

        batches = [
            row
            for row in handoff_service.list_batches()
            if row.get("目标表") == table and (not task_code or row.get("任务编号") == task_code)
        ]
        if not batches:
            return None

        # 触发/恢复该任务各批次尚未交接的清理预演（已存在的保持幂等）。
        for batch in batches:
            if batch.get("统计状态") == STAT_CONFIRMED and batch.get("预演状态") == REHEARSAL_NONE:
                handoff_service.ensure_rehearsal(batch["id"])

        for policy in store.rows("expire_snap"):
            if str(policy.get("目标表", "")) == table:
                results["expire_snap"] = policy
                break

        metric = handoff_service.upsert_metric(table)
        results["metric_aggr"] = metric
        return results
