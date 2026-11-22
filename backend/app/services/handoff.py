"""合并批次交接领域：按已完成的合并批次持久化确认记录、受理进度与清理预演。

设计目标（对应小文件合并重送/预演交接场景）：

1. 合并结果按「合并批次确认记录」(compaction_batch) 持久化，批次可追溯到
   目标表、基准快照、计划文件与实际替换文件；同一合并任务编号重送时，用
   输入指纹区分不同输入，重送走幂等通道，不扩大已确认范围。
2. 统计受理与预演交接是两条独立进度（metric_progress / expire_rehearsal），
   分别持久化；统计按实际替换文件数每批只记一次，中断后可从未受理的一侧恢复。
3. 清理预演一条批次只生成一份（可恢复、可重新安排）；每条可复核路径都归属
   原批次。尚未领取的预演可重排，已领取的范围冻结不变。
4. 流聚合按批次确认记录累计实际替换文件数；快照过期据此生成可领取的清理预演，
   不再由合并任务的「文件数量」推算快照数。

内存仓库在本项目里承担数据库职责，因此所有状态都落为 store 里的独立行，
跨请求、跨重送、跨中断都能恢复关联。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.store import store

BATCH_TABLE = "compaction_batch"
REHEARSAL_TABLE = "expire_rehearsal"
PROGRESS_TABLE = "metric_progress"

STAT_PENDING = "待确认"
STAT_CONFIRMED = "已确认"

REHEARSAL_NONE = "未生成"
REHEARSAL_WAITING = "待交接"
REHEARSAL_CLAIMED = "已领取"

CLAIM_WAITING = "待领取"
CLAIM_CLAIMED = "已领取"


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _next_id(table: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1


def _storage_path(table: str, file_name: str) -> str:
    return f"{table}/data/{file_name}.parquet"


class CompactionHandoffService:
    """合并批次确认与预演交接的持久化服务。"""

    # ---- 批次确认记录 ----

    def input_fingerprint(
        self,
        task_code: str,
        target_table: str,
        baseline_snapshot: str,
        planned_files: list[str],
        actual_files: list[str],
    ) -> str:
        """同一任务编号下区分不同输入的指纹：计划集合或实际集合不同即视为不同批次。"""
        payload = json.dumps(
            {
                "task": task_code,
                "table": target_table,
                "baseline": baseline_snapshot,
                "planned": sorted(planned_files),
                "actual": sorted(actual_files),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]

    def list_batches(self) -> list[dict[str, Any]]:
        return list(store.rows(BATCH_TABLE))

    def find_batch(self, batch_id: int) -> dict[str, Any] | None:
        return store.find(BATCH_TABLE, batch_id)

    def find_by_fingerprint(self, fingerprint: str) -> dict[str, Any] | None:
        for row in store.rows(BATCH_TABLE):
            if row.get("输入指纹") == fingerprint:
                return row
        return None

    def submit_batch(self, values: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """登记或重送一个合并批次，返回 (批次记录, 是否新建)。

        重送（指纹命中已存在批次）走幂等通道：仅累计重送次数，不修改实际替换
        范围，也不重复生成统计/预演记录，保证重送不能扩大已确认范围。
        """
        task_code = str(values.get("任务编号", "")).strip()
        target_table = str(values.get("目标表", "")).strip()
        baseline = str(values.get("基准快照", "")).strip()
        planned = [str(item).strip() for item in values.get("计划文件", []) if str(item).strip()]
        actual = [str(item).strip() for item in values.get("实际替换文件", []) if str(item).strip()]
        if not task_code or not target_table or not baseline:
            raise ValueError("任务编号、目标表、基准快照为必填项")
        if not actual:
            raise ValueError("实际替换文件不能为空，批次按实际替换数受理")

        fingerprint = self.input_fingerprint(task_code, target_table, baseline, planned, actual)
        existing = self.find_by_fingerprint(fingerprint)
        if existing is not None:
            existing["重送次数"] = int(existing.get("重送次数", 0)) + 1
            return existing, False

        batch_id = _next_id(BATCH_TABLE)
        batch = {
            "id": batch_id,
            "批次编号": f"CB-{batch_id:04d}",
            "任务编号": task_code,
            "输入指纹": fingerprint,
            "目标表": target_table,
            "基准快照": baseline,
            "计划文件": planned,
            "实际替换文件": actual,
            "替换文件数": len(actual),
            "统计状态": STAT_PENDING,
            "预演状态": REHEARSAL_NONE,
            "重送次数": 0,
            "status": STAT_PENDING,
            "pending": True,
            "abnormal": False,
            "创建时间": _now(),
        }
        store.rows(BATCH_TABLE).append(batch)
        return batch, True

    # ---- 统计受理（进度独立持久化，每批一次） ----

    def confirm_statistics(self, batch_id: int) -> dict[str, Any]:
        """确认一笔批次的统计受理；幂等，每批按实际替换数只记一次。"""
        batch = self.find_batch(batch_id)
        if batch is None:
            raise ValueError(f"合并批次 {batch_id} 不存在")
        if batch["统计状态"] == STAT_CONFIRMED:
            # 幂等：已确认的统计重复受理直接忽略，不重复累计。
            return {
                "batch": batch,
                "progress": self._progress_of_batch(batch["批次编号"]),
                "replayed": True,
            }

        batch["统计状态"] = STAT_CONFIRMED
        batch["status"] = STAT_CONFIRMED
        progress_rows = store.rows(PROGRESS_TABLE)
        progress_id = _next_id(PROGRESS_TABLE)
        progress = {
            "id": progress_id,
            "受理编号": f"ST-{progress_id:04d}",
            "批次编号": batch["批次编号"],
            "任务编号": batch["任务编号"],
            "目标表": batch["目标表"],
            "本批替换文件数": batch["替换文件数"],
            "累计替换文件数": self.cumulative_replaced(batch["目标表"]),
            "受理时间": _now(),
            "status": "已受理",
            "pending": False,
            "abnormal": False,
        }
        progress_rows.append(progress)
        batch["pending"] = batch["预演状态"] == REHEARSAL_NONE
        metric = self.upsert_metric(batch["目标表"])
        return {"batch": batch, "progress": progress, "metric": metric, "replayed": False}

    def _progress_of_batch(self, batch_code: str) -> dict[str, Any] | None:
        for row in store.rows(PROGRESS_TABLE):
            if row.get("批次编号") == batch_code:
                return row
        return None

    def cumulative_replaced(self, target_table: str) -> int:
        """流聚合口径：只累计已确认批次的实际替换文件数，每批恰好一次。"""
        return sum(
            int(row.get("替换文件数", 0))
            for row in store.rows(BATCH_TABLE)
            if row.get("目标表") == target_table and row.get("统计状态") == STAT_CONFIRMED
        )

    def upsert_metric(self, target_table: str) -> dict[str, Any]:
        """按目标表维护一条合并批次聚合窗口，值来自批次确认记录，重送不重复累计。"""
        metric_rows = store.rows("metric_aggr")
        for row in metric_rows:
            if row.get("数据流") == target_table and row.get("聚合键") == "合并批次确认":
                row["累计替换文件数"] = self.cumulative_replaced(target_table)
                row["最后写入"] = _now()
                return row

        metric_id = _next_id("metric_aggr")
        metric = {
            "id": metric_id,
            "窗口编号": f"MAG-CMP-{metric_id:04d}",
            "数据流": target_table,
            "聚合键": "合并批次确认",
            "聚合函数": "SUM(替换文件数)",
            "窗口宽度": "按合并批次",
            "输出频率": "每批统计确认一次",
            "延迟更新": "实时",
            "最后写入": _now(),
            "累计替换文件数": self.cumulative_replaced(target_table),
            "status": "聚合中",
            "pending": True,
            "abnormal": False,
        }
        metric_rows.append(metric)
        return metric

    # ---- 清理预演（一批一份，可恢复/可重排，领取后冻结） ----

    def list_rehearsals(self) -> list[dict[str, Any]]:
        return list(store.rows(REHEARSAL_TABLE))

    def find_rehearsal(self, rehearsal_id: int) -> dict[str, Any] | None:
        return store.find(REHEARSAL_TABLE, rehearsal_id)

    def _rehearsal_of_batch(self, batch_id: int) -> dict[str, Any] | None:
        for row in store.rows(REHEARSAL_TABLE):
            if int(row.get("批次id", 0)) == batch_id:
                return row
        return None

    def ensure_rehearsal(self, batch_id: int) -> dict[str, Any]:
        """生成或恢复批次的清理预演；重复调用返回同一份，不产生重复预演。"""
        batch = self.find_batch(batch_id)
        if batch is None:
            raise ValueError(f"合并批次 {batch_id} 不存在")
        if batch["统计状态"] != STAT_CONFIRMED:
            raise ValueError("统计尚未确认，不能交接清理预演")

        existing = self._rehearsal_of_batch(batch_id)
        if existing is not None:
            return existing

        actual_files = list(batch["实际替换文件"])
        rehearsal_id = _next_id(REHEARSAL_TABLE)
        rehearsal = {
            "id": rehearsal_id,
            "预演编号": f"RV-{rehearsal_id:04d}",
            "批次id": batch_id,
            "批次编号": batch["批次编号"],
            "任务编号": batch["任务编号"],
            "目标表": batch["目标表"],
            "基准快照": batch["基准快照"],
            "可复核路径": [_storage_path(batch["目标表"], name) for name in actual_files],
            "快照数": len(actual_files),
            "领取状态": CLAIM_WAITING,
            "领取人": None,
            "领取时间": None,
            "重排次数": 0,
            "status": CLAIM_WAITING,
            "pending": True,
            "abnormal": False,
            "创建时间": _now(),
        }
        store.rows(REHEARSAL_TABLE).append(rehearsal)
        batch["预演状态"] = REHEARSAL_WAITING
        batch["pending"] = True
        self._sync_expire_policy(batch["目标表"])
        return rehearsal

    def claim_rehearsal(self, rehearsal_id: int, operator: str = "值班员") -> dict[str, Any]:
        """领取一份预演：返回本次复核路径、所属批次与其他仍可领取的预演。

        已领取的预演重复领取时原样返回，已领范围保持不变。
        """
        rehearsal = self.find_rehearsal(rehearsal_id)
        if rehearsal is None:
            raise ValueError(f"清理预演 {rehearsal_id} 不存在")

        if rehearsal["领取状态"] != CLAIM_CLAIMED:
            rehearsal["领取状态"] = CLAIM_CLAIMED
            rehearsal["status"] = CLAIM_CLAIMED
            rehearsal["pending"] = False
            rehearsal["领取人"] = operator
            rehearsal["领取时间"] = _now()
            batch = self.find_batch(int(rehearsal["批次id"]))
            if batch is not None:
                batch["预演状态"] = REHEARSAL_CLAIMED
                batch["pending"] = False
            self._sync_expire_policy(rehearsal["目标表"])

        other_claimable = [
            {
                "预演编号": row["预演编号"],
                "批次编号": row["批次编号"],
                "目标表": row["目标表"],
                "快照数": row["快照数"],
                "可复核路径": row["可复核路径"],
            }
            for row in store.rows(REHEARSAL_TABLE)
            if row["id"] != rehearsal_id and row["领取状态"] == CLAIM_WAITING
        ]
        return {
            "rehearsal": rehearsal,
            "batch": self.find_batch(int(rehearsal["批次id"])),
            "other_claimable": other_claimable,
        }

    def reschedule_rehearsal(self, rehearsal_id: int) -> dict[str, Any]:
        """尚未领取的预演可以重新安排；已领取的范围冻结，拒绝重排。"""
        rehearsal = self.find_rehearsal(rehearsal_id)
        if rehearsal is None:
            raise ValueError(f"清理预演 {rehearsal_id} 不存在")
        if rehearsal["领取状态"] == CLAIM_CLAIMED:
            raise ValueError("预演已领取，已领范围保持原样，不能重新安排")
        rehearsal["重排次数"] = int(rehearsal.get("重排次数", 0)) + 1
        rehearsal["创建时间"] = _now()
        return rehearsal

    # ---- 恢复与工作台 ----

    def recover_pending(self) -> dict[str, Any]:
        """中断恢复：列出仍需交付的受理侧，已确认的统计/预演仍关联原批次。"""
        waiting_rehearsal_batches = [
            {
                "批次编号": row["批次编号"],
                "任务编号": row["任务编号"],
                "目标表": row["目标表"],
                "基准快照": row["基准快照"],
                "待办": "生成清理预演",
            }
            for row in store.rows(BATCH_TABLE)
            if row["统计状态"] == STAT_CONFIRMED and row["预演状态"] == REHEARSAL_NONE
        ]
        unclaimed = [
            {
                "预演编号": row["预演编号"],
                "批次编号": row["批次编号"],
                "目标表": row["目标表"],
                "待办": "值班员领取预演",
            }
            for row in store.rows(REHEARSAL_TABLE)
            if row["领取状态"] == CLAIM_WAITING
        ]
        return {
            "预演待生成": waiting_rehearsal_batches,
            "预演待领取": unclaimed,
        }

    def workbench(self) -> dict[str, Any]:
        """统计与预演工作台：累计替换数、批次确认记录、可领取预演队列。"""
        tables = sorted({row["目标表"] for row in store.rows(BATCH_TABLE)})
        table_stats = [
            {
                "目标表": table,
                "累计替换文件数": self.cumulative_replaced(table),
                "已确认批次数": sum(
                    1
                    for row in store.rows(BATCH_TABLE)
                    if row.get("目标表") == table and row["统计状态"] == STAT_CONFIRMED
                ),
                "待领取预演数": sum(
                    1
                    for row in store.rows(REHEARSAL_TABLE)
                    if row.get("目标表") == table and row["领取状态"] == CLAIM_WAITING
                ),
            }
            for table in tables
        ]
        return {
            "table_stats": table_stats,
            "batches": self.list_batches(),
            "rehearsals": self.list_rehearsals(),
            "progress": list(store.rows(PROGRESS_TABLE)),
        }

    def _sync_expire_policy(self, target_table: str) -> None:
        """把交接结果回写到快照过期策略：预演口径来自批次确认记录而非文件数量。"""
        for policy in store.rows("expire_snap"):
            if policy.get("目标表") != target_table:
                continue
            unclaimed = [
                row
                for row in store.rows(REHEARSAL_TABLE)
                if row.get("目标表") == target_table and row["领取状态"] == CLAIM_WAITING
            ]
            cumulative = self.cumulative_replaced(target_table)
            if unclaimed:
                policy["status"] = "预演完成"
                policy["pending"] = True
                policy["预演结果"] = (
                    f"按{len(unclaimed)}份批次预演可清理{cumulative}个历史快照"
                    f"（按实际替换文件累计）"
                )
            else:
                policy["pending"] = False
                policy["预演结果"] = f"已领取清理 {cumulative} 个历史快照对应的替换文件"
            break


handoff_service = CompactionHandoffService()
