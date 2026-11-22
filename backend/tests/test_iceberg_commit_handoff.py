"""湖仓提交 -> 合并批次的幂等回归：重入不累加文件数量、不重复统计。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.handoff import handoff_service
from app.services.orchestrator import flow_cdc_to_lake
from app.store import store

ORDER_TABLE = "ads.order_events_iceberg"


@pytest.fixture()
def client() -> TestClient:
    store._tables = {name: [dict(row) for row in rows] for name, rows in store._tables.items()}
    for name in ("compaction_batch", "expire_rehearsal", "metric_progress"):
        store._tables.pop(name, None)
    return TestClient(app)


def test_iceberg_commit_trigger_is_idempotent(client: TestClient) -> None:
    from app.services.iceberg_commit import IcebergCommitService

    service = IcebergCommitService()
    before_file_count = store.find("compaction", 1)["文件数量"]

    first = service.trigger_compaction_and_expire(1)
    second = service.trigger_compaction_and_expire(1)

    assert first is not None and second is not None
    # 合并任务文件数量不再被累加
    assert store.find("compaction", 1)["文件数量"] == before_file_count
    # 两次触发对应同一批次，只有一份预演、一条统计受理
    batches = [b for b in handoff_service.list_batches() if b["目标表"] == ORDER_TABLE]
    assert len(batches) == 1
    assert len(handoff_service.list_rehearsals()) == 1
    bench = handoff_service.workbench()
    assert len(bench["progress"]) == 1
    assert bench["table_stats"][0]["累计替换文件数"] == 1

    metric = next(
        row for row in store.rows("metric_aggr")
        if row.get("聚合键") == "合并批次确认" and row.get("数据流") == ORDER_TABLE
    )
    assert metric["累计替换文件数"] == 1


def test_main_orchestrator_flow_still_works(client: TestClient) -> None:
    result = flow_cdc_to_lake(1)
    assert result["ok"] is True
    assert result["call_count"] >= 1

    # 主编排流新建的提交默认是「提交中」，按既有规则不触发合并；
    # 将其转为「已提交」后执行编排流第 7-8 步（同一服务函数），
    # 末端应落地合并批次确认记录。
    from app.services.iceberg_commit import IcebergCommitService

    new_commit = max(store.rows("iceberg_commit"), key=lambda row: int(row["id"]))
    new_commit["status"] = "已提交"
    linked = IcebergCommitService().trigger_compaction_and_expire(int(new_commit["id"]))
    assert linked is not None and linked["batch"] is not None

    # 整条编排流再次运行不受改造影响、不产生重复批次
    again = flow_cdc_to_lake(1)
    assert again["ok"] is True

    assert handoff_service.list_batches(), "主编排流应产生合并批次确认记录"
    bench = handoff_service.workbench()
    assert bench["table_stats"], "流聚合应出现按批次累计的替换文件数"
    assert bench["progress"], "统计受理应已持久化"
