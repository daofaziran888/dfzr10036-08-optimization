"""重演交接：乙先被领取后再走一遍交接，已领范围保持原样、不生成重复预演。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store

ORDER_TABLE = "ads.order_events_iceberg"
TASK_ID = 1


@pytest.fixture()
def client() -> TestClient:
    store._tables = {name: [dict(row) for row in rows] for name, rows in store._tables.items()}
    for name in ("compaction_batch", "expire_rehearsal", "metric_progress"):
        store._tables.pop(name, None)
    return TestClient(app)


def _submit(client: TestClient, *, baseline: str, planned: list[str], actual: list[str],
            interrupted: bool = False) -> dict:
    response = client.post(
        f"/api/compaction-handoff/tasks/{TASK_ID}/batches",
        json={"基准快照": baseline, "计划文件": planned, "实际替换文件": actual,
              "interrupted": interrupted},
    )
    return response.json()


def test_replay_after_yi_claimed_keeps_claimed_scope(client: TestClient) -> None:
    # 甲中断、乙正常完成预演
    jia = _submit(client, baseline="snap-jia", planned=["a", "b", "c"], actual=["a", "b"],
                  interrupted=True)
    yi = _submit(client, baseline="snap-yi", planned=["d"], actual=["d"])
    yi_rehearsal_id = yi["rehearsal"]["id"]

    # 值班员先领取乙
    claimed = client.post(
        f"/api/compaction-handoff/rehearsals/{yi_rehearsal_id}/claim",
        json={"operator": "值班员-乙"},
    ).json()
    assert claimed["ok"] is True
    claimed_paths = list(claimed["rehearsal"]["可复核路径"])
    assert claimed_paths == [f"{ORDER_TABLE}/data/d.parquet"]

    # 重演这次交接：甲重送 + 恢复预演；乙再提交（同指纹重送）
    jia_again = _submit(client, baseline="snap-jia", planned=["a", "b", "c"], actual=["a", "b"])
    assert jia_again["resubmitted"] is True
    assert jia_again["rehearsal"] is None  # 重送不补预演
    yi_again = _submit(client, baseline="snap-yi", planned=["d"], actual=["d"])
    assert yi_again["resubmitted"] is True

    client.post(f"/api/compaction-handoff/batches/{jia['batch']['id']}/rehearsal")

    # 乙的预演仍只有一份，已领范围与领取人原样保留
    rehearsals = client.get("/api/compaction-handoff/rehearsals").json()["items"]
    yi_rows = [r for r in rehearsals if r["批次编号"] == yi["batch"]["批次编号"]]
    assert len(yi_rows) == 1
    assert yi_rows[0]["领取状态"] == "已领取"
    assert yi_rows[0]["领取人"] == "值班员-乙"
    assert yi_rows[0]["可复核路径"] == claimed_paths

    # 已领取预演拒绝重排
    blocked = client.post(
        f"/api/compaction-handoff/rehearsals/{yi_rehearsal_id}/reschedule").json()
    assert blocked["ok"] is False

    # 工作台：累计仍为 3（甲2+乙1），待领取只剩甲
    bench = client.get("/api/compaction-handoff/workbench").json()
    order_stat = next(s for s in bench["table_stats"] if s["目标表"] == ORDER_TABLE)
    assert order_stat["累计替换文件数"] == 3
    assert order_stat["待领取预演数"] == 1
    waiting_batch = bench["rehearsals"]
    assert [r["批次编号"] for r in waiting_batch if r["领取状态"] == "待领取"] == [
        jia["batch"]["批次编号"]
    ]
