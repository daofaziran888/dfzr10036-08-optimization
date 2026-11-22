"""合并批次交接端到端场景：重送幂等、中断恢复、同表预演冲突与领取冻结。

剧情（对应需求描述）：
- 订单表上两笔独立合并：甲计划替换 a/b/c、实际只替换 a/b；乙替换 d。
- 甲的统计确认成功但预演交接中断；乙先完成预演且尚未被领取。
- 随后甲重送两次：同一合并任务编号、不同输入靠指纹区分；重送不能扩大已确认
  范围，统计按实际替换数每批只记一次。
- 中断恢复后继续交付未受理的一侧（甲的预演），已确认的统计仍关联原批次。
- 同一张表上两份待交接预演并存且互不覆盖；领取一份可查到本次路径、所属批次
  以及另一份可领取预演；未领取的可重排，已领取的范围冻结。
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import store

ORDER_TABLE = "ads.order_events_iceberg"
TASK_ID = 1  # 种子数据中的 CMP-0001，状态为已完成，目标表为订单表


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
        json={
            "基准快照": baseline,
            "计划文件": planned,
            "实际替换文件": actual,
            "interrupted": interrupted,
        },
    )
    assert response.status_code == 200
    return response.json()


def test_full_handoff_scenario(client: TestClient) -> None:
    # ---- 1. 甲：统计成功后中断（预演未交接） ----
    jia = _submit(
        client,
        baseline="snap-base-jia",
        planned=["a", "b", "c"],
        actual=["a", "b"],
        interrupted=True,
    )
    assert jia["ok"] is True
    jia_batch = jia["batch"]
    assert jia_batch["替换文件数"] == 2
    assert jia_batch["统计状态"] == "已确认"
    assert jia_batch["预演状态"] == "未生成"
    # 统计仍已持久化并关联原批次
    assert jia["progress"]["批次编号"] == jia_batch["批次编号"]
    assert jia["progress"]["本批替换文件数"] == 2
    assert jia["rehearsal"] is None

    # ---- 2. 乙：先完成预演，值班员尚未领取 ----
    yi = _submit(
        client,
        baseline="snap-base-yi",
        planned=["d"],
        actual=["d"],
        interrupted=False,
    )
    assert yi["ok"] is True
    yi_batch = yi["batch"]
    assert yi_batch["批次编号"] != jia_batch["批次编号"]
    assert yi_batch["预演状态"] == "待交接"
    assert yi["rehearsal"]["领取状态"] == "待领取"
    assert yi["rehearsal"]["可复核路径"] == [f"{ORDER_TABLE}/data/d.parquet"]

    # ---- 3. 甲重送两次：幂等，不扩大已确认范围、不重复统计 ----
    jia_resend_1 = _submit(
        client, baseline="snap-base-jia", planned=["a", "b", "c"], actual=["a", "b"],
        interrupted=True,
    )
    jia_resend_2 = _submit(
        client, baseline="snap-base-jia", planned=["a", "b", "c"], actual=["a", "b"],
        interrupted=False,
    )
    assert jia_resend_1["resubmitted"] is True
    assert jia_resend_2["resubmitted"] is True
    assert jia_resend_1["batch"]["id"] == jia_batch["id"]
    assert jia_resend_1["batch"]["重送次数"] == 1
    assert jia_resend_2["batch"]["重送次数"] == 2
    # 重送不改状态、不补预演、不新增统计
    assert jia_resend_2["batch"]["统计状态"] == "已确认"
    assert jia_resend_2["batch"]["预演状态"] == "未生成"

    batches = client.get("/api/compaction-handoff/batches").json()["items"]
    assert len(batches) == 2

    # 甲乙的统计均已确认，受理记录各一条；甲的两次重送不新增受理记录
    progress_rows = client.get("/api/compaction-handoff/workbench").json()["progress"]
    assert len(progress_rows) == 2
    assert {row["本批替换文件数"] for row in progress_rows} == {2, 1}

    # ---- 4. 同任务编号不同输入必须区分（指纹） ----
    other = _submit(
        client, baseline="snap-base-jia", planned=["a", "b", "c"], actual=["a"],
        interrupted=False,
    )
    assert other["created"] is True
    assert other["batch"]["输入指纹"] != jia_batch["输入指纹"]

    # ---- 5. 中断恢复：继续交付甲未受理的预演一侧 ----
    recover = client.get("/api/compaction-handoff/recover").json()
    pending_codes = {item["批次编号"] for item in recover["预演待生成"]}
    assert jia_batch["批次编号"] in pending_codes

    rehearsal_resp = client.post(f"/api/compaction-handoff/batches/{jia_batch['id']}/rehearsal")
    assert rehearsal_resp.json()["ok"] is True
    jia_rehearsal = rehearsal_resp.json()["rehearsal"]
    assert jia_rehearsal["批次编号"] == jia_batch["批次编号"]
    assert jia_rehearsal["基准快照"] == "snap-base-jia"
    assert jia_rehearsal["可复核路径"] == [
        f"{ORDER_TABLE}/data/a.parquet",
        f"{ORDER_TABLE}/data/b.parquet",
    ]
    # 重复恢复不产生第二份预演
    again = client.post(f"/api/compaction-handoff/batches/{jia_batch['id']}/rehearsal").json()
    assert again["rehearsal"]["id"] == jia_rehearsal["id"]

    # ---- 6. 同一张表两份待交接预演并存，互不覆盖 ----
    rehearsals = client.get("/api/compaction-handoff/rehearsals").json()["items"]
    waiting = [r for r in rehearsals if r["领取状态"] == "待领取"]
    jia_yi = [r for r in waiting if r["批次编号"] in {jia_batch["批次编号"], yi_batch["批次编号"]}]
    assert len(jia_yi) == 2
    assert {tuple(r["可复核路径"]) for r in jia_yi} == {
        (f"{ORDER_TABLE}/data/a.parquet", f"{ORDER_TABLE}/data/b.parquet"),
        (f"{ORDER_TABLE}/data/d.parquet",),
    }

    # ---- 7. 领取一份：本次路径、所属批次、另一份可领取预演 ----
    claim = client.post(f"/api/compaction-handoff/rehearsals/{jia_rehearsal['id']}/claim",
                        json={"operator": "值班员-甲"}).json()
    assert claim["ok"] is True
    assert claim["rehearsal"]["领取人"] == "值班员-甲"
    assert claim["rehearsal"]["可复核路径"] == jia_rehearsal["可复核路径"]
    assert claim["batch"]["批次编号"] == jia_batch["批次编号"]
    claimable_codes = {item["预演编号"] for item in claim["other_claimable"]}
    assert yi["rehearsal"]["预演编号"] in claimable_codes
    assert jia_rehearsal["预演编号"] not in claimable_codes

    # 重复领取保持原样
    claim_again = client.post(
        f"/api/compaction-handoff/rehearsals/{jia_rehearsal['id']}/claim").json()
    assert claim_again["rehearsal"]["领取人"] == "值班员-甲"
    assert claim_again["rehearsal"]["可复核路径"] == jia_rehearsal["可复核路径"]
    # 已领取不能重排
    blocked = client.post(
        f"/api/compaction-handoff/rehearsals/{jia_rehearsal['id']}/reschedule").json()
    assert blocked["ok"] is False

    # ---- 8. 未领取预演可重新安排 ----
    reschedule = client.post(
        f"/api/compaction-handoff/rehearsals/{yi['rehearsal']['id']}/reschedule").json()
    assert reschedule["ok"] is True
    assert reschedule["rehearsal"]["重排次数"] == 1

    # ---- 9. 刷新统计与预演工作台：累计替换数与剩余队列 ----
    bench = client.get("/api/compaction-handoff/workbench").json()
    order_stat = next(item for item in bench["table_stats"] if item["目标表"] == ORDER_TABLE)
    # 甲2 + 乙1 + 额外输入(a)1 = 4，重送不重复累计
    assert order_stat["累计替换文件数"] == 4
    assert order_stat["已确认批次数"] == 3
    # 乙与额外输入两份预演仍待领取
    assert order_stat["待领取预演数"] == 2

    # 流聚合窗口按批次确认记录累计
    metric = next(
        row for row in store.rows("metric_aggr")
        if row.get("聚合键") == "合并批次确认" and row.get("数据流") == ORDER_TABLE
    )
    assert metric["累计替换文件数"] == 4

    # 快照过期策略口径来自批次确认记录，而非文件数量推算
    policy = next(row for row in store.rows("expire_snap") if row.get("目标表") == ORDER_TABLE)
    assert "按实际替换文件累计" in policy["预演结果"]
    assert "4个历史快照" in policy["预演结果"]


def test_resubmit_never_expands_confirmed_scope(client: TestClient) -> None:
    """同指纹重送任意次数，累计替换文件数与受理记录数保持不变。"""
    first = _submit(client, baseline="snap-x", planned=["a", "b"], actual=["a"])
    for _ in range(3):
        resent = _submit(client, baseline="snap-x", planned=["a", "b"], actual=["a"])
        assert resent["resubmitted"] is True
        assert resent["batch"]["id"] == first["batch"]["id"]

    bench = client.get("/api/compaction-handoff/workbench").json()
    assert bench["table_stats"][0]["累计替换文件数"] == 1
    assert len(bench["progress"]) == 1
    assert bench["batches"][0]["重送次数"] == 3


def test_interrupted_batch_recovers_from_rehearsal_side(client: TestClient) -> None:
    """统计已确认、预演未交接的中断批次：恢复时只需补预演，统计不重复受理。"""
    jia = _submit(
        client, baseline="snap-base-jia", planned=["a", "b", "c"], actual=["a", "b"],
        interrupted=True,
    )
    batch_id = jia["batch"]["id"]

    recover = client.get("/api/compaction-handoff/recover").json()
    assert any(item["批次编号"] == jia["batch"]["批次编号"] for item in recover["预演待生成"])

    # 统计确认接口幂等：再次确认不会新增受理记录
    client.post(f"/api/compaction-handoff/batches/{batch_id}/statistics")
    rehearsal = client.post(f"/api/compaction-handoff/batches/{batch_id}/rehearsal").json()
    assert rehearsal["ok"] is True
    assert rehearsal["rehearsal"]["可复核路径"] == [
        f"{ORDER_TABLE}/data/a.parquet",
        f"{ORDER_TABLE}/data/b.parquet",
    ]
    bench = client.get("/api/compaction-handoff/workbench").json()
    assert len(bench["progress"]) == 1
    assert bench["table_stats"][0]["累计替换文件数"] == 2
