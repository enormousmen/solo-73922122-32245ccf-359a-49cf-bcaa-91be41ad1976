"""模拟器核心行为测试。

覆盖需求中的关键路径：
  1. 正常切换成功：新链报文等待旧链排空后才交付；
  2. 超时回退：旧链补发、新链迟到副本丢弃、应用仍按编号收齐；
  3. 新链 4 包窗口打满：只暂停迁移流，另一条流不受影响；
  4. 参数化不变量：任意参数组合下 in-order + exactly-once 恒成立；
  5. 迁移流 2 的对称性；
  6. Flask API 校验。
"""

import pytest

from app.app import app
from app.simulator import (
    DEFAULT_PACKETS,
    NEW_LINK_CAP,
    run_simulation,
)


# --------------------------------------------------------------- 工具
def delivered_seqs(result, sid):
    return [d["seq"] for d in result["delivery"][str(sid)]]


def assert_complete_in_order(result, n=DEFAULT_PACKETS):
    """每条流都按 1..n 交付且每包一次（核心正确性断言）。"""
    assert result["ok"] is True
    for sid in (1, 2):
        assert delivered_seqs(result, sid) == list(range(1, n + 1))
        # 每包仅一条交付记录
        seqs = delivered_seqs(result, sid)
        assert len(seqs) == len(set(seqs)) == n


# --------------------------------------------------------------- 1. 成功
def test_success_waits_for_old_link_drain():
    result = run_simulation(
        delay_l1_ms=200, delay_l2_ms=150,
        deadline_ms=1200, moving_stream=1)
    assert result["handover"]["outcome"] == "success"
    assert_complete_in_order(result)

    mv = result["streams"][0]
    assert mv["old_link"] == "L1" and mv["new_link"] == "L2"

    # 新链最早交付不得早于旧链最晚交付（旧链排空后才放行）
    d1 = result["delivery"]["1"]
    last_old = max(e["ts"] for e in d1 if e["link"] == "L1")
    first_new = min(e["ts"] for e in d1 if e["link"] == "L2")
    assert last_old <= first_new

    # 切换开始前，迁移流不存在新链发送（流 2 常驻 L2 的正常流量不算）
    start = result["config"]["handover_start_ms"]
    early_new = [e for e in result["events"]
                 if e["type"] == "send" and e.get("link") == "L2"
                 and e["stream"] == 1 and e["ts"] < start]
    assert early_new == []

    # 自检项全部通过
    assert all(c["ok"] for c in result["checks"])


# --------------------------------------------------------------- 2. 回退
def test_rollback_resends_and_discards_new_copies():
    result = run_simulation(
        delay_l1_ms=900, delay_l2_ms=100,
        deadline_ms=600, moving_stream=1)
    assert result["handover"]["outcome"] == "rollback"
    assert_complete_in_order(result)

    mv_delivery = result["delivery"]["1"]

    # 回退后应用收到的迁移流报文全部来自旧链 L1
    assert all(d["link"] == "L1" for d in mv_delivery)

    # 存在旧链补发记录，且补发编号最终都交付了
    resends = {e["seq"] for e in result["events"]
               if e["type"] == "send" and e.get("attempt") == "old-resend"}
    assert resends, "超时回退应当触发旧链补发"
    assert resends <= {d["seq"] for d in mv_delivery}

    # 被丢弃的全部是新链 L2 副本（含迟到副本/回退时缓存作废）
    assert result["discards"], "回退应当丢弃新链副本"
    assert all(d["link"] == "L2" for d in result["discards"])
    reasons = " ".join(d["reason"] for d in result["discards"])
    assert "新链" in reasons

    # 切换时限语义：结果落定时间 = 开始 + 时限（取整 tick）
    ho = result["handover"]
    expected_finish = ho["handover_start_ms"] + result["config"]["deadline_ms"]
    assert ho["finish_ms"] == expected_finish


def test_late_new_link_copy_after_rollback_is_discarded():
    """新链高延时：回退发生后仍有新链在途副本到达，必须丢弃且不影响交付。"""
    result = run_simulation(
        delay_l1_ms=800, delay_l2_ms=2000,
        deadline_ms=500, moving_stream=1)
    assert result["handover"]["outcome"] == "rollback"
    assert_complete_in_order(result)

    late = [d for d in result["discards"] if "迟到" in d["reason"]]
    assert late, "回退后到达的新链迟到副本应被丢弃"
    assert all(d["link"] == "L2" for d in late)


# --------------------------------------------------------------- 3. 窗口
def test_new_link_cap_pauses_only_moving_stream():
    result = run_simulation(
        delay_l1_ms=100, delay_l2_ms=1200,
        deadline_ms=3000, moving_stream=1)
    assert result["handover"]["outcome"] == "success"
    assert_complete_in_order(result)

    # 发生过窗口打满暂停
    blocked = [e for e in result["events"] if e["type"] == "send_blocked"]
    assert blocked
    assert all(e["stream"] == 1 for e in blocked)
    assert all(len(e["window"]) == NEW_LINK_CAP for e in blocked)

    # 重建窗口：任何时刻新链已发未交付不超过 4
    live = set()
    peak = 0
    for e in result["events"]:
        if e["type"] == "send" and e.get("attempt") == "new-trial":
            live.add(e["seq"])
        elif e["type"] in ("deliver", "packet_discard") and e.get("attempt") == "new-trial":
            live.discard(e["seq"])
        peak = max(peak, len(live))
    assert peak == NEW_LINK_CAP

    # 另一条流（流2 走 L2）全程未被暂停、未受影响且正常交付
    s2 = result["streams"][1]
    assert s2["delivered_count"] == DEFAULT_PACKETS
    assert not any(e.get("stream") == 2 and e["type"] == "send_blocked"
                   for e in result["events"])


def test_non_moving_stream_never_uses_other_link():
    """流1 迁移到 L2 时，流2 始终走自己的 L2，不会串链。"""
    result = run_simulation(200, 150, 1200, moving_stream=1)
    s2_sends = [e for e in result["events"] if e["type"] == "send" and e["stream"] == 2]
    assert s2_sends and all(e["link"] == "L2" for e in s2_sends)
    assert all(d["link"] == "L2" for d in result["delivery"]["2"])


# --------------------------------------------------------------- 4. 不变量
@pytest.mark.parametrize("d1,d2,deadline,sid", [
    (50, 50, 200, 1),
    (100, 600, 300, 1),
    (600, 100, 300, 1),
    (1500, 200, 800, 1),
    (200, 1500, 400, 1),
    (300, 300, 100, 1),
    (200, 150, 1200, 2),   # 迁移流 2：L2 → L1
    (900, 100, 600, 2),
])
def test_ordering_invariant_holds_for_all_params(d1, d2, deadline, sid):
    result = run_simulation(d1, d2, deadline, moving_stream=sid)
    assert_complete_in_order(result)
    # 每条事件时间戳单调不减，且在合理范围内终止
    tss = [e["ts"] for e in result["events"]]
    assert tss == sorted(tss)
    assert result["max_ts"] > 0


def test_small_stream_single_packet():
    result = run_simulation(200, 150, 1200, moving_stream=1, packets_per_stream=1)
    assert_complete_in_order(result, n=1)


# --------------------------------------------------------------- 5. 输入校验
def test_invalid_moving_stream():
    with pytest.raises(ValueError):
        run_simulation(200, 150, 1200, moving_stream=3)


def test_invalid_delay():
    with pytest.raises(ValueError):
        run_simulation(-1, 150, 1200, moving_stream=1)


# --------------------------------------------------------------- 6. API
@pytest.fixture()
def client():
    app.testing = True
    return app.test_client()


def test_api_health(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}


def test_api_simulate_success(client):
    resp = client.post("/api/simulate", json={
        "delay_l1_ms": 200, "delay_l2_ms": 150,
        "deadline_ms": 1200, "moving_stream": 1})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["handover"]["outcome"] == "success"
    assert data["ok"] is True


def test_api_simulate_rollback(client):
    resp = client.post("/api/simulate", json={
        "delay_l1_ms": 900, "delay_l2_ms": 100,
        "deadline_ms": 600, "moving_stream": 1})
    data = resp.get_json()
    assert resp.status_code == 200
    assert data["handover"]["outcome"] == "rollback"


def test_api_rejects_bad_params(client):
    resp = client.post("/api/simulate", json={
        "delay_l1_ms": 5, "delay_l2_ms": 150,
        "deadline_ms": 1200, "moving_stream": 1})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_api_rejects_bad_stream(client):
    resp = client.post("/api/simulate", json={
        "delay_l1_ms": 200, "delay_l2_ms": 150,
        "deadline_ms": 1200, "moving_stream": 9})
    assert resp.status_code == 400


def test_index_page_renders(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "链路切换演练".encode() in resp.data
