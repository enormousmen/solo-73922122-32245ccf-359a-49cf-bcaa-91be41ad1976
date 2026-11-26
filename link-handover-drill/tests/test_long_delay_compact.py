"""长时延 / 跳空刻度 / 紧凑时间线 的新增行为测试。

覆盖任务要求：
  * 链路延时与切换时限上限放宽到 60s（API 校验）；
  * 引擎跳过空刻度、窗口满后不逐 tick 试发，stats 给出实际处理刻度数；
  * 连续且窗口集合相同的受阻只记首末刻度（blocked_spans）；
  * 交付腾出窗口的当刻、超时回退的当刻，已到发送时机的报文立即继续发；
  * 另一条流在长受阻期间照常收发；
  * 原参数范围内 /api/simulate 仍返回与旧版一致的逐条 send_blocked 事件；
  * 长时延下回退/成功都保持 in-order + exactly-once。
"""

import random

import pytest

from app.app import DELAY_MAX, DEADLINE_MAX, app
from app.simulator import run_simulation


def delivered(result, sid):
    return [d["seq"] for d in result["delivery"][str(sid)]]


def assert_complete(result, n=14):
    assert result["ok"] is True
    for sid in (1, 2):
        assert delivered(result, sid) == list(range(1, n + 1))


# ------------------------------------------------------------ 1. 上限 60s
def test_limits_raised_to_60_seconds():
    assert DELAY_MAX == 60_000
    assert DEADLINE_MAX == 60_000


@pytest.fixture()
def client():
    app.testing = True
    return app.test_client()


def test_api_accepts_60s_values(client):
    resp = client.post("/api/simulate", json={
        "delay_l1_ms": 60_000, "delay_l2_ms": 60_000,
        "deadline_ms": 60_000, "moving_stream": 1,
        "packets_per_stream": 8})
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert_complete(data, n=8)


@pytest.mark.parametrize("field,payload_extra", [
    ("delay_l1_ms", {"delay_l1_ms": 60_001}),
    ("delay_l2_ms", {"delay_l2_ms": 60_001}),
    ("deadline_ms", {"deadline_ms": 60_001}),
])
def test_api_rejects_above_60s(client, field, payload_extra):
    payload = {"delay_l1_ms": 100, "delay_l2_ms": 100,
               "deadline_ms": 1000, "moving_stream": 1}
    payload.update(payload_extra)
    resp = client.post("/api/simulate", json=payload)
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_page_allows_60s_and_shows_compact_panel(client):
    html = client.get("/").get_data(as_text=True)
    assert 'id="compact-card"' in html
    assert 'id="engine-stats"' in html
    assert 'max="60000"' in html
    assert "longblock" in html  # 长时延预置场景 D


# ------------------------------------------------------------ 2. 受阻时段
def test_long_delay_blocked_span_first_and_last_ticks():
    # 场景 D：L2=60s(1200 ticks)，seq3 在 tick8 试发、tick1208 才到达释放，
    # 4 包窗口 [3,4,5,6] 从 tick24 到 tick1207 连续满。
    r = run_simulation(200, 60_000, 60_000, moving_stream=1)
    assert r["handover"]["outcome"] == "success"
    assert_complete(r)

    spans = [s for s in r["blocked_spans"] if s["stream"] == 1]
    assert spans, "长延时新链必须产生受阻时段"
    first = spans[0]
    assert (first["first_tick"], first["last_tick"]) == (24, 1207)
    assert first["first_ts"] == 24 * 50
    assert first["last_ts"] == 1207 * 50
    assert first["blocked_ticks"] == 1207 - 24 + 1
    assert first["window"] == [3, 4, 5, 6]
    # 首末毫秒/时长别名齐备
    assert first["start_ms"] == first["first_ts"]
    assert first["end_ms"] == first["last_ts"]
    assert first["duration_ms"] == first["blocked_ticks"] * 50


def test_compact_events_only_keep_edges_per_span():
    r = run_simulation(200, 60_000, 60_000, moving_stream=1, compact=True)
    blocked = [e for e in r["events"] if e["type"] == "send_blocked"]
    # 每个长度>1 的 span 只有首、末两条刻度事件
    expected = sum(1 if s["last_tick"] == s["first_tick"] else 2
                   for s in r["blocked_spans"])
    assert len(blocked) == expected
    edges = {(e["edge"], e["tick"]) for e in blocked}
    assert ("first", 24) in edges
    assert ("last", 1207) in edges
    assert all(e.get("compressed") is True for e in blocked)


# ------------------------------------------------------------ 3. 跳刻度统计
def test_stats_report_actually_processed_ticks():
    r = run_simulation(200, 60_000, 60_000, moving_stream=1)
    s = r["stats"]
    assert s["processed_ticks"] < s["total_ticks"]
    assert s["skipped_ticks"] == s["total_ticks"] - s["processed_ticks"]
    # 59s 大段空转后，实际处理刻度应远少于时间线槽位
    assert s["processed_ticks"] < 100
    assert s["total_ticks"] > 3000
    # 顶层别名也能取到
    assert r["processed_ticks"] == s["processed_ticks"]


def test_engine_fast_on_extreme_parameters():
    # 50 包 + 双 60s：旧逐 tick 循环要走 1.4 万+ 刻度并产生上万条重复事件，
    # 跳刻度引擎应在约百级刻度内完成。
    r = run_simulation(60_000, 60_000, 60_000, moving_stream=2,
                       packets_per_stream=50)
    assert_complete(r, n=50)
    assert r["stats"]["processed_ticks"] < 400
    assert not r["stats"]["hit_safety_cap"]
    assert not r["stats"]["stalled"]


# ------------------------------------------------------------ 4. 当刻续发
def test_delivery_release_resumes_send_same_tick():
    r = run_simulation(200, 60_000, 60_000, moving_stream=1)
    # tick1208：流1 的 seq3 交付释放窗口 → seq7 当刻新链续发
    release = [e for e in r["events"]
               if e["type"] == "deliver" and e["stream"] == 1
               and e["seq"] == 3]
    resume = [e for e in r["events"]
              if e["type"] == "send" and e["stream"] == 1
              and e["seq"] == 7 and e.get("attempt") == "new-trial"]
    assert len(release) == 1 and len(resume) == 1
    assert release[0]["tick"] == resume[0]["tick"] == 1208
    assert release[0]["ts"] == resume[0]["ts"] == 60_400


def test_rollback_resends_same_tick_when_send_timing_ready():
    # 长时延回退场景 E：超时落定 tick168，补发 seq3 与超时事件同 tick
    r = run_simulation(30_000, 200, 8_000, moving_stream=1)
    assert r["handover"]["outcome"] == "rollback"
    to = [e for e in r["events"] if e["type"] == "handover_timeout"]
    rs = [e for e in r["events"]
          if e["type"] == "send" and e.get("attempt") == "old-resend"
          and e["seq"] == 3]
    assert to and rs
    assert to[0]["tick"] == rs[0]["tick"] == 168

    # 经典场景 B 同样：tick20 到点即判、当刻补发
    rb = run_simulation(900, 100, 600, moving_stream=1)
    to_b = next(e for e in rb["events"] if e["type"] == "handover_timeout")
    rs_b = next(e for e in rb["events"]
                if e["type"] == "send" and e.get("attempt") == "old-resend"
                and e["seq"] == 3)
    assert to_b["tick"] == rs_b["tick"] == 20


def test_other_stream_keeps_sending_during_blocked_span():
    r = run_simulation(200, 60_000, 60_000, moving_stream=1)
    # 第一个大受阻段 tick24..1207 期间，流2 仍按 4 tick 节奏发送并交付
    s2_sends = [e["tick"] for e in r["events"]
                if e["type"] == "send" and e["stream"] == 2
                and 24 <= e["tick"] <= 60]
    assert s2_sends == [24, 28, 32, 36, 40, 44, 48, 52]
    assert delivered(r, 2) == list(range(1, 15))
    assert not any(e["type"] == "send_blocked" and e["stream"] == 2
                   for e in r["events"])


# ------------------------------------------------------------ 5. 向后兼容
def test_old_range_defaults_to_legacy_per_tick_events():
    r = run_simulation(100, 1200, 3000, moving_stream=1)
    assert r["stats"]["mode"] == "legacy"
    blocked = [e for e in r["events"] if e["type"] == "send_blocked"]
    # 逐条事件数 == 各 span 长度之和
    assert len(blocked) == sum(s["blocked_ticks"] for s in r["blocked_spans"])
    # 每条都是旧结构（没有 compressed/edge 新字段）
    assert all("compressed" not in e for e in blocked)
    # tick 连续、窗口恒为 4
    ticks = [e["tick"] for e in blocked]
    for a, b in zip(ticks, ticks[1:]):
        assert b >= a
    assert all(len(e["window"]) == 4 for e in blocked)


def test_api_old_request_still_itemized(client):
    resp = client.post("/api/simulate", json={
        "delay_l1_ms": 100, "delay_l2_ms": 1200,
        "deadline_ms": 3000, "moving_stream": 1})
    data = resp.get_json()
    assert data["stats"]["mode"] == "legacy"
    blocked = [e for e in data["events"] if e["type"] == "send_blocked"]
    assert len(blocked) == sum(s["blocked_ticks"] for s in data["blocked_spans"])
    # 显式 compact=true 可在旧参数上启用压缩
    resp2 = client.post("/api/simulate", json={
        "delay_l1_ms": 100, "delay_l2_ms": 1200,
        "deadline_ms": 3000, "moving_stream": 1, "compact": True})
    d2 = resp2.get_json()
    edge_blocked = [e for e in d2["events"] if e["type"] == "send_blocked"]
    assert len(edge_blocked) == 2 * len(d2["blocked_spans"])


# ------------------------------------------------------------ 6. 随机不变量
@pytest.mark.parametrize("seed", [1, 2, 3])
def test_random_extended_parameters_invariants(seed):
    rnd = random.Random(seed)
    for _ in range(30):
        d1 = rnd.choice([50, 100, 300, 1000, 3000, 10_000, 30_000, 60_000])
        d2 = rnd.randint(1, 1200) * 50
        dl = rnd.randint(2, 1200) * 50
        sid = rnd.choice((1, 2))
        n = rnd.randint(1, 50)
        r = run_simulation(d1, d2, dl, sid, packets_per_stream=n)
        assert r["ok"] is True
        for s in (1, 2):
            assert delivered(r, s) == list(range(1, n + 1))
        st = r["stats"]
        assert st["processed_ticks"] <= st["total_ticks"]
        assert not st["hit_safety_cap"]
        # 任何 span 首≤末、长度自洽
        for sp in r["blocked_spans"]:
            assert sp["first_tick"] <= sp["last_tick"]
            assert sp["blocked_ticks"] == sp["last_tick"] - sp["first_tick"] + 1
