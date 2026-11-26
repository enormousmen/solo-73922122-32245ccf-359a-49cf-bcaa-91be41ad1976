"""链路切换演练 —— 确定性离散事件模拟引擎。

场景背景
========
机房维护前，值班员要把一条备份流从旧链路迁到新链路。约束：

1. 切换采用 make-before-break 语义：
   - 切换开始后，该流后续报文先经新链路“试发”；
   - 接收端在旧链路上的在途报文全部按编号交付之前，不放行任何新链路上的
     报文（新链路报文先进入接收端重排序缓存等待）；
   - 旧链排空后，切换成功，等待中的新链路报文按编号继续交付，后续报文
     全部走新链路。
2. 切换期间，发往新链路但尚未交付给应用的报文最多 4 个；窗口满时只暂停
   这一条流（另一条流不受影响）。
3. 旧链在切换时限内未排空则超时：
   - 回退到旧链路；
   - 试发但未交付的报文经旧链路补发（重传）；
   - 迟到的新链路副本一律丢弃；
   - 新链缓存中尚未交付的报文同时作废。
4. 无论切换成功还是回退，接收端都必须把每条流的全部报文按编号、每包
   恰好一次交给应用（in-order + exactly-once，由重排序缓存和去重保证）。

时间模型：离散 tick，TICK_MS 毫秒一个 tick；每条链路的延时按 tick 取整，
同一 tick 内先处理报文到达，再处理发送，保证“到达 → 交付 → 释放窗口”
的顺序确定、可复现。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------- 常量

TICK_MS = 50            # 一个 tick 代表 50ms
SEND_INTERVAL_TICKS = 4  # 同一条流每 4 tick（200ms）发一个新包
NEW_LINK_CAP = 4        # 切换期间新链“已发未交付”报文上限
DEFAULT_PACKETS = 14    # 每条内置样例流的报文数（连续编号 1..N）
HANDOVER_START_TICK = 8  # 切换在第 8 个 tick（400ms）开始
MAX_TICKS = 4000        # 模拟安全阀，防止参数异常时死循环

# 阶段
PHASE_NORMAL = "normal"          # 常规运行，走当前链路
PHASE_HANDOVER = "handover"      # 切换中：旧链排空 + 新链试发
PHASE_SUCCESS = "success"        # 切换成功，新链正式承担流量
PHASE_ROLLBACK = "rollback"      # 已超时回退，旧链补发 + 后续走旧链


def _delay_to_ticks(delay_ms: int) -> int:
    """毫秒延时换算为 tick 数，最小 1 tick。"""
    return max(1, int(round(delay_ms / TICK_MS)))


@dataclass
class _StreamState:
    """单条流的发送端/接收端运行态。"""

    sid: int
    home: str                                   # 初始（回退后的）链路 L1/L2
    old_link: str                               # 切换前所在链路
    new_link: str                               # 切换目标链路
    is_moving: bool                             # 本次演练是否被迁移
    phase: str = PHASE_NORMAL
    next_seq: int = 1                           # 下一个待生成的编号
    cooldown: int = 0                           # 发包冷却（tick）
    last_old_seq: int = 0                       # 旧链最后发出的编号
    receiver_next: int = 1                      # 接收端下一个应交付编号
    hold: dict[int, dict[str, Any]] = field(default_factory=dict)  # 重排序缓存
    new_undelivered: set[int] = field(default_factory=set)         # 新链已发未交付编号
    resend_queue: list[int] = field(default_factory=list)          # 回退后待补发编号
    finished: bool = False                      # 是否已全部交付
    handover_outcome: str | None = None         # success / rollback
    handover_tick: int | None = None           # 结果落定的 tick
    delivered_count: int = 0


def run_simulation(
    delay_l1_ms: int,
    delay_l2_ms: int,
    deadline_ms: int,
    moving_stream: int,
    packets_per_stream: int = DEFAULT_PACKETS,
) -> dict[str, Any]:
    """执行一次完整演练，返回结构化时间线（纯函数，无副作用）。

    参数
    ----
    delay_l1_ms / delay_l2_ms: 两条链路的单向延时（毫秒）。
    deadline_ms: 切换时限——从切换开始计时，旧链必须在此时间内排空。
    moving_stream: 要迁移的流编号（1 或 2）。流 1 默认走 L1，流 2 默认走 L2，
                   迁移即把它换到另一条链路。
    packets_per_stream: 每条流的报文总数，编号连续 1..N。
    """
    # ---- 输入校验（API 层也会校验，这里兜底）
    if moving_stream not in (1, 2):
        raise ValueError("moving_stream 必须为 1 或 2")
    for name, val in (("delay_l1_ms", delay_l1_ms),
                      ("delay_l2_ms", delay_l2_ms),
                      ("deadline_ms", deadline_ms)):
        if not isinstance(val, int) or val <= 0:
            raise ValueError(f"{name} 必须为正整数（毫秒）")
    if packets_per_stream < 1:
        raise ValueError("packets_per_stream 至少为 1")

    link_delay = {
        "L1": _delay_to_ticks(delay_l1_ms),
        "L2": _delay_to_ticks(delay_l2_ms),
    }
    deadline_ticks = max(1, int(round(deadline_ms / TICK_MS)))

    # 两条连续编号的内置样例流
    streams: dict[int, _StreamState] = {}
    for sid, home in ((1, "L1"), (2, "L2")):
        target = "L2" if home == "L1" else "L1"
        streams[sid] = _StreamState(
            sid=sid,
            home=home,
            old_link=home,
            new_link=target,
            is_moving=(sid == moving_stream),
        )

    in_flight: list[dict[str, Any]] = []   # 在途报文
    events: list[dict[str, Any]] = []      # 时间线事件
    handover_start_tick = HANDOVER_START_TICK
    handover_started = False

    def ev(ts: int, kind: str, stream: int | None = None, **extra: Any) -> None:
        entry = {"ts": ts, "tick": ts // TICK_MS, "type": kind}
        if stream is not None:
            entry["stream"] = stream
        entry.update(extra)
        events.append(entry)

    # ------------------------------------------------------------ 主循环
    for tick in range(MAX_TICKS):
        ts = tick * TICK_MS

        # 1) 切换开始（仅一次）
        if not handover_started and tick >= handover_start_tick:
            handover_started = True
            mv = streams[moving_stream]
            mv.phase = PHASE_HANDOVER
            mv.last_old_seq = mv.next_seq - 1
            ev(ts, "handover_start", mv.sid,
              old_link=mv.old_link, new_link=mv.new_link,
              deadline_ms=deadline_ms, cap=NEW_LINK_CAP,
              last_old_seq=mv.last_old_seq)

        # 2) 到达处理：本 tick 到期的在途报文
        arrivals = [p for p in in_flight if p["arrive_tick"] == tick]
        in_flight = [p for p in in_flight if p["arrive_tick"] != tick]
        for pkt in sorted(arrivals, key=lambda p: (p["stream"], p["seq"], p["link"])):
            st = streams[pkt["stream"]]

            # 回退后：新链路上迟到的副本（在途 + 缓存）一律丢弃
            if st.is_moving and st.phase == PHASE_ROLLBACK and pkt["link"] == st.new_link:
                ev(ts, "packet_discard", st.sid, seq=pkt["seq"], link=pkt["link"],
                   attempt=pkt["attempt"], reason="回退后迟到的新链路副本")
                st.new_undelivered.discard(pkt["seq"])
                continue

            # 去重：已经交付过的编号直接丢弃
            if pkt["seq"] < st.receiver_next:
                ev(ts, "packet_discard", st.sid, seq=pkt["seq"], link=pkt["link"],
                   attempt=pkt["attempt"], reason="重复副本（该编号已交付）")
                continue
            if pkt["seq"] in st.hold:
                ev(ts, "packet_discard", st.sid, seq=pkt["seq"], link=pkt["link"],
                   attempt=pkt["attempt"], reason="重复副本（重排序缓存中已有）")
                continue

            st.hold[pkt["seq"]] = {"link": pkt["link"], "attempt": pkt["attempt"]}
            ev(ts, "packet_arrive", st.sid, seq=pkt["seq"], link=pkt["link"],
               attempt=pkt["attempt"])

        # 3) 接收端按编号交付（in-order + exactly-once）
        for st in streams.values():
            while st.receiver_next in st.hold:
                seq = st.receiver_next
                meta = st.hold.pop(seq)
                st.delivered_count += 1
                st.receiver_next += 1
                st.new_undelivered.discard(seq)   # 交付即释放新链窗口
                ev(ts, "deliver", st.sid, seq=seq, link=meta["link"],
                   attempt=meta["attempt"])
            if not st.finished and st.receiver_next > packets_per_stream:
                st.finished = True
                ev(ts, "stream_done", st.sid, delivered=st.delivered_count)

        # 4) 切换判定：成功（旧链排空）优先于超时
        mv = streams[moving_stream]
        if handover_started and mv.phase == PHASE_HANDOVER:
            old_drained = mv.receiver_next > mv.last_old_seq
            # 时限到点（elapsed >= deadline）即判超时；排空优先，同 tick 排空算成功
            timed_out = (tick - handover_start_tick) >= deadline_ticks
            if old_drained:
                # 旧链在途报文已全部按编号交付 → 放行新链缓存，切换成功
                mv.phase = PHASE_SUCCESS
                mv.handover_outcome = "success"
                mv.handover_tick = tick
                ev(ts, "handover_success", mv.sid,
                   new_link=mv.new_link, old_link=mv.old_link,
                   held_new_packets=sorted(mv.new_undelivered))
            elif timed_out:
                # 超时 → 回退旧链：作废新链缓存，未交付试发报文排队补发
                purged = sorted(mv.new_undelivered)
                for seq in purged:
                    st_hold = mv.hold.pop(seq, None)
                    if st_hold is not None:
                        ev(ts, "packet_discard", mv.sid, seq=seq,
                           link=mv.new_link, attempt="new-trial",
                           reason="超时回退：作废新链缓存中未交付报文")
                mv.resend_queue = list(purged)
                mv.new_undelivered.clear()
                mv.phase = PHASE_ROLLBACK
                mv.handover_outcome = "rollback"
                mv.handover_tick = tick
                ev(ts, "handover_timeout", mv.sid,
                   old_link=mv.old_link, new_link=mv.new_link,
                   deadline_ms=deadline_ms,
                   resend_seqs=purged)

        # 5) 全部交付即结束
        if all(st.finished for st in streams.values()):
            break

        # 6) 发送端
        for st in streams.values():
            if st.finished or st.cooldown > 0:
                if st.cooldown > 0:
                    st.cooldown -= 1
                continue

            if not st.is_moving or st.phase == PHASE_NORMAL:
                # 非迁移流：始终走自己的链路；迁移流切换前走旧链
                link = st.home if not st.is_moving else st.old_link
                attempt = "original"
                seq = st.next_seq
            elif st.phase == PHASE_HANDOVER:
                # 切换中：先补发没有（切换瞬间旧链上一般无积压，队列通常为空），
                # 再经新链试发；新链窗口满 → 只暂停本流
                if st.resend_queue:
                    seq = st.resend_queue.pop(0)
                    link = st.old_link
                    attempt = "old-resend"
                elif len(st.new_undelivered) >= NEW_LINK_CAP:
                    ev(ts, "send_blocked", st.sid,
                      cap=NEW_LINK_CAP,
                      window=sorted(st.new_undelivered),
                      reason=f"新链未交付报文已达 {NEW_LINK_CAP} 个上限，仅暂停本流")
                    continue
                else:
                    seq = st.next_seq
                    link = st.new_link
                    attempt = "new-trial"
            elif st.phase == PHASE_SUCCESS:
                # 成功后剩余报文走新链；只要新链“已发未交付”仍达上限，
                # 同样只暂停本流（4 包约束覆盖整个新链生命周期）
                if len(st.new_undelivered) >= NEW_LINK_CAP:
                    ev(ts, "send_blocked", st.sid,
                      cap=NEW_LINK_CAP,
                      window=sorted(st.new_undelivered),
                      reason=f"新链未交付报文已达 {NEW_LINK_CAP} 个上限，仅暂停本流")
                    continue
                link = st.new_link
                attempt = "new-trial"
                seq = st.next_seq
            else:  # PHASE_ROLLBACK：先补发未交付的试发报文，再发后续新报文
                if st.resend_queue:
                    seq = st.resend_queue.pop(0)
                    link = st.old_link
                    attempt = "old-resend"
                else:
                    seq = st.next_seq
                    link = st.old_link
                    attempt = "original"

            if seq > packets_per_stream:
                continue

            in_flight.append({
                "stream": st.sid,
                "seq": seq,
                "link": link,
                "attempt": attempt,
                "send_tick": tick,
                "arrive_tick": tick + link_delay[link],
            })
            ev(ts, "send", st.sid, seq=seq, link=link, attempt=attempt,
               arrive_ms=(tick + link_delay[link]) * TICK_MS)
            if attempt == "new-trial":
                st.new_undelivered.add(seq)
            # 只有“新生成”的编号推进 next_seq；重传编号不推进
            if seq == st.next_seq:
                st.next_seq += 1
            st.cooldown = SEND_INTERVAL_TICKS - 1

    # ------------------------------------------------------------ 汇总
    delivery_log = [e for e in events if e["type"] == "deliver"]
    discard_log = [e for e in events if e["type"] == "packet_discard"]
    result = _build_result(
        streams, events, delivery_log, discard_log,
        link_delay, delay_l1_ms, delay_l2_ms,
        deadline_ms, deadline_ticks, moving_stream,
        packets_per_stream, handover_start_tick,
    )
    return result


def _build_result(
    streams: dict[int, _StreamState],
    events: list[dict[str, Any]],
    delivery_log: list[dict[str, Any]],
    discard_log: list[dict[str, Any]],
    link_delay_ticks: dict[str, int],
    delay_l1_ms: int,
    delay_l2_ms: int,
    deadline_ms: int,
    deadline_ticks: int,
    moving_stream: int,
    packets_per_stream: int,
    handover_start_tick: int,
) -> dict[str, Any]:
    """组装返回结构并做自检（in-order / exactly-once 等断言）。"""
    checks: list[dict[str, Any]] = []

    def check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    # 每条流：编号 1..N 齐全、有序、仅交付一次
    all_ordered_unique = True
    per_stream: dict[int, list[dict[str, Any]]] = {1: [], 2: []}
    for e in delivery_log:
        per_stream[e["stream"]].append(e)
    for sid in (1, 2):
        seqs = [e["seq"] for e in per_stream[sid]]
        ok = seqs == list(range(1, packets_per_stream + 1))
        all_ordered_unique &= ok
        check(f"流{sid} 按编号交付且每包一次", ok,
              f"交付编号序列: {seqs}")

    # 切换成功时：旧链报文必须全部先于新链报文交付（新链放行前旧链已排空）
    mv = streams[moving_stream]
    order_ok = True
    order_detail = "未发生迁移判定"
    if mv.handover_outcome == "success":
        first_new = next((e["tick"] for e in per_stream[moving_stream]
                          if e["link"] == mv.new_link), None)
        last_old = max((e["tick"] for e in per_stream[moving_stream]
                        if e["link"] == mv.old_link), default=-1)
        if first_new is None:
            # 切换前报文已全部发完并交付，没有新链报文——合法情形
            order_ok = True
            order_detail = "无新链报文（切换前该流已全部发完）"
        else:
            order_ok = last_old <= first_new
            order_detail = (f"旧链最后交付 tick={last_old}，"
                            f"新链最早交付 tick={first_new}")
    elif mv.handover_outcome == "rollback":
        # 回退后应用不应再收到任何新链交付
        new_after = [e for e in per_stream[moving_stream] if e["link"] == mv.new_link]
        order_ok = len(new_after) == 0
        order_detail = f"回退后新链交付次数={len(new_after)}（应为 0）"
    check("旧链排空后才放行新链 / 回退后无新链交付", order_ok, order_detail)

    # 回退场景：被丢弃的都是新链路副本，且每个未交付试发编号都经旧链补发
    rollback_ok = True
    rollback_detail = "本场景为切换成功"
    if mv.handover_outcome == "rollback":
        bad_discard = [d for d in discard_log if d["link"] != mv.new_link]
        resent = {e["seq"] for e in events
                  if e["type"] == "send" and e.get("attempt") == "old-resend"}
        delivered_seqs = {e["seq"] for e in per_stream[moving_stream]}
        missing = resent - delivered_seqs
        rollback_ok = not bad_discard and not missing
        rollback_detail = (f"旧链补发 {len(resent)} 包，全部已交付；"
                           f"丢弃新链副本 {len(discard_log)} 个；"
                           f"非新链误丢 {len(bad_discard)} 个")
    check("回退补发完整且仅丢弃新链副本", rollback_ok, rollback_detail)

    # 新链窗口上限：任一时刻 new_undelivered 不超过 4（用事件重建窗口峰值）
    peak = 0
    live: set[int] = set()
    for e in events:
        if e["type"] == "send" and e.get("attempt") == "new-trial":
            live.add(e["seq"])
        elif e["type"] == "deliver" and e.get("attempt") == "new-trial":
            live.discard(e["seq"])
        elif e["type"] == "packet_discard" and e.get("attempt") == "new-trial":
            live.discard(e["seq"])
        peak = max(peak, len(live))
    cap_ok = peak <= NEW_LINK_CAP
    check(f"新链在途未交付不超过 {NEW_LINK_CAP} 个", cap_ok,
          f"窗口峰值={peak}")

    overall = all(c["ok"] for c in checks)

    return {
        "config": {
            "delay_l1_ms": delay_l1_ms,
            "delay_l2_ms": delay_l2_ms,
            "deadline_ms": deadline_ms,
            "deadline_ticks": deadline_ticks,
            "moving_stream": moving_stream,
            "packets_per_stream": packets_per_stream,
            "tick_ms": TICK_MS,
            "send_interval_ms": SEND_INTERVAL_TICKS * TICK_MS,
            "new_link_cap": NEW_LINK_CAP,
            "handover_start_ms": handover_start_tick * TICK_MS,
        },
        "link_delay_ticks": link_delay_ticks,
        "events": events,
        "timeline": events,  # 前端按 ts 播放
        "streams": [
            {
                "sid": st.sid,
                "home_link": st.home,
                "old_link": st.old_link,
                "new_link": st.new_link,
                "is_moving": st.is_moving,
                "handover_outcome": st.handover_outcome,
                "handover_tick": st.handover_tick,
                "handover_ms": (st.handover_tick * TICK_MS
                                if st.handover_tick is not None else None),
                "delivered_count": st.delivered_count,
            }
            for st in streams.values()
        ],
        "delivery": {
            str(sid): [
                {"seq": e["seq"], "link": e["link"], "attempt": e["attempt"],
                 "ts": e["ts"]}
                for e in per_stream[sid]
            ]
            for sid in (1, 2)
        },
        "discards": [
            {"seq": e["seq"], "link": e["link"], "attempt": e["attempt"],
             "reason": e["reason"], "ts": e["ts"], "stream": e["stream"]}
            for e in discard_log
        ],
        "handover": {
            "moving_stream": moving_stream,
            "outcome": mv.handover_outcome,
            "handover_start_ms": handover_start_tick * TICK_MS,
            "finish_ms": (mv.handover_tick * TICK_MS
                          if mv.handover_tick is not None else None),
        },
        "checks": checks,
        "ok": overall,
        "max_ts": max((e["ts"] for e in events), default=0),
    }
