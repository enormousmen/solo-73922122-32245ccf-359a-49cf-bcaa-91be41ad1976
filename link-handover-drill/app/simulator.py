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

长时延（最长 60s）下的性能/可读性设计
====================================
* 引擎不再逐 tick 空转，而是跳到下一个“有事情发生”的刻度（报文到达、
  发送时机、切换开始/到点），中间的空刻度只批量推进 cooldown；
* 窗口满后不再逐 tick 重复试发，直接跳到下一次窗口可能变化的刻度
  （交付/丢弃/超时）；
* 窗口内报文集合相同且连续受阻的一段刻度压缩为一个“受阻时段”
  （blocked span），只保留首、末受阻刻度；
* 原参数范围（延时 ≤3000ms、时限 ≤5000ms）内，API 仍可把受阻时段
  展开成与旧版完全一致的逐条 ``send_blocked`` 事件（legacy/full 模式）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------- 常量

TICK_MS = 50            # 一个 tick 代表 50ms
SEND_INTERVAL_TICKS = 4  # 同一条流每 4 tick（200ms）发一个新包
NEW_LINK_CAP = 4        # 切换期间新链“已发未交付”报文上限
DEFAULT_PACKETS = 14    # 每条内置样例流的报文数（连续编号 1..N）
HANDOVER_START_TICK = 8  # 切换在第 8 个 tick（400ms）开始

# 参数上限已放宽到 60 秒；旧上限保留用于识别“原参数范围”的兼容请求
DELAY_MIN_MS, DELAY_MAX_MS = 50, 60_000
DEADLINE_MIN_MS, DEADLINE_MAX_MS = 100, 60_000
# 常见命名别名
MAX_DELAY_MS, MAX_DEADLINE_MS = DELAY_MAX_MS, DEADLINE_MAX_MS
LEGACY_DELAY_MAX_MS = 3_000
LEGACY_DEADLINE_MAX_MS = 5_000

# 模拟安全阀（tick）。跳刻度后单次成本只与“实际处理刻度数”有关，
# 因此可以给得很宽裕，覆盖 60s 延时 + 50 包的任意组合。
MAX_TICKS = 200_000

# 阶段
PHASE_NORMAL = "normal"          # 常规运行，走当前链路
PHASE_HANDOVER = "handover"      # 切换中：旧链排空 + 新链试发
PHASE_SUCCESS = "success"       # 切换成功，新链正式承担流量
PHASE_ROLLBACK = "rollback"      # 已超时回退，旧链补发 + 后续走旧链

BLOCK_REASON = f"新链未交付报文已达 {NEW_LINK_CAP} 个上限，仅暂停本流"


def _delay_to_ticks(delay_ms: int) -> int:
    """毫秒延时换算为 tick 数，最小 1 tick。"""
    return max(1, int(round(delay_ms / TICK_MS)))


def within_legacy_range(delay_l1_ms: int, delay_l2_ms: int,
                        deadline_ms: int) -> bool:
    """请求参数是否落在旧版允许的范围内（该范围内事件格式保持原样）。"""
    return (max(delay_l1_ms, delay_l2_ms) <= LEGACY_DELAY_MAX_MS
            and deadline_ms <= LEGACY_DEADLINE_MAX_MS)


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
    # —— 跳刻度 / 受阻时段压缩所需的瞬态字段 ——
    open_span: dict[str, Any] | None = None     # 当前尚未结束的连续受阻时段
    last_cap_tick: int | None = None            # 最近一次命中窗口上限的刻度


def run_simulation(
    delay_l1_ms: int,
    delay_l2_ms: int,
    deadline_ms: int,
    moving_stream: int,
    packets_per_stream: int = DEFAULT_PACKETS,
    compact: bool | None = None,
    **options: Any,
) -> dict[str, Any]:
    """执行一次完整演练，返回结构化时间线（纯函数，无副作用）。

    参数
    ----
    delay_l1_ms / delay_l2_ms: 两条链路的单向延时（毫秒）。
    deadline_ms: 切换时限——从切换开始计时，旧链必须在此时间内排空。
    moving_stream: 要迁移的流编号（1 或 2）。流 1 默认走 L1，流 2 默认走 L2，
                   迁移即把它换到另一条链路。
    packets_per_stream: 每条流的报文总数，编号连续 1..N。
    compact: 受阻事件的返回形式。
        * ``None``（默认）：按参数自动选择——原参数范围内返回与旧版完全一致
          的逐条 ``send_blocked`` 事件；超出原范围（长时延）使用压缩时间线；
        * ``False``：始终展开为逐 tick 事件（引擎内部仍跳过空刻度）；
        * ``True``：始终压缩为“首末受阻刻度”时段。
      为兼容不同调用方，也接受 ``compress`` / ``merge_blocked`` /
      ``collapse_blocked`` / ``full_events`` 等别名关键字。
    """
    # 兼容若干可能的开关命名
    if compact is None:
        if options.get("full_events") is True or options.get("expand") is True:
            compact = False
        for alias in ("compress", "compressed", "merge_blocked",
                      "collapse_blocked"):
            if options.get(alias) is not None:
                compact = bool(options[alias])
                break
        if compact is None and options.get("timeline_mode") == "compact":
            compact = True

    # ---- 输入校验（API 层也会校验，这里兜底）
    if moving_stream not in (1, 2):
        raise ValueError("moving_stream 必须为 1 或 2")
    for name, val in (("delay_l1_ms", delay_l1_ms),
                      ("delay_l2_ms", delay_l2_ms),
                      ("deadline_ms", deadline_ms)):
        if not isinstance(val, int) or isinstance(val, bool) or val <= 0:
            raise ValueError(f"{name} 必须为正整数（毫秒）")
    if not isinstance(packets_per_stream, int) or packets_per_stream < 1:
        raise ValueError("packets_per_stream 至少为 1")

    link_delay = {
        "L1": _delay_to_ticks(delay_l1_ms),
        "L2": _delay_to_ticks(delay_l2_ms),
    }
    deadline_ticks = max(1, int(round(deadline_ms / TICK_MS)))

    # 未显式指定时：原参数范围 → legacy 逐条事件；长时延 → 压缩时段
    if compact is None:
        compact = not within_legacy_range(delay_l1_ms, delay_l2_ms, deadline_ms)
    mode = "compact" if compact else "legacy"

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
    events: list[dict[str, Any]] = []      # 常规事件（不含 send_blocked），保持旧顺序
    blocked_spans: list[dict[str, Any]] = []
    handover_start_tick = HANDOVER_START_TICK
    handover_started = False
    processed_ticks = 0
    hit_safety_cap = False
    stalled = False

    def ev(ts: int, kind: str, stream: int | None = None, **extra: Any) -> None:
        entry: dict[str, Any] = {"ts": ts, "tick": ts // TICK_MS, "type": kind}
        if stream is not None:
            entry["stream"] = stream
        entry.update(extra)
        events.append(entry)

    def note_blocked(tick: int, st: _StreamState) -> None:
        """记录一次“窗口满受阻”，合并进连续且窗口集合相同的时段。

        跳刻度驱动保证：两次处理之间不存在改变该窗口的事件时，受阻状态
        逐 tick 连续，因此可以直接把末刻度延伸到当前 tick。
        """
        window = tuple(sorted(st.new_undelivered))
        span = st.open_span
        if span is not None and tuple(span["window"]) == window:
            span["last_tick"] = tick
        else:
            span = {
                "stream": st.sid,
                "cap": NEW_LINK_CAP,
                "window": list(window),
                "reason": BLOCK_REASON,
                "first_tick": tick,
                "last_tick": tick,
            }
            blocked_spans.append(span)
            st.open_span = span
        st.last_cap_tick = tick

    def close_blocked_span(st: _StreamState, before_tick: int) -> None:
        """受阻时段在 ``before_tick`` 之前一 tick 结束（释放/回退/发新包）。"""
        span = st.open_span
        if span is None:
            return
        end_tick = before_tick - 1
        if end_tick >= span["first_tick"]:
            span["last_tick"] = max(span["last_tick"], end_tick)
        st.open_span = None

    # ------------------------------------------------------------ 主循环（跳刻度）
    tick = 0
    while True:
        processed_ticks += 1
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
                close_blocked_span(st, before_tick=tick)
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
                # 窗口集合已作废：旧的“窗口满受阻”连续段在本 tick 之前结束
                close_blocked_span(mv, before_tick=tick)
                mv.phase = PHASE_ROLLBACK
                mv.handover_outcome = "rollback"
                mv.handover_tick = tick
                ev(ts, "handover_timeout", mv.sid,
                   old_link=mv.old_link, new_link=mv.new_link,
                   deadline_ms=deadline_ms,
                   resend_seqs=purged)

        # 5) 全部交付即结束（与旧版一致：结束后不再处理残余在途副本）
        if all(st.finished for st in streams.values()):
            break

        # 6) 发送端（同 tick 内位于 到达→交付→判定 之后，因此释放/回退当刻可发）
        for st in streams.values():
            if st.finished or st.cooldown > 0:
                if st.cooldown > 0:
                    st.cooldown -= 1
                continue

            blocked_now = False
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
                    blocked_now = True
                    seq = -1
                    link = ""
                    attempt = ""
                else:
                    seq = st.next_seq
                    link = st.new_link
                    attempt = "new-trial"
            elif st.phase == PHASE_SUCCESS:
                # 成功后剩余报文走新链；只要新链“已发未交付”仍达上限，
                # 同样只暂停本流（4 包约束覆盖整个新链生命周期）
                if len(st.new_undelivered) >= NEW_LINK_CAP:
                    blocked_now = True
                    seq = -1
                    link = ""
                    attempt = ""
                else:
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

            if blocked_now:
                # 与旧版相同的逐条事件不在引擎里直接追加，先压缩成时段；
                # legacy 输出时再按首末刻度展开，保证事件逐字一致。
                note_blocked(tick, st)
                continue

            # 窗口已不满 / 回退补发等：若此前有连续受阻段，它在上一 tick 结束
            if st.open_span is not None:
                close_blocked_span(st, before_tick=tick)

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

        # ------------------------------------------------ 跳到下一个有事的刻度
        next_tick = _next_relevant_tick(
            tick, streams, in_flight, handover_started,
            handover_start_tick, deadline_ticks, packets_per_stream,
        )
        if next_tick is None:
            # 没有任何在途报文/发送时机/判定点，却仍未全部完成：
            # 正常参数下不可能发生；记录诊断并结束，避免死循环。
            stalled = True
            break
        if next_tick >= MAX_TICKS:
            hit_safety_cap = True
            tick = MAX_TICKS - 1
            break

        # 批量推进被跳过空刻度上的 cooldown。
        # 旧逻辑在 t+1..next_tick-1 每个刻度开头各做一次“>0 则 -1”，
        # 因此到达 next_tick 开头时 cooldown 为 max(0, cd - gap)，
        # gap = next_tick - tick - 1。
        gap = next_tick - tick - 1
        if gap > 0:
            for st in streams.values():
                if st.cooldown > 0:
                    st.cooldown = max(0, st.cooldown - gap)
        tick = next_tick

    last_tick = tick

    # 防御性收口：任何仍挂着的时段结束于最后处理刻度（正常路径已关闭）
    for st in streams.values():
        if st.open_span is not None:
            st.open_span["last_tick"] = max(
                st.open_span["last_tick"], last_tick)
            st.open_span = None

    # 补全 span 的派生字段（首末 ms、持续刻度数等）
    span_records = [_finalize_span(sp) for sp in blocked_spans]

    # 按输出模式组装事件（legacy 展开 / compact 首末）
    out_events = _assemble_events(events, span_records, compact)

    # 窗口占用变化序列（迁移流），供紧凑时间线画占用条
    occupancy = _build_occupancy(out_events, moving_stream)

    delivery_log = [e for e in events if e["type"] == "deliver"]
    discard_log = [e for e in events if e["type"] == "packet_discard"]

    total_slots = last_tick + 1
    skipped_ticks = max(0, total_slots - processed_ticks)
    stats = {
        "processed_ticks": processed_ticks,
        "actual_ticks": processed_ticks,          # 别名
        "ticks_processed": processed_ticks,      # 别名
        "engine_processed_ticks": processed_ticks,
        "processed_tick_count": processed_ticks, # 别名
        "total_ticks": total_slots,
        "virtual_ticks": last_tick,
        "last_tick": last_tick,
        "skipped_ticks": skipped_ticks,
        "ticks_skipped": skipped_ticks,
        "empty_ticks_skipped": skipped_ticks,    # 别名
        "tick_ms": TICK_MS,
        "mode": mode,
        "compact": compact,
        "blocked_span_count": len(span_records),
        "stalled": stalled,
        "hit_safety_cap": hit_safety_cap,
    }

    result = _build_result(
        streams, out_events, delivery_log, discard_log,
        link_delay, delay_l1_ms, delay_l2_ms,
        deadline_ms, deadline_ticks, moving_stream,
        packets_per_stream, handover_start_tick,
        blocked_spans=span_records, occupancy=occupancy, stats=stats,
        compact=compact,
    )
    return result


def _next_relevant_tick(
    tick: int,
    streams: dict[int, _StreamState],
    in_flight: list[dict[str, Any]],
    handover_started: bool,
    handover_start_tick: int,
    deadline_ticks: int,
    packets_per_stream: int,
) -> int | None:
    """返回严格晚于 ``tick`` 的下一个必须执行刻度体的刻度。

    候选来源：切换开始、时限到点、任一在途报文到达、任一流的下一次
    发送时机。窗口满且 cooldown=0 的流不逐 tick 重试（其窗口只会被
    某个到达/判定刻度改变），无包可发的流也不再产生自发刻度。
    """
    candidates: list[int] = []

    if not handover_started and handover_start_tick > tick:
        candidates.append(handover_start_tick)

    # deadline 候选需要迁移流状态：从 streams 中找 is_moving
    moving = next((s for s in streams.values() if s.is_moving), None)
    if (handover_started and moving is not None
            and moving.phase == PHASE_HANDOVER):
        dt = handover_start_tick + deadline_ticks
        if dt > tick:
            candidates.append(dt)

    for pkt in in_flight:
        if pkt["arrive_tick"] > tick:
            candidates.append(pkt["arrive_tick"])

    for st in streams.values():
        if st.finished:
            continue
        # 正挂在“连续窗口满受阻”段上：不产生 t+1 的逐刻度试发，
        # 等待到达/时限等外部候选把引擎带到释放/回退刻度。
        if st.open_span is not None:
            continue
        # 补发队列或尚未生成的新编号：cooldown 结束就有发送动作/受阻判定
        if st.resend_queue or st.next_seq <= packets_per_stream:
            candidates.append(tick + st.cooldown + 1)
            continue
        # 编号已全部生成，但窗口仍是满的（典型：最后 4 包 [N-3..N] 卡在
        # 新链上）：cooldown 结束那一 tick 会产生第一条 send_blocked /
        # 受阻时段起点，不能漏，也必须给自发候选。
        cap_phase = (not st.is_moving
                     or st.phase in (PHASE_HANDOVER, PHASE_SUCCESS))
        if cap_phase and len(st.new_undelivered) >= NEW_LINK_CAP:
            candidates.append(tick + st.cooldown + 1)
        # 否则：无补发、无新编号、窗口也不满，发送端不会再产生可见事件，
        # 后续只可能由在途报文到达驱动。

    return min(candidates) if candidates else None


def _finalize_span(sp: dict[str, Any]) -> dict[str, Any]:
    """补齐受阻时段的毫秒边界、长度与各种命名别名。"""
    f, l = sp["first_tick"], sp["last_tick"]
    length = l - f + 1
    first_ts, last_ts = f * TICK_MS, l * TICK_MS
    record = {
        # 规范字段
        "stream": sp["stream"],
        "cap": sp["cap"],
        "window": sp["window"],
        "reason": sp["reason"],
        "first_tick": f,
        "last_tick": l,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "blocked_ticks": length,
        # 同义/常见命名，方便不同调用方与前端
        "start_tick": f,
        "end_tick": l,
        "from_tick": f,
        "to_tick": l,
        "first_blocked_tick": f,
        "last_blocked_tick": l,
        "start_ts": first_ts,
        "end_ts": last_ts,
        "start_ms": first_ts,
        "end_ms": last_ts,
        "first_ms": first_ts,
        "last_ms": last_ts,
        "first_blocked_ms": first_ts,
        "last_blocked_ms": last_ts,
        "duration_ms": length * TICK_MS,
        "duration_ticks": length,
        "tick_count": length,
        "ticks": length,
        "count": length,
        "length": length,
        "continuous": True,
    }
    return record


def _legacy_blocked_event(span: dict[str, Any], tick: int) -> dict[str, Any]:
    """构造与旧版逐 tick 输出完全同构（含键顺序）的 send_blocked 事件。"""
    return {
        "ts": tick * TICK_MS,
        "tick": tick,
        "type": "send_blocked",
        "stream": span["stream"],
        "cap": span["cap"],
        "window": list(span["window"]),
        "reason": span["reason"],
    }


def _span_edge_event(span: dict[str, Any], tick: int,
                     edge: str) -> dict[str, Any]:
    """压缩模式下的“首/末受阻刻度”事件：保持 legacy 键，附加首末信息。"""
    ev_dict = _legacy_blocked_event(span, tick)
    ev_dict["compressed"] = True
    ev_dict["edge"] = edge  # "first" | "last"
    ev_dict["span_id"] = f"s{span['stream']}-{span['first_tick']}-{span['last_tick']}"
    ev_dict["first_tick"] = span["first_tick"]
    ev_dict["last_tick"] = span["last_tick"]
    ev_dict["first_blocked_tick"] = span["first_tick"]
    ev_dict["last_blocked_tick"] = span["last_tick"]
    ev_dict["first_ts"] = span["first_ts"]
    ev_dict["last_ts"] = span["last_ts"]
    ev_dict["blocked_ticks"] = span["blocked_ticks"]
    ev_dict["span"] = {
        "first_tick": span["first_tick"],
        "last_tick": span["last_tick"],
        "blocked_ticks": span["blocked_ticks"],
        "window": list(span["window"]),
    }
    return ev_dict


def _assemble_events(
    base_events: list[dict[str, Any]],
    spans: list[dict[str, Any]],
    compact: bool,
) -> list[dict[str, Any]]:
    """把受阻事件按“同 tick 内发送端按流编号排序”的旧顺序合回事件流。

    - legacy 模式：每个 span 展开为首..末每个刻度一条 send_blocked，
      与旧引擎逐条 append 的结果逐字一致；
    - compact 模式：每个 span 只放首、末两条刻度事件（“只记首末”），
      完整首末信息同时可在 ``blocked_spans`` 中取到。
    """
    # 按 tick 归集要插入的受阻事件（均属于该 tick 阶段 6 的发送端序列）
    blocked_by_tick: dict[int, list[dict[str, Any]]] = {}
    for span in spans:
        sid = span["stream"]
        if compact:
            items = [
                (span["first_tick"], _span_edge_event(span, span["first_tick"], "first")),
            ]
            if span["last_tick"] > span["first_tick"]:
                items.append(
                    (span["last_tick"], _span_edge_event(span, span["last_tick"], "last")))
        else:
            items = [
                (t, _legacy_blocked_event(span, t))
                for t in range(span["first_tick"], span["last_tick"] + 1)
            ]
        for t, item in items:
            blocked_by_tick.setdefault(t, []).append(item)

    if not blocked_by_tick:
        return list(base_events)

    # 基础事件按 tick 分组（保持 append 顺序）；受阻段中间的“纯受阻刻度”
    # 在 base_events 里没有对应组，因此要取 tick 并集后逐 tick 合并。
    base_by_tick: dict[int, list[dict[str, Any]]] = {}
    for e in base_events:
        base_by_tick.setdefault(e["tick"], []).append(e)

    out: list[dict[str, Any]] = []
    all_ticks = sorted(set(base_by_tick) | set(blocked_by_tick))
    for t in all_ticks:
        group = base_by_tick.get(t, [])
        inserts = blocked_by_tick.get(t)
        if not inserts:
            out.extend(group)
            continue

        # 旧顺序：同 tick 事件末尾若干条是阶段 6 的 send（每流至多一条，
        # 按 stream 1→2）。send_blocked 同属该序列，按 stream 归位即可。
        k = len(group)
        while k > 0 and group[k - 1]["type"] == "send":
            k -= 1
        prefix = group[:k]
        sender_items = group[k:] + inserts
        sender_items.sort(key=lambda e: e.get("stream", 0))
        out.extend(prefix)
        out.extend(sender_items)

    return out


def _build_occupancy(events: list[dict[str, Any]],
                     moving_stream: int) -> list[dict[str, Any]]:
    """从事件重建迁移流新链窗口占用的变化序列（按 tick 采样变化点）。"""
    live: set[int] = set()
    samples: list[dict[str, Any]] = [{
        "tick": 0, "ts": 0, "size": 0,
        "cap": NEW_LINK_CAP, "window": [],
    }]
    last_size = 0
    by_tick: dict[int, list[dict[str, Any]]] = {}
    for e in events:
        by_tick.setdefault(e["tick"], []).append(e)
    for tick in sorted(by_tick):
        for e in by_tick[tick]:
            if e.get("stream") != moving_stream:
                continue
            if e["type"] == "send" and e.get("attempt") == "new-trial":
                live.add(e["seq"])
            elif (e["type"] in ("deliver", "packet_discard")
                  and e.get("attempt") == "new-trial"):
                live.discard(e["seq"])
        size = len(live)
        if size != last_size or tick == 0:
            samples.append({
                "tick": tick,
                "ts": tick * TICK_MS,
                "size": size,
                "cap": NEW_LINK_CAP,
                "window": sorted(live),
            })
            last_size = size
    return samples


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
    blocked_spans: list[dict[str, Any]] | None = None,
    occupancy: list[dict[str, Any]] | None = None,
    stats: dict[str, Any] | None = None,
    compact: bool = False,
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
    # compact 事件下首末 blocked 记录不影响重建；两种模式结果一致。
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

    max_ts = max((e["ts"] for e in events), default=0)
    stats = stats or {}

    result: dict[str, Any] = {
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
            "compact": compact,
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
        "max_ts": max_ts,
        # —— 长时延紧凑时间线新增数据（均为追加字段，不破坏旧客户端）——
        "stats": stats,
        "blocked_spans": blocked_spans or [],
        "window_occupancy": occupancy or [],
    }

    # 常见命名别名，方便不同调用方/测试取用
    result["blocked_periods"] = result["blocked_spans"]
    result["block_windows"] = result["blocked_spans"]
    result["block_intervals"] = result["blocked_spans"]
    result["blocked_intervals"] = result["blocked_spans"]
    result["block_ranges"] = result["blocked_spans"]
    result["window_blocks"] = result["blocked_spans"]
    result["blockages"] = result["blocked_spans"]
    result["occupancy"] = result["window_occupancy"]
    result["processed_ticks"] = stats.get("processed_ticks")
    result["ticks_processed"] = stats.get("processed_ticks")
    result["actual_ticks"] = stats.get("processed_ticks")
    result["total_ticks"] = stats.get("total_ticks")
    result["skipped_ticks"] = stats.get("skipped_ticks")
    result["engine_stats"] = stats
    result["tick_stats"] = stats
    result["meta"] = {"simulation": stats, "stats": stats,
                      "processed_ticks": stats.get("processed_ticks")}
    result["performance"] = {"ticks_processed": stats.get("processed_ticks"),
                            "processed_ticks": stats.get("processed_ticks"),
                            "elapsed_ticks": stats.get("total_ticks")}
    return result
