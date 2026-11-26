/* 链路切换演练 —— 时间线回放引擎（纯原生 JS，无外部依赖）。
 *
 * 后端一次算好完整确定性时间线，前端按虚拟时间回放：
 *   send      → 报文出现在对应链路泳道，按 (now-send)/(arrive-send) 推进
 *   packet_arrive → 进入接收端重排序缓存（新链报文显示为“等待旧链排空”）
 *   deliver   → 按编号进入交付区（颜色即所走链路）
 *   packet_discard → 标记丢弃
 *   handover_* / send_blocked / stream_done → 写入事件日志与总览
 *
 * 长时延（≤60s）场景下，后端跳过空刻度并把连续窗口满压缩成
 * blocked_spans（首/末受阻刻度）。本文件额外渲染一条“紧凑时间线”：
 * 红带 = 受阻时段、阶梯条 = 窗口占用、竖线 = 切换节点、游标尺同步回放，
 * 并显示引擎实际处理的刻度数。
 */
"use strict";

const $ = (sel) => document.querySelector(sel);
const CAP = window.NEW_LINK_CAP;
const TICK_MS = 50;

let result = null;          // 最近一次模拟结果
let rafId = null;           // requestAnimationFrame 句柄
let speed = 4;              // 播放倍速，0 = 瞬时
let startWall = 0;          // 播放起始墙钟
let startVirt = 0;          // 播放起始虚拟时间(ms)
let evIndex = 0;            // 已消费事件下标
let packets = new Map();    // key: stream-seq → 运行态
let blockedSeqs = new Set();

const TYPE_TEXT = {
  send: "发送",
  packet_arrive: "到达接收端",
  deliver: "交付应用",
  packet_discard: "丢弃",
  handover_start: "▶ 切换开始",
  handover_success: "✔ 切换成功：旧链已排空，放行新链",
  handover_timeout: "✖ 切换超时：回退旧链补发",
  send_blocked: "⏸ 新链窗口满，仅暂停本流",
  stream_done: "■ 该流全部交付完成",
};
const ATTEMPT_TEXT = {
  original: "原发",
  "new-trial": "新链试发",
  "old-resend": "旧链补发",
};

// ------------------------------------------------------------ 工具
function fmtMs(ms) {
  const v = Math.round(Number(ms) || 0);
  if (Math.abs(v) < 1000) return v + "ms";
  const s = v / 1000;
  return (Math.round(s * 100) / 100) + "s";
}

function pctOf(ms) {
  const total = Math.max(1, result ? result.max_ts : 1);
  return Math.max(0, Math.min(100, 100 * ms / total));
}

// ------------------------------------------------------------ 预置 & 表单
document.querySelectorAll(".preset-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    const p = window.PRESETS[btn.dataset.preset];
    $("#delay_l1_ms").value = p.delay_l1_ms;
    $("#delay_l2_ms").value = p.delay_l2_ms;
    $("#deadline_ms").value = p.deadline_ms;
    $("#moving_stream").value = String(p.moving_stream);
    if (p.packets_per_stream) $("#packets_per_stream").value = p.packets_per_stream;
  });
});

document.querySelectorAll(".speed-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".speed-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    speed = Number(btn.dataset.speed);
    if (result) {
      // 以当前虚拟时间为新起点无缝变速
      startVirt = currentVirtual();
      startWall = performance.now();
      if (speed === 0) seek(result.max_ts);
    }
  });
});

$("#cfg-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  $("#form-error").hidden = true;
  const payload = {
    delay_l1_ms: Number($("#delay_l1_ms").value),
    delay_l2_ms: Number($("#delay_l2_ms").value),
    deadline_ms: Number($("#deadline_ms").value),
    moving_stream: Number($("#moving_stream").value),
    packets_per_stream: Number($("#packets_per_stream").value),
  };
  $("#run-btn").disabled = true;
  try {
    const resp = await fetch("/api/simulate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok) {
      const err = $("#form-error");
      err.textContent = data.error || "模拟失败";
      err.hidden = false;
      return;
    }
    result = data;
    setupScene();
    play(0);
    $("#replay-btn").disabled = false;
  } catch (err) {
    const box = $("#form-error");
    box.textContent = "请求失败：" + err.message;
    box.hidden = false;
  } finally {
    $("#run-btn").disabled = false;
  }
});

$("#replay-btn").addEventListener("click", () => {
  if (result) { setupScene(); play(0); }
});

// 点击受阻时段列表 → 跳到该时段起点继续播放
$("#span-list").addEventListener("click", (e) => {
  const chip = e.target.closest(".span-chip");
  if (!chip || !result) return;
  jumpTo(Number(chip.dataset.startMs || 0));
});

// ------------------------------------------------------------ 场景搭建
function setupScene() {
  cancelAnimationFrame(rafId);
  evIndex = 0;
  packets = new Map();
  blockedSeqs = new Set();
  heldOffsetMap.clear();

  ["#overview-card", "#compact-card", "#lanes-card", "#delivery-card",
   "#log-card", "#checks-card"].forEach((s) => $(s).hidden = false);

  const cfg = result.config;
  $("#clock-total").textContent = result.max_ts + " ms";

  // ---- 总览
  const mv = result.streams.find((s) => s.is_moving);
  const outcomeBadge = mv.handover_outcome === "success"
    ? '<span class="badge success">切换成功</span>'
    : mv.handover_outcome === "rollback"
      ? '<span class="badge rollback">超时回退</span>'
      : '<span class="badge pending">进行中</span>';
  const longest = longestSpan(result.blocked_spans);
  $("#overview").innerHTML = `
    <div class="metric"><b>流 ${mv.sid}：${mv.old_link} → ${mv.new_link}</b><span>被迁移的备份流</span></div>
    <div class="metric"><b>${outcomeBadge}</b><span>切换结果（回放结束落定）</span></div>
    <div class="metric"><b>L1 ${cfg.delay_l1_ms}ms / L2 ${cfg.delay_l2_ms}ms</b><span>两条链路单向延时</span></div>
    <div class="metric"><b>时限 ${cfg.deadline_ms}ms · 窗口上限 ${cfg.new_link_cap}</b><span>切换从 ${cfg.handover_start_ms}ms 开始</span></div>
    <div class="metric"><b>${longest ? fmtMs(longest.duration_ms) : "无窗口打满"}</b><span>${longest ? `最长受阻时段（tick ${longest.first_tick}–${longest.last_tick}）` : "本次没有连续受阻时段"}</span></div>`;

  renderEngineStats();
  renderCompactTimeline();
  renderSpanList();

  // ---- 泳道：每条流两行（L1/L2）
  const lanes = $("#lanes");
  lanes.innerHTML = "";
  result.streams.forEach((st) => {
    const block = document.createElement("div");
    block.className = "stream-block";
    const role = st.is_moving ? "（迁移流）" : "（不受影响的另一条流）";
    block.innerHTML = `<h3>流 ${st.sid} ${role} · 常驻 ${st.home_link}${st.is_moving ? `，目标 ${st.new_link}` : ""}</h3>
      <div class="lane"><div class="lane-name">L1 链路</div><div class="lane-track l1" id="track-${st.sid}-L1"></div></div>
      <div class="lane"><div class="lane-name">L2 链路</div><div class="lane-track l2" id="track-${st.sid}-L2"></div></div>`;
    lanes.appendChild(block);
  });

  // ---- 交付区
  const deliv = $("#delivery");
  deliv.innerHTML = "";
  result.streams.forEach((st) => {
    const box = document.createElement("div");
    box.className = "deliv-stream";
    box.innerHTML = `<h3>流 ${st.sid} 交付顺序（左→右即应用收到的顺序）</h3><div class="chips" id="chips-${st.sid}"></div>`;
    deliv.appendChild(box);
  });

  // ---- 日志、自检、进度
  $("#log").innerHTML = "";
  renderChecks();
  $("#progress").style.width = "0%";
  $("#cap-meter").textContent = `0/${CAP}`;
  $("#cap-meter").style.color = "";
  $("#deadline-meter").textContent = cfg.deadline_ms + " ms";
  $("#deadline-meter").style.color = "";
  $("#clock").textContent = "0 ms";
}

function longestSpan(spans) {
  if (!spans || !spans.length) return null;
  return spans.reduce((a, b) => (b.duration_ms > a.duration_ms ? b : a));
}

function renderEngineStats() {
  const s = result.stats || {};
  const processed = s.processed_ticks ?? result.processed_ticks ?? "—";
  const total = s.total_ticks ?? "—";
  const skipped = s.skipped_ticks ?? "—";
  const skipPct = (typeof skipped === "number" && total)
    ? Math.round(100 * skipped / total) : 0;
  const modeText = s.mode === "compact"
    ? "紧凑模式（受阻段只记首末刻度）"
    : "逐条事件模式（旧参数范围兼容输出）";
  $("#engine-stats").innerHTML = `
    <span class="stat">引擎实际处理刻度 <b>${processed}</b></span>
    <span class="stat">时间线刻度槽位 <b>${total}</b></span>
    <span class="stat">跳过空刻度 <b>${skipped}</b>（${skipPct}%）</span>
    <span class="stat">压缩受阻段 <b>${(result.blocked_spans || []).length}</b></span>
    <span class="stat">事件条数 <b>${result.events.length}</b></span>
    <span class="stat mode">${modeText}</span>`;
}

function renderChecks() {
  const ul = $("#checks");
  ul.innerHTML = "";
  result.checks.forEach((c) => {
    const li = document.createElement("li");
    li.className = c.ok ? "ok" : "fail";
    li.innerHTML = `<span class="mark">${c.ok ? "✔" : "✖"}</span>${c.name}
      <span class="detail">${c.detail}</span>`;
    ul.appendChild(li);
  });
}

// ------------------------------------------------------------ 紧凑时间线
function niceAxisStep(maxMs) {
  const candidates = [250, 500, 1000, 2000, 5000, 10000, 15000,
    30000, 60000, 120000, 300000];
  for (const step of candidates) {
    if (maxMs / step <= 12) return step;
  }
  return 600000;
}

function renderCompactTimeline() {
  const ct = $("#compact-timeline");
  ct.innerHTML = "";
  const mvSid = result.handover.moving_stream;
  const step = niceAxisStep(result.max_ts);

  // ---- 时间轴行
  const axisRow = document.createElement("div");
  axisRow.className = "ct-row axis-row";
  let ticksHtml = "";
  for (let t = 0; t <= result.max_ts; t += step) {
    ticksHtml += `<span class="ct-axis-tick" style="left:${pctOf(t)}%">${fmtMs(t)}</span>`;
  }
  axisRow.innerHTML = `<div class="ct-label">时间轴</div>
    <div class="ct-track ct-axis">${ticksHtml}</div>`;
  ct.appendChild(axisRow);

  // ---- 每条流一行：发送/交付刻度 + 受阻红带 + 切换标记
  result.streams.forEach((st) => {
    const row = document.createElement("div");
    row.className = "ct-row";
    row.innerHTML = `<div class="ct-label" title="流 ${st.sid}">流 ${st.sid}${st.is_moving ? " 🚚" : ""}</div>
      <div class="ct-track" id="ct-track-${st.sid}"></div>`;
    ct.appendChild(row);
    const track = row.querySelector(".ct-track");

    // 发送 / 交付刻度（细竖线）
    for (const ev of result.events) {
      if (ev.stream !== st.sid) continue;
      if (ev.type === "send") {
        addMark(track, ev.ts, "ct-mark send",
          `流${st.sid} #${ev.seq} 发送→${ev.link}（${ATTEMPT_TEXT[ev.attempt] || ev.attempt}）@${ev.ts}ms`);
      } else if (ev.type === "deliver") {
        addMark(track, ev.ts, "ct-mark deliver",
          `流${st.sid} #${ev.seq} 交付（${ev.link}）@${ev.ts}ms`);
      }
    }

    // 受阻时段红带
    for (const sp of (result.blocked_spans || [])) {
      if (sp.stream !== st.sid) continue;
      // 首刻度 → 释放刻度(last+1) 构成完整受阻区间
      const releaseMs = (sp.last_tick + 1) * TICK_MS;
      const left = pctOf(sp.first_ts);
      const width = pctOf(Math.min(releaseMs, result.max_ts)) - left;
      const band = document.createElement("div");
      band.className = "ct-band blocked";
      band.dataset.startMs = sp.first_ts;
      band.dataset.endMs = releaseMs;
      band.style.left = left + "%";
      band.style.width = Math.max(0.4, width) + "%";
      band.title = `流${sp.stream} 窗口 [${sp.window.join(",")}] 满\n`
        + `受阻 tick ${sp.first_tick}–${sp.last_tick}\n`
        + `时间 ${fmtMs(sp.first_ts)}–${fmtMs(sp.last_ts)}（释放于 ${fmtMs(releaseMs)}）\n`
        + `连续 ${sp.blocked_ticks} 个刻度（${fmtMs(sp.duration_ms)}）\n点击跳转到起点`;
      const wide = width > 12;
      band.innerHTML = wide
        ? `<span class="ct-band-label">窗口满 [${sp.window.join(",")}] · ${fmtMs(sp.duration_ms)} · tick ${sp.first_tick}–${sp.last_tick}</span>`
        : `<span class="ct-band-label short">#${sp.window[0]}… ·${sp.blocked_ticks}t</span>`;
      band.addEventListener("click", () => jumpTo(sp.first_ts));
      track.appendChild(band);
    }

    // 切换开始 / 成功 / 回退竖线（只标在迁移流行）
    if (st.sid === mvSid) {
      addMarker(track, result.config.handover_start_ms, "ct-marker start",
        `切换开始 @${result.config.handover_start_ms}ms`);
      if (result.handover.finish_ms != null) {
        const okOutcome = result.handover.outcome === "success";
        const cls = okOutcome ? "success" : "rollback";
        addMarker(track, result.handover.finish_ms, `ct-marker ${cls}`,
          `${okOutcome ? "切换成功" : "超时回退"} @${result.handover.finish_ms}ms`);
      }
    }

    const head = document.createElement("div");
    head.className = "ct-playhead";
    track.appendChild(head);
  });

  // ---- 窗口占用阶梯条（迁移流）
  const occRow = document.createElement("div");
  occRow.className = "ct-row occ-row";
  occRow.innerHTML = `<div class="ct-label">窗口占用<br><small>n/${CAP}</small></div>
    <div class="ct-track occ-track" id="ct-occ"></div>`;
  ct.appendChild(occRow);
  const occ = occRow.querySelector(".occ-track");
  const samples = result.window_occupancy || [];
  for (let i = 0; i < samples.length; i++) {
    const a = samples[i];
    const b = samples[i + 1];
    const endMs = b ? b.ts : result.max_ts;
    if (endMs <= a.ts) continue;
    const seg = document.createElement("div");
    const ratio = a.size / CAP;
    seg.className = "occ-seg " + (a.size >= CAP ? "full" : a.size > 0 ? "part" : "zero");
    seg.style.left = pctOf(a.ts) + "%";
    seg.style.width = Math.max(0, pctOf(endMs) - pctOf(a.ts)) + "%";
    seg.style.height = Math.max(2, ratio * 100) + "%";
    seg.title = `tick ${a.tick}（${fmtMs(a.ts)}）窗口占用 ${a.size}/${CAP} [${(a.window || []).join(",")}]`;
    occ.appendChild(seg);
  }
  const capLine = document.createElement("div");
  capLine.className = "occ-cap-line";
  capLine.title = `窗口上限 ${CAP}`;
  occ.appendChild(capLine);
  const occHead = document.createElement("div");
  occHead.className = "ct-playhead";
  occ.appendChild(occHead);
}

function addMark(track, ms, cls, title) {
  const m = document.createElement("span");
  m.className = cls;
  m.style.left = pctOf(ms) + "%";
  m.title = title;
  track.appendChild(m);
}

function addMarker(track, ms, cls, title) {
  if (ms == null) return;
  const m = document.createElement("span");
  m.className = cls;
  m.style.left = pctOf(ms) + "%";
  m.title = title;
  track.appendChild(m);
}

function renderSpanList() {
  const box = $("#span-list");
  const spans = result.blocked_spans || [];
  if (!spans.length) {
    box.innerHTML = `<span class="span-empty muted">本场景没有出现 4 包窗口打满的连续受阻时段。</span>`;
    return;
  }
  box.innerHTML = spans.map((sp, i) => `
    <button type="button" class="span-chip" data-start-ms="${sp.first_ts}">
      <b>受阻段 ${i + 1}</b> · 流${sp.stream} 窗口 [${sp.window.join(",")}]
      · tick <b>${sp.first_tick}–${sp.last_tick}</b>
      · ${fmtMs(sp.first_ts)}–${fmtMs(sp.last_ts)}
      · 持续 <b>${fmtMs(sp.duration_ms)}</b>（${sp.blocked_ticks} 刻度）
      <span class="jump">跳转 ▸</span>
    </button>`).join("");
}

function updateCompactPlayhead(nowMs) {
  if (!result) return;
  const x = pctOf(nowMs) + "%";
  document.querySelectorAll("#compact-timeline .ct-playhead").forEach((h) => {
    h.style.left = x;
  });
  document.querySelectorAll("#compact-timeline .ct-band.blocked").forEach((b) => {
    const s = Number(b.dataset.startMs);
    const e = Number(b.dataset.endMs);
    b.classList.toggle("active", nowMs >= s && nowMs <= e);
  });
}

// ------------------------------------------------------------ 播放控制
function currentVirtual() {
  if (!result) return 0;
  if (speed === 0) return result.max_ts;
  return Math.min(result.max_ts,
    startVirt + (performance.now() - startWall) * speed);
}

function play(fromMs = 0) {
  cancelAnimationFrame(rafId);
  startWall = performance.now();
  startVirt = fromMs;
  if (speed === 0) { seek(result.max_ts); return; }
  const loop = () => {
    const now = currentVirtual();
    seek(now);
    if (now < result.max_ts) rafId = requestAnimationFrame(loop);
  };
  rafId = requestAnimationFrame(loop);
}

/* 跳转到任意虚拟时刻：完整重建场景后快进消费 ≤targetMs 的事件，
 * 再从该时刻继续播放（用于点击受阻段）。 */
function jumpTo(targetMs) {
  if (!result) return;
  setupScene();
  const target = Math.max(0, Math.min(result.max_ts, targetMs));
  while (evIndex < result.timeline.length && result.timeline[evIndex].ts <= target) {
    applyEvent(result.timeline[evIndex]);
    evIndex++;
  }
  seek(target);
  play(target);
}

function seek(nowMs) {
  // 消费所有 ts <= nowMs 的事件
  while (evIndex < result.timeline.length && result.timeline[evIndex].ts <= nowMs) {
    applyEvent(result.timeline[evIndex]);
    evIndex++;
  }
  renderMotion(nowMs);
  renderMeters(nowMs);
  updateCompactPlayhead(nowMs);
}

// ------------------------------------------------------------ 事件应用
function pktKey(stream, seq) { return `${stream}-${seq}`; }

function applyEvent(ev) {
  switch (ev.type) {
    case "send": {
      const key = pktKey(ev.stream, ev.seq);
      // 同一编号重传（回退补发）：重建为旧链在途报文；首发则新建
      packets.set(key, {
        stream: ev.stream, seq: ev.seq,
        link: ev.link, attempt: ev.attempt,
        sendMs: ev.ts, arriveMs: ev.arrive_ms,
        state: "inflight", el: null,
      });
      blockedSeqs.delete(key);
      break;
    }
    case "packet_arrive": {
      const p = packets.get(pktKey(ev.stream, ev.seq));
      if (p) p.state = "held";
      break;
    }
    case "deliver": {
      const p = packets.get(pktKey(ev.stream, ev.seq));
      if (p) p.state = "delivered";
      addDeliveryChip(ev);
      break;
    }
    case "packet_discard": {
      const p = packets.get(pktKey(ev.stream, ev.seq));
      if (p) {
        p.state = "discarded";
        p.link = ev.link;
      }
      break;
    }
    case "send_blocked":
      (ev.window || []).forEach((seq) => blockedSeqs.add(pktKey(ev.stream, seq)));
      break;
    case "handover_start":
    case "handover_success":
    case "handover_timeout":
    case "stream_done":
      break;
  }
  logRow(ev);
}

function addDeliveryChip(ev) {
  const box = $(`#chips-${ev.stream}`);
  if (!box) return;
  const chip = document.createElement("span");
  chip.className = `chip show ${ev.link.toLowerCase()}`;
  chip.innerHTML = `<span class="n">#${ev.seq}</span>
    <small>${ev.link} · ${ATTEMPT_TEXT[ev.attempt] || ev.attempt}</small>`;
  box.appendChild(chip);
}

function logRow(ev) {
  const log = $("#log");
  if (!log) return;

  // 压缩模式每个受阻段有首/末两条事件：首条输出完整汇总，末条不重复刷屏
  if (ev.type === "send_blocked" && ev.compressed && ev.edge === "last") return;

  const row = document.createElement("div");
  row.className = "log-row " + ev.type + (ev.compressed ? " compressed-span" : "");
  const who = ev.stream ? `流${ev.stream} ` : "";
  let msg = TYPE_TEXT[ev.type] || ev.type;
  if (ev.type === "send") {
    msg += ` #${ev.seq} → ${ev.link}（${ATTEMPT_TEXT[ev.attempt]}），预计 ${ev.arrive_ms}ms 到达`;
  } else if (ev.type === "packet_arrive") {
    msg += ` #${ev.seq}（${ev.link}，${ATTEMPT_TEXT[ev.attempt]}）`;
  } else if (ev.type === "deliver") {
    msg += ` #${ev.seq}（经 ${ev.link}，${ATTEMPT_TEXT[ev.attempt]}）`;
  } else if (ev.type === "packet_discard") {
    msg += ` #${ev.seq}（${ev.link}）：${ev.reason}`;
  } else if (ev.type === "send_blocked" && ev.compressed) {
    msg += `：窗口内 [${ev.window.join(",")}] 相同且连续受阻，只记首末刻度 `
      + `tick ${ev.first_tick}→${ev.last_tick}`
      + `（${ev.first_ts}ms→${ev.last_ts}ms，共 ${ev.blocked_ticks} 刻度），等待交付腾出窗口或超时回退`;
  } else if (ev.type === "send_blocked") {
    msg += `：窗口内 ${ev.window.join(",")}，等待交付腾出窗口`;
  } else if (ev.type === "handover_start") {
    msg += `：${who}${ev.old_link} → ${ev.new_link}，旧链最后编号 #${ev.last_old_seq}，时限 ${ev.deadline_ms}ms`;
  } else if (ev.type === "handover_success") {
    msg += `：旧链 ${ev.old_link} 在途报文全部交付，新链缓存 [${ev.held_new_packets.join(",")}] 放行`;
  } else if (ev.type === "handover_timeout") {
    msg += `：旧链未在 ${ev.deadline_ms}ms 内排空，补发 [${ev.resend_seqs.join(",")}]，新链副本丢弃`;
  } else if (ev.type === "stream_done") {
    msg += `：共 ${ev.delivered} 包`;
  }
  row.innerHTML = `<span class="t">${ev.ts}ms</span><span>${who}${msg}</span>`;
  log.appendChild(row);
  log.scrollTop = log.scrollHeight;
}

// ------------------------------------------------------------ 画面渲染
function renderMotion(nowMs) {
  packets.forEach((p, key) => {
    const trackId = `#track-${p.stream}-${p.link}`;
    const track = $(trackId);
    if (!track) return;
    let el = p.el;
    if (!el || !el.isConnected || el.dataset.track !== trackId) {
      if (el) el.remove();
      el = document.createElement("div");
      el.className = "pkt";
      el.dataset.track = trackId;
      el.textContent = p.seq;
      el.title = `流${p.stream} 报文#${p.seq} · ${p.link} · ${ATTEMPT_TEXT[p.attempt]}`;
      track.appendChild(el);
      p.el = el;
    }
    // 先清掉旧的状态类
    el.className = "pkt " + p.link.toLowerCase();

    if (p.state === "inflight") {
      const ratio = Math.max(0, Math.min(1,
        (nowMs - p.sendMs) / Math.max(1, p.arriveMs - p.sendMs)));
      el.style.left = `calc(${ratio * 86}% + 4px)`;
      el.style.opacity = "1";
    } else if (p.state === "held") {
      // 到达后停在接收端一侧；同轨多包轻微错位避免完全重叠
      const siblings = heldOffsets(p);
      el.style.left = `calc(86% + ${siblings}px)`;
      el.classList.add("held");
      if (blockedSeqs.has(key)) el.classList.add("blocked");
    } else if (p.state === "delivered") {
      el.style.left = "calc(94% - 18px)";
      el.classList.add("delivered");
    } else if (p.state === "discarded") {
      el.classList.add("discarded");
      el.style.opacity = ".65";
    }
  });
}

const heldOffsetMap = new Map();
function heldOffsets(p) {
  // 每个 (stream,link) 缓存中最多放 CAP 个左右，按 seq 错位 8px
  const k = `${p.stream}-${p.link}`;
  let arr = heldOffsetMap.get(k);
  if (!arr) { arr = []; heldOffsetMap.set(k, arr); }
  const idx = arr.indexOf(p.seq);
  if (idx === -1) { arr.push(p.seq); return (arr.length - 1) * 8; }
  return idx * 8;
}

function renderMeters(nowMs) {
  const cfg = result.config;
  $("#clock").textContent = Math.round(nowMs) + " ms";
  $("#progress").style.width = (100 * nowMs / Math.max(1, result.max_ts)) + "%";

  // 优先用后端给的窗口占用采样取当前值；否则用事件重建
  let live = 0;
  if (Array.isArray(result.window_occupancy) && result.window_occupancy.length) {
    for (const s of result.window_occupancy) {
      if (s.ts <= nowMs) live = s.size;
      else break;
    }
  } else {
    const peakKey = new Set();
    for (const ev of result.timeline) {
      if (ev.ts > nowMs) break;
      if (ev.type === "send" && ev.attempt === "new-trial") peakKey.add(`${ev.stream}-${ev.seq}`);
      if ((ev.type === "deliver" || ev.type === "packet_discard") && ev.attempt === "new-trial") {
        peakKey.delete(`${ev.stream}-${ev.seq}`);
      }
    }
    live = peakKey.size;
  }
  $("#cap-meter").textContent = `${live}/${CAP}`;
  $("#cap-meter").style.color = live >= CAP ? "var(--bad)" : "";

  // 时限倒计时（切换开始后）
  const start = cfg.handover_start_ms;
  const mv = result.streams.find((s) => s.is_moving);
  if (nowMs < start) {
    $("#deadline-meter").textContent = `未开始（${start - Math.round(nowMs)}ms 后切换）`;
  } else if (mv.handover_ms !== null && nowMs >= mv.handover_ms) {
    $("#deadline-meter").textContent =
      `已于 ${mv.handover_ms}ms 落定：${mv.handover_outcome === "success" ? "成功" : "回退"}`;
  } else {
    const remain = cfg.deadline_ms - (nowMs - start);
    $("#deadline-meter").textContent = Math.max(0, Math.round(remain)) + " ms";
    $("#deadline-meter").style.color = remain < 200 ? "var(--bad)" : "";
  }
}
