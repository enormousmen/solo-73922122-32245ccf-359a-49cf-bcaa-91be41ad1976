/* 链路切换演练 —— 时间线回放引擎（纯原生 JS，无外部依赖）。
 *
 * 后端一次算好完整确定性时间线，前端按虚拟时间回放：
 *   send      → 报文出现在对应链路泳道，按 (now-send)/(arrive-send) 推进
 *   packet_arrive → 进入接收端重排序缓存（新链报文显示为“等待旧链排空”）
 *   deliver   → 按编号进入交付区（颜色即所走链路）
 *   packet_discard → 标记丢弃
 *   handover_* / send_blocked / stream_done → 写入事件日志与总览
 */
"use strict";

const $ = (sel) => document.querySelector(sel);
const CAP = window.NEW_LINK_CAP;

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

// ------------------------------------------------------------ 预置 & 表单
document.querySelectorAll(".preset-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    const p = window.PRESETS[btn.dataset.preset];
    $("#delay_l1_ms").value = p.delay_l1_ms;
    $("#delay_l2_ms").value = p.delay_l2_ms;
    $("#deadline_ms").value = p.deadline_ms;
    $("#moving_stream").value = String(p.moving_stream);
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
    play();
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
  if (result) { setupScene(); play(); }
});

// ------------------------------------------------------------ 场景搭建
function setupScene() {
  cancelAnimationFrame(rafId);
  evIndex = 0;
  packets = new Map();
  blockedSeqs = new Set();
  heldOffsetMap.clear();

  ["#overview-card", "#lanes-card", "#delivery-card", "#log-card", "#checks-card"]
    .forEach((s) => $(s).hidden = false);

  const cfg = result.config;
  $("#clock-total").textContent = result.max_ts + " ms";

  // ---- 总览
  const mv = result.streams.find((s) => s.is_moving);
  const outcomeBadge = mv.handover_outcome === "success"
    ? '<span class="badge success">切换成功</span>'
    : mv.handover_outcome === "rollback"
      ? '<span class="badge rollback">超时回退</span>'
      : '<span class="badge pending">进行中</span>';
  $("#overview").innerHTML = `
    <div class="metric"><b>流 ${mv.sid}：${mv.old_link} → ${mv.new_link}</b><span>被迁移的备份流</span></div>
    <div class="metric"><b>${outcomeBadge}</b><span>切换结果（回放结束落定）</span></div>
    <div class="metric"><b>L1 ${cfg.delay_l1_ms}ms / L2 ${cfg.delay_l2_ms}ms</b><span>两条链路单向延时</span></div>
    <div class="metric"><b>时限 ${cfg.deadline_ms}ms · 窗口上限 ${cfg.new_link_cap}</b><span>切换从 ${cfg.handover_start_ms}ms 开始</span></div>`;

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
  $("#deadline-meter").textContent = cfg.deadline_ms + " ms";
  $("#clock").textContent = "0 ms";
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

// ------------------------------------------------------------ 播放控制
function currentVirtual() {
  if (!result) return 0;
  if (speed === 0) return result.max_ts;
  return Math.min(result.max_ts,
    startVirt + (performance.now() - startWall) * speed);
}

function play() {
  startWall = performance.now();
  startVirt = 0;
  if (speed === 0) { seek(result.max_ts); return; }
  const loop = () => {
    const now = currentVirtual();
    seek(now);
    if (now < result.max_ts) rafId = requestAnimationFrame(loop);
  };
  rafId = requestAnimationFrame(loop);
}

function seek(nowMs) {
  // 消费所有 ts <= nowMs 的事件
  while (evIndex < result.timeline.length && result.timeline[evIndex].ts <= nowMs) {
    applyEvent(result.timeline[evIndex]);
    evIndex++;
  }
  renderMotion(nowMs);
  renderMeters(nowMs);
}

// ------------------------------------------------------------ 事件应用
function pktKey(stream, seq) { return `${stream}-${seq}`; }

function applyEvent(ev) {
  switch (ev.type) {
    case "send": {
      const key = pktKey(ev.stream, ev.seq);
      if (!packets.has(key)) {
        packets.set(key, {
          stream: ev.stream, seq: ev.seq,
          link: ev.link, attempt: ev.attempt,
          sendMs: ev.ts, arriveMs: ev.arrive_ms,
          state: "inflight", el: null,
        });
      } else {
        // 同一编号重传（回退补发）：重建为旧链在途报文
        packets.set(key, {
          stream: ev.stream, seq: ev.seq,
          link: ev.link, attempt: ev.attempt,
          sendMs: ev.ts, arriveMs: ev.arrive_ms,
          state: "inflight", el: null,
        });
      }
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
      ev.window.forEach((seq) => blockedSeqs.add(pktKey(ev.stream, seq)));
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
  const row = document.createElement("div");
  row.className = "log-row " + ev.type;
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

  // 用事件重建当前新链窗口占用
  let live = 0;
  const peakKey = new Set();
  for (const ev of result.timeline) {
    if (ev.ts > nowMs) break;
    if (ev.type === "send" && ev.attempt === "new-trial") peakKey.add(`${ev.stream}-${ev.seq}`);
    if ((ev.type === "deliver" || ev.type === "packet_discard") && ev.attempt === "new-trial") {
      peakKey.delete(`${ev.stream}-${ev.seq}`);
    }
  }
  live = peakKey.size;
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
