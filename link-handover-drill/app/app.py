"""Flask 本地演练页：链路切换（make-before-break）模拟。

路由：
  GET  /            演练页面
  POST /api/simulate  提交参数，返回完整确定性时间线
  GET  /api/health    健康检查（容器用）
"""

from __future__ import annotations

from flask import Flask, jsonify, render_template, request

from .simulator import (
    DEFAULT_PACKETS,
    NEW_LINK_CAP,
    run_simulation,
)

app = Flask(__name__)

# 参数边界（毫秒）
DELAY_MIN, DELAY_MAX = 50, 3000
DEADLINE_MIN, DEADLINE_MAX = 100, 5000

# 预置演练场景：覆盖 成功 / 回退 / 窗口打满暂停 三种关键路径
PRESETS = {
    "success": {
        "label": "场景A：正常切换成功（新链稍快，时限宽松）",
        "delay_l1_ms": 200,
        "delay_l2_ms": 150,
        "deadline_ms": 1200,
        "moving_stream": 1,
    },
    "rollback": {
        "label": "场景B：旧链拥塞超时 → 回退补发、丢弃新链迟到副本",
        "delay_l1_ms": 900,
        "delay_l2_ms": 100,
        "deadline_ms": 600,
        "moving_stream": 1,
    },
    "cap": {
        "label": "场景C：新链高延时，4 包窗口打满仅暂停迁移流",
        "delay_l1_ms": 100,
        "delay_l2_ms": 1200,
        "deadline_ms": 3000,
        "moving_stream": 1,
    },
}


def _validate_payload(data: dict) -> tuple[dict | None, str | None]:
    """校验并归一化请求参数。"""
    if not isinstance(data, dict):
        return None, "请求体必须是 JSON 对象"
    try:
        moving_stream = int(data.get("moving_stream", 1))
        delay_l1_ms = int(data.get("delay_l1_ms"))
        delay_l2_ms = int(data.get("delay_l2_ms"))
        deadline_ms = int(data.get("deadline_ms"))
    except (TypeError, ValueError):
        return None, "延时、时限和流编号必须为整数"

    if moving_stream not in (1, 2):
        return None, "只能选择流 1 或流 2"
    for name, val, lo, hi in (
        ("链路 L1 延时", delay_l1_ms, DELAY_MIN, DELAY_MAX),
        ("链路 L2 延时", delay_l2_ms, DELAY_MIN, DELAY_MAX),
        ("切换时限", deadline_ms, DEADLINE_MIN, DEADLINE_MAX),
    ):
        if not (lo <= val <= hi):
            return None, f"{name} 需在 {lo}–{hi} ms 之间（当前 {val}）"

    packets = int(data.get("packets_per_stream", DEFAULT_PACKETS))
    if not (1 <= packets <= 50):
        return None, "每条流的报文数需在 1–50 之间"

    return {
        "moving_stream": moving_stream,
        "delay_l1_ms": delay_l1_ms,
        "delay_l2_ms": delay_l2_ms,
        "deadline_ms": deadline_ms,
        "packets_per_stream": packets,
    }, None


@app.get("/")
def index():
    return render_template(
        "index.html",
        presets=PRESETS,
        cap=NEW_LINK_CAP,
        delay_min=DELAY_MIN,
        delay_max=DELAY_MAX,
        deadline_min=DEADLINE_MIN,
        deadline_max=DEADLINE_MAX,
    )


@app.get("/api/health")
def health():
    return jsonify({"ok": True})


@app.post("/api/simulate")
def simulate():
    data = request.get_json(silent=True)
    params, err = _validate_payload(data)
    if err:
        return jsonify({"error": err}), 400
    result = run_simulation(**params)
    return jsonify(result)


if __name__ == "__main__":
    # 本地开发直接 python -m app.app 启动；容器内由 gunicorn 启动
    app.run(host="0.0.0.0", port=8000, debug=False)
