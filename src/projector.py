"""出站投影器：把内部事件翻译成浏览器能懂的 JSON 包裹（wire payload）。

对应 nanobot：
    nanobot/webui/outbound_projection.py  WebUIOutboundProjector.send()  事件分发
    nanobot/webui/outbound_wire.py        WebUIWirePayload / project_tool_events()
    nanobot/channels/websocket/runtime.py send_projected_message()        具体序列化

为什么要单独一层？
    内部事件（进度、思考、delta、file_edit、goal_state...）有十几种，
    但线上协议是"稳定契约"——不能因为内部多加一种事件就污染客户端。
    投影层就是这条分界线：**内部怎么变，客户端看到的字段不变。**
"""

from __future__ import annotations

from typing import Any

from . import bus as bus_mod


def project(msg: bus_mod.OutboundMessage) -> dict[str, Any] | None:
    """把一条 OutboundMessage 投影成 wire payload。

    返回 None 表示"这条事件不需要下发给客户端"（例如内部同步事件）。

    对照 nanobot/channels/websocket/runtime.py 的 send_projected_message()：
        payload = {"event": "message", "chat_id": ..., "text": ...}
        if progress_event and progress_event.tool_hint: payload["kind"] = "tool_hint"
        elif progress_event: payload["kind"] = "progress"
    """
    payload: dict[str, Any] = {
        "event": msg.event or bus_mod.EVENT_MESSAGE,
        "chat_id": msg.chat_id,
    }

    if msg.event == bus_mod.EVENT_DELTA:
        payload["text"] = msg.content
        # stream_id 用于同一条消息内多段流的"就地更新"（nanobot 同样带这个字段）
        stream_id = msg.metadata.get("stream_id")
        if stream_id is not None:
            payload["stream_id"] = stream_id
        return payload

    if msg.event == bus_mod.EVENT_STREAM_END:
        stream_id = msg.metadata.get("stream_id")
        if stream_id is not None:
            payload["stream_id"] = stream_id
        if msg.content:
            # 收尾时补发全文，客户端可据此校正因丢帧造成的偏差
            payload["text"] = msg.content
        if msg.metadata.get("resuming"):
            payload["resuming"] = True
        return payload

    if msg.event == bus_mod.EVENT_PROGRESS:
        payload["text"] = msg.content
        payload["kind"] = msg.metadata.get("kind", "progress")
        return payload

    if msg.event == bus_mod.EVENT_MESSAGE:
        payload["text"] = msg.content
        # 中间态面包屑（如工具调用提示）标记为 tool_hint，
        # 让客户端渲染成"从属轨迹"而不是一条正式回复
        kind = msg.metadata.get("kind")
        if kind:
            payload["kind"] = kind
        turn_id = msg.metadata.get("turn_id")
        if turn_id:
            payload["turn_id"] = turn_id
        latency = msg.metadata.get("latency_ms")
        if isinstance(latency, (int, float)):
            payload["latency_ms"] = int(latency)
        return payload

    # 未知事件：不认识的类型直接丢弃，保证客户端契约稳定
    return None
