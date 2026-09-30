"""消息总线：把"通道"和"Agent 核心"解耦。

对应 nanobot：
    nanobot/bus/queue.py       —— MessageBus（inbound / outbound 两个 asyncio.Queue）
    nanobot/bus/events.py      —— InboundMessage / OutboundMessage 数据类

设计要点（与真实实现一致）：
    1. 通道只负责"收发字节"，不认识 Agent；
    2. Agent 只负责"处理消息"，不认识 WebSocket；
    3. 两条队列（入站/出站）让两侧可以独立阻塞、独立限流。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

# 事件名：与 nanobot WebUI 线上协议保持一致
EVENT_DELTA = "delta"           # 流式增量
EVENT_STREAM_END = "stream_end"  # 一段流结束
EVENT_MESSAGE = "message"        # 完整消息 / 进度行
EVENT_PROGRESS = "progress"      # 工具调用等中间态


@dataclass(slots=True)
class InboundMessage:
    """通道 → Agent。对标 nanobot/bus/events.py 的 InboundMessage。"""

    channel: str
    sender_id: str
    chat_id: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def session_key(self) -> str:
        """会话主键：nanobot 里 WebUI 会话是 `websocket:<chat_id>`。"""
        return f"{self.channel}:{self.chat_id}"


@dataclass(slots=True)
class OutboundMessage:
    """Agent → 通道。event 为空表示普通文本回复。"""

    channel: str
    chat_id: str
    content: str = ""
    event: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class MessageBus:
    """两条队列 + 发布/消费方法。对标 nanobot/bus/queue.py:19。"""

    def __init__(self) -> None:
        self.inbound: asyncio.Queue[InboundMessage] = asyncio.Queue()
        self.outbound: asyncio.Queue[OutboundMessage] = asyncio.Queue()

    # -- 入站（通道调用 publish，Agent 调用 consume） ----------------------

    async def publish_inbound(self, msg: InboundMessage) -> None:
        """对标 nanobot/bus/queue.py:37 `publish_inbound`。"""
        await self.inbound.put(msg)

    async def consume_inbound(self) -> InboundMessage:
        """对标 nanobot/bus/queue.py:41 `consume_inbound`。"""
        return await self.inbound.get()

    # -- 出站 ---------------------------------------------------------------

    async def publish_outbound(self, msg: OutboundMessage) -> None:
        """对标 nanobot/bus/queue.py:45 `publish_outbound`。"""
        await self.outbound.put(msg)

    async def consume_outbound(self) -> OutboundMessage:
        """对标 nanobot/bus/queue.py:67 `consume_outbound`。"""
        return await self.outbound.get()

    @property
    def inbound_size(self) -> int:
        return self.inbound.qsize()

    @property
    def outbound_size(self) -> int:
        return self.outbound.qsize()
