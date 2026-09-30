"""迷你 Agent 循环：消费入站消息，按真实协议产出流式回复。

对应 nanobot：
    nanobot/agent/loop.py:1310    AgentLoop.run()         消费入站
    nanobot/agent/loop.py:1439    create_task(...)        每条消息一个任务
    nanobot/agent/loop.py:1695    _process_message()      Turn 编排（七个阶段）
    nanobot/agent/runner.py:368   _run_core()             多轮 LLM + 工具
    nanobot/agent/loop.py:1964    _dispatch_command()     斜杠命令分流

这里刻意把 LLM 换成"假模型"，因为本实验室的重点是**通道与协议**，
不是 provider 接入。事件顺序与真实实现完全一致，所以前端代码可以复用。
"""

from __future__ import annotations

import asyncio
import time

from . import bus as bus_mod

# 一小段固定回复，用来模拟 LLM 的流式输出
_CANNED_REPLY = (
    "我是迷你版 nanobot。你刚才说的是「{content}」。"
    "这条回复是通过 delta → stream_end → message 三个事件流式送来的，"
    "和真实 nanobot 的 WebSocket 通道协议一致。"
)


class MiniAgentLoop:
    """对标 AgentLoop：只做"取消息 → 开任务 → 跑 Turn"。"""

    def __init__(self, bus: bus_mod.MessageBus) -> None:
        self.bus = bus
        self._running = False
        self._tasks: set[asyncio.Task[None]] = set()

    async def _publish(self, chat_id: str, content: str, event: str = "", **metadata: object) -> None:
        await self.bus.publish_outbound(bus_mod.OutboundMessage(
            channel="websocket",
            chat_id=chat_id,
            content=content,
            event=event,
            metadata=dict(metadata),
        ))

    async def _turn(self, msg: bus_mod.InboundMessage) -> None:
        """一条消息的完整处理。风格上对应 loop.py 的 _process_message。"""
        chat_id = msg.chat_id
        turn_id = str(msg.metadata.get("turn_id") or "")
        started = time.perf_counter()

        # ---- 阶段 1：中间态（进度提示，落在客户端的"从属轨迹"区域）--------
        await self._publish(chat_id, "正在思考…", bus_mod.EVENT_PROGRESS, kind="progress")

        # ---- 阶段 2：斜杠命令分流（对标 loop.py:1964 _dispatch_command）----
        if msg.content.startswith("/"):
            name = msg.content.split()[0]
            await self._publish(
                chat_id,
                f"（迷你版只识别 /help）你输入了 {name}",
                bus_mod.EVENT_MESSAGE,
                kind="tool_hint",
                turn_id=turn_id,
            )
            await self._finish(chat_id, turn_id, started)
            return

        # ---- 阶段 3：模拟一次工具调用 --------------------------------------
        await self._publish(
            chat_id,
            "调用工具 read_file(README.md)",
            bus_mod.EVENT_MESSAGE,
            kind="tool_hint",
            turn_id=turn_id,
        )
        await asyncio.sleep(0.05)

        # ---- 阶段 4：流式输出（delta 逐块 → stream_end 收尾）---------------
        reply = _CANNED_REPLY.format(content=msg.content)
        stream_id = f"stream-{chat_id[:8]}"
        chunks: list[str] = []
        for index in range(0, len(reply), 8):
            chunk = reply[index:index + 8]
            chunks.append(chunk)
            await self._publish(
                chat_id, chunk, bus_mod.EVENT_DELTA,
                stream_id=stream_id, turn_id=turn_id,
            )
            await asyncio.sleep(0.03)  # 模拟 token 生成节奏

        await self._publish(
            chat_id, "", bus_mod.EVENT_STREAM_END,
            stream_id=stream_id, turn_id=turn_id,
        )

        # ---- 阶段 5：最终消息（客户端以它为准落库）-------------------------
        await self._publish(
            chat_id, reply, bus_mod.EVENT_MESSAGE,
            turn_id=turn_id, latency_ms=int((time.perf_counter() - started) * 1000),
        )

    async def _finish(self, chat_id: str, turn_id: str, started: float) -> None:
        await self._publish(
            chat_id, "", bus_mod.EVENT_STREAM_END, turn_id=turn_id,
        )
        await self._publish(
            chat_id, "命令处理完成", bus_mod.EVENT_MESSAGE,
            turn_id=turn_id,
            latency_ms=int((time.perf_counter() - started) * 1000),
        )

    async def run(self) -> None:
        """主循环。对标 loop.py:1310。"""
        self._running = True
        print("[agent] 主循环已启动")
        while self._running:
            try:
                msg = await asyncio.wait_for(self.bus.consume_inbound(), timeout=1.0)
            except TimeoutError:
                continue
            # 每条消息一个任务：慢请求不会阻塞后续消息（对应 loop.py:1311 的注释
            # "dispatching messages as tasks to stay responsive to /stop"）
            task = asyncio.create_task(self._turn(msg))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def stop(self) -> None:
        self._running = False
        for task in tuple(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
