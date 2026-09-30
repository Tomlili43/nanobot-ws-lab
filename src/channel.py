"""迷你 WebSocket 通道：nanobot WebSocketChannel 的教学版还原。

逐块对照（左边是 nanobot 真实位置，右边是本文件）：

    nanobot/channels/websocket/runtime.py:174  WebSocketConfig          → LocalConfig
    nanobot/channels/websocket/runtime.py:348  _OutboundFrame           → _Frame
    nanobot/channels/websocket/runtime.py:355  _ConnectionOutbound      → _ConnOutbound
    nanobot/channels/websocket/runtime.py:363  class WebSocketChannel   → MiniWebSocketChannel
    runtime.py:473  _attach()                     → _attach()
    runtime.py:794  _connection_loop()            → _connection_loop()
    runtime.py:836  _parse_envelope()             → _parse_envelope()
    runtime.py:861  _dispatch_envelope()          → _dispatch_envelope()
    runtime.py:450  webui_dispatch_message()      → _handle_message()
    runtime.py:1052 _safe_send_to()               → _safe_send_to()   ★背压核心
    runtime.py:946  _drain_connection_outbound()  → _drain_connection_outbound()
    runtime.py:509  _cleanup_connection()         → _cleanup_connection()
    runtime.py:1175 send()                        → send()

三个必须掌握的设计：
    1) 订阅模型：chat_id → 连接集合，"一个会话多端同时看"靠 fan-out；
    2) 每连接独立出站队列：慢客户端只拖慢自己，阻塞不了别人（head-of-line isolation）；
    3) 有界队列 + 字节预算 + 发送超时：超限就断开慢客户端，防止内存被流式输出撑爆。
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from . import bus as bus_mod
from . import projector
from .ws_proto import WSConnection, start_server


@dataclass
class LocalConfig:
    """对标 nanobot/channels/websocket/runtime.py:174 WebSocketConfig。"""

    host: str = "127.0.0.1"
    port: int = 9988
    path: str = "/"
    # 下面三个数字直接抄真实实现的量级思路（教学版调小，方便触发）
    max_frames: int = 256          # 真实值 256 帧
    max_bytes: int = 8 * 1024 * 1024  # 真实值 8 MiB
    send_timeout_s: float = 10.0   # 真实值 10 秒


@dataclass(slots=True)
class _Frame:
    """待发送的一帧。对标 runtime.py:348 _OutboundFrame。"""

    raw: str
    utf8_bytes: int
    label: str = ""


@dataclass
class _ConnOutbound:
    """单连接的出站状态。对标 runtime.py:355 _ConnectionOutbound。"""

    queue: asyncio.Queue[_Frame]
    buffered_bytes: int = 0
    writer: asyncio.Task[None] | None = None
    closing: bool = False


class MiniWebSocketChannel:
    """一个只做四件事的通道：握手、路由、订阅、投递。"""

    name = "websocket"

    def __init__(self, config: LocalConfig, bus: bus_mod.MessageBus) -> None:
        self.config = config
        self.bus = bus
        # chat_id -> 订阅它的连接（fan-out 目标）。对标 runtime.py:377 `_subs`
        self._subs: dict[str, set[WSConnection]] = {}
        # 连接 -> 它订阅的 chat_id 集合（断开时 O(1) 清理）。对标 `_conn_chats`
        self._conn_chats: dict[WSConnection, set[str]] = {}
        # 连接 -> 默认 chat_id（给不带路由的旧式裸文本帧兜底）。对标 `_conn_default`
        self._conn_default: dict[WSConnection, str] = {}
        # 连接 -> 出站队列状态。对标 `_connection_outbound`
        self._connection_outbound: dict[WSConnection, _ConnOutbound] = {}
        self._server: asyncio.Server | None = None
        self._running = False
        self._tasks: set[asyncio.Task[Any]] = set()

    # =====================================================================
    # 订阅簿记（runtime.py:451-509）
    # =====================================================================

    def subscribers(self, chat_id: str) -> tuple[WSConnection, ...]:
        """返回某会话当前订阅者的稳定快照（务必先拷贝再遍历）。"""
        return tuple(self._subs.get(chat_id, ()))

    def _attach(self, connection: WSConnection, chat_id: str) -> None:
        """把连接订阅到 chat_id。幂等，可重复调用。

        注意必须先确保出站队列存在——否则 `_safe_send_to` 会因为找不到 state
        而静默丢弃所有消息（这是最容易踩的坑，真实实现对应
        runtime.py:473 `_attach` 里的 `_register_connection_outbound`）。
        """
        self._ensure_outbound(connection)
        self._subs.setdefault(chat_id, set()).add(connection)
        self._conn_chats.setdefault(connection, set()).add(chat_id)

    def _detach(self, connection: WSConnection, chat_id: str) -> None:
        chats = self._conn_chats.get(connection)
        if chats is not None:
            chats.discard(chat_id)
            if not chats:
                self._conn_chats.pop(connection, None)
        subscribers = self._subs.get(chat_id)
        if subscribers is not None:
            subscribers.discard(connection)
            if not subscribers:
                self._subs.pop(chat_id, None)

    # =====================================================================
    # 出站：每连接一个队列 + 一个写任务（runtime.py:921-1050）
    # =====================================================================

    def _ensure_outbound(self, connection: WSConnection) -> _ConnOutbound:
        state = self._connection_outbound.get(connection)
        if state is None:
            state = _ConnOutbound(
                queue=asyncio.Queue(maxsize=self.config.max_frames),
            )
            self._connection_outbound[connection] = state
        return state

    def _start_writer(self, connection: WSConnection, state: _ConnOutbound) -> None:
        if state.closing or (state.writer is not None and not state.writer.done()):
            return
        state.writer = asyncio.create_task(
            self._drain_connection_outbound(connection, state),
            name=f"ws-outbound-{id(connection):x}",
        )

    async def _drain_connection_outbound(
        self,
        connection: WSConnection,
        state: _ConnOutbound,
    ) -> None:
        """单连接的写循环：只在有帧时存活，发完就退出（避免空转）。

        对标 runtime.py:946 `_drain_connection_outbound`。
        """
        try:
            while not state.closing:
                try:
                    frame = state.queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    async with asyncio.timeout(self.config.send_timeout_s):
                        await connection.send_text(frame.raw)
                except (TimeoutError, OSError):
                    # 发送超时/写失败 → 判定为慢客户端，直接退役这条连接
                    self._retire(connection, state, reason="outbound send failed")
                    return
                finally:
                    state.buffered_bytes = max(0, state.buffered_bytes - frame.utf8_bytes)
        finally:
            if state.writer is asyncio.current_task():
                state.writer = None
            # 竞态兜底：期间又有新帧入队，补起一个新写任务
            if not state.closing and not state.queue.empty():
                self._start_writer(connection, state)

    async def _safe_send_to(self, connection: WSConnection, raw: str, *, label: str = "") -> None:
        """把一帧塞进该连接的队列——**绝不会**因为某个客户端慢而卡住全局。

        对标 runtime.py:1052 `_safe_send_to`。这是整个通道最关键的函数：
            1. 帧数上限（max_frames）—— 防止帧堆积
            2. 字节预算（max_bytes）—— 防止大 payload 撑爆内存
            3. 超限即断开该连接，其它客户端完全不受影响
        """
        state = self._connection_outbound.get(connection)
        if state is None or state.closing:
            return

        utf8_bytes = len(raw.encode("utf-8"))
        if state.queue.full() or state.buffered_bytes + utf8_bytes > self.config.max_bytes:
            print(
                f"[backpressure] 断开慢客户端: {state.queue.qsize()} 帧, "
                f"{state.buffered_bytes} 字节待发"
            )
            self._retire(connection, state, reason="outbound queue full")
            return

        try:
            state.queue.put_nowait(_Frame(raw, utf8_bytes, label))
        except asyncio.QueueFull:
            self._retire(connection, state, reason="outbound queue full")
            return

        state.buffered_bytes += utf8_bytes
        self._start_writer(connection, state)
        await asyncio.sleep(0)  # 让空闲的写任务有机会立刻启动

    def _retire(self, connection: WSConnection, state: _ConnOutbound, *, reason: str) -> None:
        """标记退役并异步收尾。对标 runtime.py:1023 `_retire_connection`。"""
        if state.closing:
            return
        state.closing = True
        task = asyncio.create_task(self._retire_connection(connection, reason=reason))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _retire_connection(self, connection: WSConnection, *, reason: str) -> None:
        try:
            await connection.close(code=1013, reason=reason)
        finally:
            await self._cleanup_connection(connection)

    # =====================================================================
    # 连接生命周期（runtime.py:509 / 794）
    # =====================================================================

    async def _cleanup_connection(self, connection: WSConnection) -> None:
        """幂等清理：从所有订阅集合摘除，取消写任务。对标 runtime.py:509。"""
        state = self._connection_outbound.get(connection)
        if state is not None:
            state.closing = True
            writer = state.writer
            if writer is not None and writer is not asyncio.current_task() and not writer.done():
                writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
            while True:
                try:
                    state.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            state.buffered_bytes = 0
            self._connection_outbound.pop(connection, None)

        for chat_id in tuple(self._conn_chats.get(connection, ())):
            self._detach(connection, chat_id)
        self._conn_default.pop(connection, None)

    async def _connection_loop(self, connection: WSConnection) -> None:
        """一条连接的一生：握手 → ready → 收帧循环 → 清理。

        对标 runtime.py:794 `_connection_loop`。
        """
        # 1) client_id 来自 ?client_id=xxx，用于 allowFrom 鉴权；缺省则生成
        client_id = connection.query_first("client_id").strip()
        if not client_id:
            client_id = f"anon-{uuid.uuid4().hex[:12]}"

        # 2) 每条连接先拿到一个默认 chat_id（一个连接 = 一个会话）
        default_chat_id = str(uuid.uuid4())

        try:
            # 3) 先发 ready，再登记：顺序反了会出现"已登记但客户端没收到 chat_id"
            await connection.send_text(json.dumps(
                {"event": "ready", "chat_id": default_chat_id, "client_id": client_id},
                ensure_ascii=False,
            ))
            self._conn_default[connection] = default_chat_id
            self._ensure_outbound(connection)
            self._attach(connection, default_chat_id)

            # 4) 收帧循环
            async for raw in connection:
                envelope = _parse_envelope(raw)
                if envelope is not None:
                    await self._dispatch_envelope(connection, client_id, envelope)
                    continue

                # 5) 兜底：不带信封的旧式裸文本帧，直接当一条消息
                content = raw.strip()
                if not content:
                    continue
                await self._handle_message(
                    sender_id=client_id,
                    chat_id=default_chat_id,
                    content=content,
                )
        except Exception as exc:  # 单连接异常不得影响服务
            print(f"[connection] ended: {exc}")
        finally:
            await self._cleanup_connection(connection)

    # =====================================================================
    # 入站路由（runtime.py:836-878 → webui/inbound_commands.py:473）
    # =====================================================================

    async def _dispatch_envelope(
        self,
        connection: WSConnection,
        client_id: str,
        envelope: dict[str, Any],
    ) -> None:
        """按 type 字段分发。对标 runtime.py:861 + webui/inbound_commands.py:473。"""
        kind = envelope.get("type")

        if kind == "message":
            # 真正的用户输入：对标 inbound_commands.py:482 _dispatch_message
            await self._dispatch_message(connection, client_id, envelope)
            return

        if kind == "attach":
            # 切会话/订阅另一个 chat_id
            chat_id = envelope.get("chat_id")
            if isinstance(chat_id, str) and chat_id:
                self._attach(connection, chat_id)
                await self._send_event(connection, "attached", chat_id=chat_id)
            return

        if kind == "ping":
            await self._send_event(connection, "pong", echo=envelope.get("echo"))
            return

        await self._send_event(connection, "error", detail=f"unknown type: {kind!r}")

    async def _dispatch_message(
        self,
        connection: WSConnection,
        client_id: str,
        envelope: dict[str, Any],
    ) -> None:
        """校验后交给通道基类逻辑。对标 inbound_commands.py:482 _dispatch_message。"""
        chat_id = envelope.get("chat_id")
        content = envelope.get("content")
        if not isinstance(chat_id, str) or not chat_id:
            await self._send_event(connection, "error", detail="invalid chat_id")
            return
        if not isinstance(content, str):
            await self._send_event(connection, "error", detail="missing content")
            return
        await self._handle_message(
            sender_id=client_id,
            chat_id=chat_id,
            content=content,
            metadata={"turn_id": envelope.get("turn_id"), "webui": True},
        )

    async def _handle_message(
        self,
        *,
        sender_id: str,
        chat_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """构造 InboundMessage 并投入总线。

        对标 nanobot/channels/base.py:255 `_handle_message`（:310 构造、:321 入队）。
        真实实现里这一步之前还有 is_allowed() 鉴权 / allowFrom / pairing 检查。
        """
        msg = bus_mod.InboundMessage(
            channel=self.name,
            sender_id=str(sender_id),
            chat_id=str(chat_id),
            content=content,
            metadata=dict(metadata or {}),
        )
        await self.bus.publish_inbound(msg)

    async def _send_event(self, connection: WSConnection, event: str, **fields: Any) -> None:
        """发送一条控制事件。对标 runtime.py:530 `_send_event`。"""
        payload = {"event": event, **fields}
        await self._safe_send_to(
            connection,
            json.dumps(payload, ensure_ascii=False),
            label=f" {event} ",
        )

    # =====================================================================
    # 出站投递（runtime.py:1175 send → 投影 → fan-out）
    # =====================================================================

    async def send(self, msg: bus_mod.OutboundMessage) -> None:
        """投影 + 扇出。对标 runtime.py:1175 `send` + outbound_projection.py:138。

        注意这里的 fan-out：**一个 chat_id 可以被多个连接订阅**，
        所以 3 个浏览器标签页开着同一个会话，消息会同时到达 3 个连接。
        """
        payload = projector.project(msg)
        if payload is None:
            return
        raw = json.dumps(payload, ensure_ascii=False)

        connections = self.subscribers(msg.chat_id)
        if not connections:
            # 没有订阅者：真实实现会打日志（不同事件日志级别不同）
            print(f"[send] no subscribers for chat_id={msg.chat_id}")
            return
        for connection in connections:
            await self._safe_send_to(connection, raw, label=f" {payload['event']} ")

    async def dispatch_outbound(self) -> None:
        """出站分发循环。对标 nanobot/channels/manager.py:767 `_dispatch_outbound_loop`。"""
        while self._running:
            try:
                msg = await asyncio.wait_for(self.bus.consume_outbound(), timeout=1.0)
            except TimeoutError:
                continue
            await self.send(msg)

    # =====================================================================
    # 服务生命周期（runtime.py:678 start / 886 stop）
    # =====================================================================

    async def start(self, http_responder=None) -> None:
        self._running = True
        self._server = await start_server(
            self._connection_loop,
            self.config.host,
            self.config.port,
            http_responder=http_responder,
        )
        print(f"[channel] WebSocket 监听 ws://{self.config.host}:{self.config.port}{self.config.path}")
        asyncio.create_task(self.dispatch_outbound())

    async def stop(self) -> None:
        self._running = False
        for connection in tuple(self._connection_outbound):
            await self._cleanup_connection(connection)
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        self._subs.clear()
        self._conn_chats.clear()
        self._conn_default.clear()


def _parse_envelope(raw: str) -> dict[str, Any] | None:
    """尝试把一帧解析成 JSON 信封；不像信封就返回 None。

    对标 runtime.py:836 `_parse_envelope`。
    """
    text = raw.strip()
    if not text.startswith("{"):
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict) or "type" not in parsed:
        return None
    return parsed
