"""协议层与通道行为的测试（只用标准库 unittest，无需安装依赖）。

覆盖五个最容易出错的点：
    1. WebSocket 握手（Sec-WebSocket-Accept 计算）与真实端到端收发
    2. 信封解析：什么算信封、什么算裸文本
    3. 订阅与 fan-out：一个 chat_id 多端同时收到
    4. 背压：队列超限要断开慢客户端，而不是拖垮服务
    5. 连接清理：断开后订阅表必须干净（否则内存泄漏）

运行：python -m unittest discover -s tests -v
（在 nanobot-ws-lab 目录下执行）
"""

from __future__ import annotations

import asyncio
import base64
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import bus as bus_mod
from src.agent import MiniAgentLoop
from src.channel import LocalConfig, MiniWebSocketChannel, _parse_envelope
from src.projector import project
from src.ws_proto import WSClient, accept_key


class FakeConnection:
    """替身连接：记录发出去的帧，不碰真实 socket。"""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = False
        self.close_reason = ""

    async def send_text(self, text: str) -> None:
        self.sent.append(text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = True
        self.close_reason = reason

    def json_events(self) -> list[dict]:
        return [json.loads(item) for item in self.sent]


class TestHandshake(unittest.TestCase):
    def test_accept_key_matches_rfc6455_example(self) -> None:
        # RFC 6455 第 1.3 节给出的官方示例值
        self.assertEqual(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        )


class TestEnvelope(unittest.TestCase):
    def test_valid_envelope(self) -> None:
        raw = '{"type":"message","chat_id":"c1","content":"hi"}'
        parsed = _parse_envelope(raw)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["type"], "message")

    def test_bare_text_is_not_envelope(self) -> None:
        self.assertIsNone(_parse_envelope("hello"))

    def test_json_without_type_is_not_envelope(self) -> None:
        self.assertIsNone(_parse_envelope('{"chat_id":"c1"}'))

    def test_broken_json_is_not_envelope(self) -> None:
        self.assertIsNone(_parse_envelope('{"type":'))


class TestProjector(unittest.TestCase):
    def test_delta_keeps_stream_id(self) -> None:
        msg = bus_mod.OutboundMessage(
            channel="websocket", chat_id="c1", content="ab",
            event=bus_mod.EVENT_DELTA, metadata={"stream_id": "s1"},
        )
        payload = project(msg)
        self.assertEqual(payload["event"], "delta")
        self.assertEqual(payload["text"], "ab")
        self.assertEqual(payload["stream_id"], "s1")

    def test_unknown_event_is_dropped(self) -> None:
        msg = bus_mod.OutboundMessage(channel="websocket", chat_id="c1", event="internal_sync")
        self.assertIsNone(project(msg))

    def test_tool_hint_marked_with_kind(self) -> None:
        msg = bus_mod.OutboundMessage(
            channel="websocket", chat_id="c1", content="read_file",
            event=bus_mod.EVENT_MESSAGE, metadata={"kind": "tool_hint"},
        )
        self.assertEqual(project(msg)["kind"], "tool_hint")


class TestChannelBehaviour(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.bus = bus_mod.MessageBus()
        self.channel = MiniWebSocketChannel(LocalConfig(), self.bus)
        # 测试里默认不需要真实写任务，关掉它以便直接观察队列状态
        self._stub_writer()

    def _stub_writer(self) -> None:
        self.channel._start_writer = lambda *args, **kwargs: None  # type: ignore[method-assign]

    def _use_real_writer(self) -> None:
        self.channel._start_writer = MiniWebSocketChannel._start_writer.__get__(self.channel)  # type: ignore[method-assign]

    async def test_handle_message_lands_on_bus(self) -> None:
        await self.channel._handle_message(sender_id="u1", chat_id="c1", content="hi")
        msg = await asyncio.wait_for(self.bus.consume_inbound(), timeout=1)
        self.assertEqual(msg.chat_id, "c1")
        self.assertEqual(msg.session_key, "websocket:c1")

    async def test_fan_out_to_all_subscribers(self) -> None:
        """一个 chat_id 被两个连接订阅 → 两边都要收到。"""
        self._use_real_writer()
        first, second = FakeConnection(), FakeConnection()
        self.channel._attach(first, "c1")  # type: ignore[arg-type]
        self.channel._attach(second, "c1")  # type: ignore[arg-type]

        await self.channel.send(bus_mod.OutboundMessage(
            channel="websocket", chat_id="c1", content="广播", event=bus_mod.EVENT_MESSAGE,
        ))
        await asyncio.sleep(0.05)

        for conn in (first, second):
            self.assertEqual(len(conn.sent), 1)
            self.assertEqual(conn.json_events()[0]["text"], "广播")

    async def test_backpressure_disconnects_slow_client(self) -> None:
        """队列装满后，继续投递必须触发断开，而不是无限堆积。"""
        self.channel.config.max_frames = 2
        conn = FakeConnection()
        self.channel._ensure_outbound(conn)  # type: ignore[arg-type]

        for index in range(2):
            await self.channel._safe_send_to(conn, f'{{"n":{index}}}')  # type: ignore[arg-type]
        state = self.channel._connection_outbound[conn]  # type: ignore[index]
        self.assertEqual(state.queue.qsize(), 2)

        await self.channel._safe_send_to(conn, '{"n":"overflow"}')  # type: ignore[arg-type]
        self.assertTrue(state.closing, "超限后应把连接标记为退役")
        await asyncio.sleep(0.05)
        self.assertTrue(conn.closed)

    async def test_byte_budget_disconnects_slow_client(self) -> None:
        """字节预算比帧数先超限时，同样要断开。"""
        self.channel.config.max_bytes = 32
        conn = FakeConnection()
        self.channel._ensure_outbound(conn)  # type: ignore[arg-type]

        await self.channel._safe_send_to(conn, "x" * 100)  # type: ignore[arg-type]
        state = self.channel._connection_outbound[conn]  # type: ignore[index]
        self.assertTrue(state.closing)

    async def test_cleanup_clears_subscriptions(self) -> None:
        conn = FakeConnection()
        self.channel._attach(conn, "c1")  # type: ignore[arg-type]
        self.channel._attach(conn, "c2")  # type: ignore[arg-type]

        await self.channel._cleanup_connection(conn)  # type: ignore[arg-type]

        self.assertEqual(self.channel.subscribers("c1"), ())
        self.assertEqual(self.channel.subscribers("c2"), ())
        self.assertEqual(self.channel._conn_chats, {})

    async def test_unknown_envelope_gets_error(self) -> None:
        self._use_real_writer()
        conn = FakeConnection()
        self.channel._ensure_outbound(conn)  # type: ignore[arg-type]
        await self.channel._dispatch_envelope(conn, "u1", {"type": "nope"})  # type: ignore[arg-type]
        await asyncio.sleep(0.05)
        event = conn.json_events()[0]
        self.assertEqual(event["event"], "error")


class TestEndToEnd(unittest.IsolatedAsyncioTestCase):
    async def test_full_round_trip_over_real_socket(self) -> None:
        """真 socket：握手 → ready → 发消息 → 收到流式 delta 与最终 message。"""
        bus = bus_mod.MessageBus()
        channel = MiniWebSocketChannel(LocalConfig(port=0), bus)
        agent = MiniAgentLoop(bus)
        await channel.start()

        port = channel._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        agent_task = asyncio.create_task(agent.run())

        try:
            client = WSClient(f"ws://127.0.0.1:{port}/?client_id=test-client")
            await client.connect()

            ready = await asyncio.wait_for(client.recv_json(), timeout=5)
            self.assertEqual(ready["event"], "ready")
            self.assertEqual(ready["client_id"], "test-client")
            chat_id = ready["chat_id"]

            await client.send_text(json.dumps(
                {"type": "message", "chat_id": chat_id, "content": "ping", "turn_id": "t1"},
            ))

            seen: list[str] = []
            final_text = ""
            for _ in range(200):
                event = await asyncio.wait_for(client.recv_json(), timeout=5)
                if event is None:
                    break
                seen.append(event["event"])
                if event["event"] == "message" and not event.get("kind"):
                    final_text = event["text"]
                    break

            await client.close()

            self.assertIn("delta", seen, "必须收到流式增量")
            self.assertIn("stream_end", seen, "必须收到流结束")
            self.assertIn("ping", final_text, "最终消息里应包含用户输入")
        finally:
            await agent.stop()
            await channel.stop()
            agent_task.cancel()
            await asyncio.gather(agent_task, return_exceptions=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
