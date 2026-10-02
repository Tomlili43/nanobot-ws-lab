"""pytest 版通道测试：专盯 channel.py 里的订阅簿记那一行。

    self._subs.setdefault(chat_id, set()).add(connection)      # src/channel.py:106

这行看着简单，其实一次干了四件事，每件事都对应一个真实踩过的坑：

    1. 用 `setdefault` 而不是 `self._subs[chat_id].add(...)`
       —— 第一次订阅某个 chat_id 时桶还不存在，用下标访问会直接 KeyError。
    2. 桶是 `set` 而不是 `list`
       —— 同一个连接重复 attach（浏览器重连、重复切会话）不能演变成重复投递。
    3. 键是 chat_id、值是"连接集合"
       —— 这是 fan-out 的前提：一个会话可以被 N 个连接同时订阅。
    4. 桶空了必须被摘掉（见 `_detach`）
       —— 否则长跑服务里 `_subs` 会攒下一堆空 set，属于内存泄漏。

运行：
    pytest tests/test_channel_subs.py -v

不依赖 pytest-asyncio：异步用例内部用 asyncio.run 驱动，装个 pytest 就能跑。
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import bus as bus_mod
from src.channel import LocalConfig, MiniWebSocketChannel
from src.ws_proto import WSConnection

# =====================================================================
# 测试脚手架
# =====================================================================


class FakeConnection:
    """替身连接：只记录发出去的帧，不碰真实 socket。"""

    def __init__(self, name: str = "conn") -> None:
        self.name = name
        self.sent: list[str] = []
        self.closed = False
        self.close_reason = ""

    async def send_text(self, text: str) -> None:
        self.sent.append(text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = True
        self.close_reason = reason

    def __repr__(self) -> str:  # 断言失败时看得懂是哪个连接
        return f"<FakeConnection {self.name}>"




@pytest.fixture
def channel() -> MiniWebSocketChannel:
    """一个离线的通道：默认打桩写任务，只观察订阅表与出站队列。"""
    ch = MiniWebSocketChannel(LocalConfig(), bus_mod.MessageBus())
    ch._start_writer = lambda *args, **kwargs: None  # type: ignore[method-assign]
    return ch


def run(coro):
    """在用例内部驱动协程，避免引入 pytest-asyncio 依赖。"""
    return asyncio.run(coro)


def out(chat_id: str, content: str) -> bus_mod.OutboundMessage:
    """造一条要下发给 chat_id 的出站消息。"""
    return bus_mod.OutboundMessage(
        channel="websocket",
        chat_id=chat_id,
        content=content,
        event=bus_mod.EVENT_MESSAGE,
    )


async def wait_until(predicate, *, timeout: float = 1.0) -> None:
    """等条件成立（真写任务是独立 task，不能只靠 sleep(0)）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)


# =====================================================================
# 一、这行的本体：建桶 + 加入 + 去重
# =====================================================================

def test_first_define(channel: MiniWebSocketChannel) -> None:
    async def body() -> None:
        reader, writer = await asyncio.open_connection(
        "127.0.0.1",
         9988,
        )
        conn = WSConnection(reader = reader,writer= writer)

        channel._subs.setdefault("chat_id", set()).add(conn)
        print(channel)
    run(body())

def test_first_attach_creates_bucket(channel: MiniWebSocketChannel) -> None:
    """第一次订阅必须能自己建桶（下标访问的写法在这里就会 KeyError）。"""

    async def body() -> None:
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]

        assert "c1" in channel._subs
        assert isinstance(channel._subs["c1"], set), "桶必须是 set，用 list 会失去去重"
        assert channel._subs["c1"] == {conn}

    run(body())


def test_attach_is_idempotent(channel: MiniWebSocketChannel) -> None:
    """重复 attach 同一个 (连接, chat_id) —— 幂等，不能撑大桶。"""

    async def body() -> None:
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]
        channel._attach(conn, "c1")  # type: ignore[arg-type]
        channel._attach(conn, "c1")  # type: ignore[arg-type]

        assert len(channel._subs["c1"]) == 1
        assert channel.subscribers("c1") == (conn,)

    run(body())


def test_multiple_connections_share_one_bucket(channel: MiniWebSocketChannel) -> None:
    """一个 chat_id 多个订阅者 —— fan-out 的基础。"""

    async def body() -> None:
        first, second = FakeConnection("first"), FakeConnection("second")
        channel._attach(first, "c1")  # type: ignore[arg-type]
        channel._attach(second, "c1")  # type: ignore[arg-type]

        assert channel._subs["c1"] == {first, second}
        assert set(channel.subscribers("c1")) == {first, second}

    run(body())


def test_buckets_are_isolated_per_chat_id(channel: MiniWebSocketChannel) -> None:
    """不同 chat_id 各自一个桶；同一连接可以同时待在多个桶里。"""

    async def body() -> None:
        first, second = FakeConnection("first"), FakeConnection("second")
        channel._attach(first, "c1")  # type: ignore[arg-type]
        channel._attach(first, "c2")  # type: ignore[arg-type]
        channel._attach(second, "c2")  # type: ignore[arg-type]

        assert channel._subs["c1"] == {first}
        assert channel._subs["c2"] == {first, second}
        # 反向索引必须同步维护，否则断开时清不干净
        assert channel._conn_chats[first] == {"c1", "c2"}  # type: ignore[index]
        assert channel._conn_chats[second] == {"c2"}  # type: ignore[index]

    run(body())


def test_attach_also_registers_outbound_queue(channel: MiniWebSocketChannel) -> None:
    """_attach 顺手建出站队列：没有 state 的连接会被 _safe_send_to 静默丢弃。"""

    async def body() -> None:
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]

        assert conn in channel._connection_outbound

    run(body())


# =====================================================================
# 二、这行的可观测结果：投递次数与扇出
# =====================================================================


def test_repeated_attach_delivers_only_once(channel: MiniWebSocketChannel) -> None:
    """set 语义的可观测后果：重复 attach 不能变成重复推送。"""

    async def body() -> None:
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]
        channel._attach(conn, "c1")  # type: ignore[arg-type]

        await channel.send(out("c1", "只应收到一次"))

        state = channel._connection_outbound[conn]  # type: ignore[index]
        assert state.queue.qsize() == 1

    run(body())


def test_send_fans_out_to_every_subscriber(channel: MiniWebSocketChannel) -> None:
    """三个连接订阅同一会话 → 三条出站队列各拿一帧。"""

    async def body() -> None:
        conns = [FakeConnection(f"tab{i}") for i in range(3)]
        for conn in conns:
            channel._attach(conn, "c1")  # type: ignore[arg-type]

        await channel.send(out("c1", "广播"))

        for conn in conns:
            assert channel._connection_outbound[conn].queue.qsize() == 1  # type: ignore[index]

    run(body())


def test_send_ignores_unsubscribed_chat(channel: MiniWebSocketChannel) -> None:
    """没人订阅的会话：不进任何队列，也不能抛异常。"""

    async def body() -> None:
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]

        await channel.send(out("nobody-here", "hi"))

        assert channel._connection_outbound[conn].queue.qsize() == 0  # type: ignore[index]

    run(body())


def test_attached_connection_really_receives_frame(channel: MiniWebSocketChannel) -> None:
    """接上真写任务：attach 之后 send 必须真的把帧写到连接上。"""

    async def body() -> None:
        channel._start_writer = MiniWebSocketChannel._start_writer.__get__(channel)  # type: ignore[method-assign]
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]

        await channel.send(out("c1", "hello"))
        await wait_until(lambda: len(conn.sent) == 1)

        assert len(conn.sent) == 1
        assert json.loads(conn.sent[0])["text"] == "hello"

    run(body())


# =====================================================================
# 三、桶的生命周期：空桶必须被摘掉，快照必须能安全遍历
# =====================================================================


def test_detach_removes_empty_bucket(channel: MiniWebSocketChannel) -> None:
    """最后一个订阅者离开时，桶本身要被摘掉 —— 否则 _subs 越攒越多。"""

    async def body() -> None:
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]
        channel._detach(conn, "c1")  # type: ignore[arg-type]

        assert "c1" not in channel._subs
        assert channel._conn_chats == {}

    run(body())


def test_detach_keeps_remaining_subscribers(channel: MiniWebSocketChannel) -> None:
    """只摘掉退订的那个，其余订阅者不受影响。"""

    async def body() -> None:
        leaving, staying = FakeConnection("leaving"), FakeConnection("staying")
        channel._attach(leaving, "c1")  # type: ignore[arg-type]
        channel._attach(staying, "c1")  # type: ignore[arg-type]

        channel._detach(leaving, "c1")  # type: ignore[arg-type]

        assert channel._subs["c1"] == {staying}
        # 反向索引里没有残留：chats 空了连键一起摘掉
        assert leaving not in channel._conn_chats

    run(body())


def test_attach_after_detach_re_registers(channel: MiniWebSocketChannel) -> None:
    """重新订阅（切回旧会话）必须能正常回到桶里。"""

    async def body() -> None:
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]
        channel._detach(conn, "c1")  # type: ignore[arg-type]
        channel._attach(conn, "c1")  # type: ignore[arg-type]

        assert channel._subs["c1"] == {conn}
        assert channel._conn_chats[conn] == {"c1"}  # type: ignore[index]

    run(body())


async def _cleanup_scenario(channel: MiniWebSocketChannel) -> None:
    """断开 first：它订阅的 c1 桶消失，c2 里只留下还在线的 second。"""
    first, second = FakeConnection("first"), FakeConnection("second")
    channel._attach(first, "c1")  # type: ignore[arg-type]
    channel._attach(first, "c2")  # type: ignore[arg-type]
    channel._attach(second, "c2")  # type: ignore[arg-type]

    await channel._cleanup_connection(first)  # type: ignore[arg-type]
    assert channel.subscribers("c1") == (), "断开后 c1 的桶必须消失"
    assert channel._subs["c2"] == {second}, "还在线的连接不能被误摘"

    await channel._cleanup_connection(second)  # type: ignore[arg-type]
    assert channel._subs == {}
    assert channel._conn_chats == {}


def test_cleanup_clears_every_bucket(channel: MiniWebSocketChannel) -> None:
    """断开一条连接：它订阅过的所有桶都要清干净，且不误伤别人。"""
    run(_cleanup_scenario(channel))


def test_subscribers_returns_snapshot(channel: MiniWebSocketChannel) -> None:
    """subscribers() 返回的是拷贝：扇出中途有人退订也不会炸。"""

    async def body() -> None:
        conn = FakeConnection()
        channel._attach(conn, "c1")  # type: ignore[arg-type]

        snapshot = channel.subscribers("c1")
        channel._detach(conn, "c1")  # type: ignore[arg-type]

        assert snapshot == (conn,), "快照必须与后续改动解耦"
        assert channel.subscribers("c1") == ()
        assert channel.subscribers("never-seen") == ()

    run(body())


if __name__ == "__main__":  # 也支持 python tests/test_channel_subs.py 直接跑
    raise SystemExit(pytest.main([__file__, "-v"]))
