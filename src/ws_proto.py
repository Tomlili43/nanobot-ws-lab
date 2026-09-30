"""WebSocket 传输层：用标准库手写握手与帧编解码（教学用）。

为什么要手写？
    nanobot 真实实现直接依赖第三方库 `websockets`
    （见 nanobot/channels/websocket/runtime.py 的 import：
     `from websockets.asyncio.server import Server, ServerConnection, serve, unix_serve`）。
    库把握手和帧格式藏起来了，而理解这两件事恰恰是掌握 WS 通道的关键。
    这里用 ~200 行标准库代码把它们摊开，之后看真实代码就不会"隔一层纱"。

对应关系：
    nanobot: websockets.asyncio.server.serve(...)          → 本文件 start_server()
    nanobot: ServerConnection.send(raw)                     → 本文件 WSConnection.send_text()
    nanobot: `async for raw in connection`                  → 本文件 WSConnection.recv_text()
    nanobot: process_request（同端口跑普通 HTTP）            → 本文件 _handle_plain_http()
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import struct
from contextlib import suppress
from typing import Awaitable, Callable
from urllib.parse import parse_qs, urlsplit

# RFC 6455 规定的固定 GUID：Sec-WebSocket-Accept = base64(sha1(key + GUID))
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


def accept_key(sec_websocket_key: str) -> str:
    """计算握手响应里的 Sec-WebSocket-Accept。"""
    digest = hashlib.sha1((sec_websocket_key + WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def encode_frame(opcode: int, payload: bytes, *, fin: bool = True, mask: bool = False) -> bytes:
    """把一条消息编码成 WS 帧。

    帧头结构（第一个字节：FIN + opcode；第二个字节：MASK + 长度）：
        0                   1                   2                   3
        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
       +-+-+-+-+-------+-+-------------+-------------------------------+
       |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
       |I|S|S|S|  (4)  |A|     (7)     | (if len==126/127, 16/64 bit) |
       |N|V|V|V|       |S|             |                               |
       +-+-+-+-+-------+-+-------------+-------------------------------+

    规则：客户端 → 服务端的帧**必须**掩码（这里的 mask=True），反向则必须不掩码。
    """
    b0 = (0x80 if fin else 0x00) | opcode
    length = len(payload)
    mask_bit = 0x80 if mask else 0x00

    if length < 126:
        header = struct.pack("!BB", b0, mask_bit | length)
    elif length < 65536:
        header = struct.pack("!BBH", b0, mask_bit | 126, length)
    else:
        header = struct.pack("!BBQ", b0, mask_bit | 127, length)

    if not mask:
        return header + payload

    mask_key = b"\x37\xfa\x21\x3d"  # 教学实现固定掩码；真实客户端应使用随机 4 字节
    masked = bytes(byte ^ mask_key[i % 4] for i, byte in enumerate(payload))
    return header + mask_key + masked


async def read_frame(reader: asyncio.StreamReader) -> tuple[bool, int, bytes]:
    """从流里读出一条原始帧，返回 (fin, opcode, payload)。"""
    head = await reader.readexactly(2)
    b0, b1 = head[0], head[1]
    fin = bool(b0 & 0x80)
    opcode = b0 & 0x0F
    masked = bool(b1 & 0x80)
    length = b1 & 0x7F

    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]

    mask_key = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(length) if length else b""
    if masked:
        payload = bytes(byte ^ mask_key[i % 4] for i, byte in enumerate(payload))
    return fin, opcode, payload


class WSConnection:
    """一条已握手的 WebSocket 连接（服务端视角）。"""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        path: str = "/",
        query: dict[str, list[str]] | None = None,
        remote: str = "",
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.path = path
        self.query = query or {}
        self.remote = remote
        self.closed = False
        self._frag_parts: list[bytes] = []

    # -- 查询参数（对标 nanobot 的 query_first(query, "client_id")） --------

    def query_first(self, name: str) -> str:
        values = self.query.get(name)
        return values[0] if values else ""

    # -- 发送 ---------------------------------------------------------------

    async def _write_frame(self, opcode: int, payload: bytes, *, fin: bool = True) -> None:
        self.writer.write(encode_frame(opcode, payload, fin=fin, mask=False))
        await self.writer.drain()

    async def send_text(self, text: str) -> None:
        """发送一条文本消息（nanobot 里对应 `connection.send(raw)`）。"""
        if self.closed:
            return
        await self._write_frame(OP_TEXT, text.encode("utf-8"))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self.closed:
            return
        self.closed = True
        payload = struct.pack("!H", code) + reason.encode("utf-8")
        with suppress(Exception):
            await self._write_frame(OP_CLOSE, payload)
        with suppress(Exception):
            self.writer.close()

    # -- 接收 ---------------------------------------------------------------

    async def _answer_close(self) -> None:
        with suppress(Exception):
            self.writer.write(encode_frame(OP_CLOSE, struct.pack("!H", 1000), mask=False))
            await self.writer.drain()

    async def recv_text(self) -> str | None:
        """返回一条完整文本消息；None 表示对端关闭。

        控制帧（ping/pong/close）在这里被吸收，分片帧在这里被拼接——
        这正是 `websockets` 库替 nanobot 做的事情。
        """
        while True:
            try:
                fin, opcode, payload = await read_frame(self.reader)
            except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
                return None

            if opcode == OP_CLOSE:
                await self._answer_close()
                return None
            if opcode == OP_PING:
                await self._write_frame(OP_PONG, payload)
                continue
            if opcode == OP_PONG:
                continue

            if opcode in (OP_TEXT, OP_BINARY):
                if fin:
                    return payload.decode("utf-8", "replace")
                self._frag_parts = [payload]
                continue

            if opcode == OP_CONT:
                self._frag_parts.append(payload)
                if fin:
                    merged = b"".join(self._frag_parts)
                    self._frag_parts = []
                    return merged.decode("utf-8", "replace")

    def __aiter__(self) -> "WSConnection":
        return self

    async def __anext__(self) -> str:
        message = await self.recv_text()
        if message is None:
            raise StopAsyncIteration
        return message


ConnectionHandler = Callable[[WSConnection], Awaitable[None]]
# 普通 HTTP 响应器：接收 (request_line, headers)，返回 (status, content_type, body) 或 None
PlainHttpResponder = Callable[[str, dict[str, str]], tuple[int, str, bytes] | None]


def _write_http(
    writer: asyncio.StreamWriter,
    status: int,
    content_type: str,
    raw: bytes,
) -> None:
    reason = "OK" if status == 200 else "Not Found"
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\n".encode("ascii")
        + f"Content-Type: {content_type}\r\n".encode("ascii")
        + b"Content-Length: " + str(len(raw)).encode() + b"\r\n"
        + b"Cache-Control: no-store\r\n"
        + b"Connection: close\r\n\r\n"
        + raw
    )


async def _handle_plain_http(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    request_line: str,
    headers: dict[str, str],
    responder: PlainHttpResponder | None,
) -> None:
    """同端口上的普通 HTTP（对标 nanobot 的 process_request 分支）。

    nanobot 就是靠这个机制在 8765 端口上同时提供 /webui/bootstrap 这类 HTTP 接口，
    以及同一个进程里的静态 WebUI 资源。
    """
    if responder is not None:
        result = responder(request_line, headers)
        if result is not None:
            status, content_type, raw = result
            _write_http(writer, status, content_type, raw)
            with suppress(Exception):
                await writer.drain()
            writer.close()
            return

    path = request_line.split(" ")[1] if len(request_line.split(" ")) > 1 else "/"
    body = json.dumps(
        {"ok": True, "path": path, "hint": "这里只提供 WebSocket 升级"},
        ensure_ascii=False,
    ).encode("utf-8")
    _write_http(writer, 200, "application/json; charset=utf-8", body)
    with suppress(Exception):
        await writer.drain()
    writer.close()


async def _serve_one(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    handler: ConnectionHandler,
    responder: PlainHttpResponder | None,
) -> None:
    try:
        raw_request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10.0)
    except Exception:
        writer.close()
        return

    lines = raw_request.decode("latin-1").split("\r\n")
    request_line = lines[0]
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            key, _, value = line.partition(":")
            headers[key.strip().lower()] = value.strip()

    if headers.get("upgrade", "").lower() != "websocket":
        await _handle_plain_http(reader, writer, request_line, headers, responder)
        return

    parts = request_line.split(" ")
    target = parts[1] if len(parts) > 1 else "/"
    split = urlsplit(target)
    query = parse_qs(split.query)
    remote = writer.get_extra_info("peername")

    key = headers.get("sec-websocket-key", "")
    writer.write(
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\n"
        b"Connection: Upgrade\r\n"
        b"Sec-WebSocket-Accept: " + accept_key(key).encode("ascii") + b"\r\n\r\n"
    )
    await writer.drain()

    connection = WSConnection(
        reader,
        writer,
        path=split.path or "/",
        query=query,
        remote=str(remote) if remote else "",
    )
    try:
        await handler(connection)
    except Exception:  # 单连接出错不能影响其它连接
        pass
    finally:
        with suppress(Exception):
            writer.close()


async def start_server(
    handler: ConnectionHandler,
    host: str,
    port: int,
    *,
    http_responder: PlainHttpResponder | None = None,
) -> asyncio.Server:
    """启动监听；每个新连接都会调用 *handler*。

    对标 nanobot：`server = await serve(handler, host, port, process_request=...)`
    """

    async def on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _serve_one(reader, writer, handler, http_responder)

    return await asyncio.start_server(on_client, host, port)


# ---------------------------------------------------------------------------
# 极简客户端：用于测试和命令行验证（真实场景里由浏览器/TUI 充当这个角色）
# ---------------------------------------------------------------------------


class WSClient:
    """最小可用的 WebSocket 客户端，负责生成掩码帧。"""

    def __init__(self, url: str) -> None:
        self.url = url
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._frag_parts: list[bytes] = []

    async def connect(self) -> None:
        split = urlsplit(self.url)
        host = split.hostname or "127.0.0.1"
        port = split.port or 80
        target = split.path or "/"
        if split.query:
            target += "?" + split.query

        self.reader, self.writer = await asyncio.open_connection(host, port)
        key = base64.b64encode(b"nanobot-ws-lab-key").decode("ascii")
        self.writer.write(
            f"GET {target} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n".encode("ascii")
        )
        await self.writer.drain()
        response = await self.reader.readuntil(b"\r\n\r\n")
        if b"101" not in response.split(b"\r\n")[0]:
            raise RuntimeError(f"握手失败: {response[:120]!r}")

    async def send_text(self, text: str) -> None:
        assert self.writer is not None
        self.writer.write(encode_frame(OP_TEXT, text.encode("utf-8"), mask=True))
        await self.writer.drain()

    async def recv_json(self) -> dict | None:
        assert self.reader is not None
        while True:
            try:
                fin, opcode, payload = await read_frame(self.reader)
            except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
                return None
            if opcode == OP_CLOSE:
                return None
            if opcode == OP_PING:
                assert self.writer is not None
                self.writer.write(encode_frame(OP_PONG, payload, mask=True))
                await self.writer.drain()
                continue
            if opcode in (OP_TEXT, OP_BINARY):
                if not fin:
                    self._frag_parts = [payload]
                    continue
                return json.loads(payload.decode("utf-8"))
            if opcode == OP_CONT:
                self._frag_parts.append(payload)
                if fin:
                    merged = b"".join(self._frag_parts)
                    self._frag_parts = []
                    return json.loads(merged.decode("utf-8"))

    async def close(self) -> None:
        if self.writer is not None:
            with suppress(Exception):
                self.writer.write(encode_frame(OP_CLOSE, struct.pack("!H", 1000), mask=True))
                await self.writer.drain()
            self.writer.close()
