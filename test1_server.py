"""配对测试用的迷你 TCP 回显服务端（标准库，零依赖）。

配合 test1.py 使用：先跑这个，再跑 test1.py，就能看到完整的收发往返。

    python test1_server.py            # 监听 127.0.0.1:8899
    python test1_server.py 9000       # 换端口

行为刻意做得"可预期"，方便观察字节流的边界问题：
  - 收到数据 → 立即回一段带前缀的文本（会分两次 write，故意制造"半包"）
  - 客户端发 "bye" → 服务端主动关闭连接
  - Ctrl+C 停止
"""

from __future__ import annotations

import asyncio
import sys

DEFAULT_PORT = 8899


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    print(f"[server] 客户端接入: {peer}")

    try:
        while True:
            data = await reader.read(64)
            if not data:  # 对端关闭
                print(f"[server] 客户端断开: {peer}")
                return

            text = data.decode("utf-8", errors="replace")
            print(f"[server] 收到 {len(data)} 字节: {text!r}")

            if text.strip().lower() == "bye":
                writer.write("再见\n".encode("utf-8"))
                await writer.drain()
                return

            # 故意拆成两次 write：让客户端体会"read() 一次不一定读全"
            writer.write(f"echo({len(data)}B): ".encode("utf-8"))
            await writer.drain()
            await asyncio.sleep(0.05)
            writer.write(f"{text}\n".encode("utf-8"))
            await writer.drain()
    except (ConnectionResetError, OSError) as exc:
        print(f"[server] 连接异常 {peer}: {exc}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, OSError):
            pass
        print(f"[server] 已关闭: {peer}")


async def main(port: int) -> None:
    server = await asyncio.start_server(handle_client, "127.0.0.1", port)
    sockets = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f"[server] 监听 {sockets}  (Ctrl+C 退出)")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT))
    except KeyboardInterrupt:
        print("\n[server] 已停止")
