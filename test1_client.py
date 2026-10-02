"""配对测试用的 TCP 客户端（test1.py 的加固版）。

相比 test1.py 多了三件事：
  1. 连接失败时给出人话提示，而不是裸 ConnectionRefusedError 堆栈；
  2. 支持命令行传端口；
  3. 用「先写后读 + 超时」的方式收数据，避免 read() 无限挂住。

    python test1_client.py                 # 连 127.0.0.1:8899
    python test1_client.py 9000 "你好"
"""

from __future__ import annotations

import asyncio
import sys

DEFAULT_PORT = 8899
DEFAULT_MESSAGE = "Hello Server"


async def main(port: int, message: str) -> None:
    # ---- 1) 连接：把 ConnectionRefusedError 翻译成人话 -------------------
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
    except ConnectionRefusedError:
        print(f"连不上 127.0.0.1:{port} —— 端口上没有任何程序在监听。")
        print("先另开一个终端跑：python test1_server.py")
        return
    except OSError as exc:
        print(f"连接出错: {exc}")
        return

    print(f"连接服务器成功 (127.0.0.1:{port})")

    try:
        payload = message.encode("utf-8")
        writer.write(payload)
        await writer.drain()
        print(f"已发送 {len(payload)} 字节: {message!r}")

        # ---- 2) 收响应：加超时，别让 read() 无限挂住 --------------------
        chunks: list[bytes] = []
        try:
            while True:
                chunk = await asyncio.wait_for(reader.read(1024), timeout=2.0)
                if not chunk:  # 服务端关闭连接 = EOF
                    break
                chunks.append(chunk)
                # 服务端回一行就够；真实场景要靠分隔符/长度头判断边界
                if chunk.endswith(b"\n"):
                    break
        except TimeoutError:
            print("等待响应超时（2 秒内没等到数据）")

        raw = b"".join(chunks)
        if raw:
            print(f"服务器返回 {len(raw)} 字节: {raw.decode('utf-8', errors='replace')!r}")
        else:
            print("服务器没有返回数据")
    finally:
        writer.close()
        await writer.wait_closed()
        print("连接已关闭")


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    text = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_MESSAGE
    try:
        asyncio.run(main(port, text))
    except KeyboardInterrupt:
        pass
