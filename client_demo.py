"""命令行客户端：不用浏览器也能验证整条链路。

用法（先在一个终端跑 python run.py，再在另一个终端跑本脚本）：
    python client_demo.py 9988

它会打印服务端推来的每一条原始事件，是排查协议问题最快的办法。
"""

from __future__ import annotations

import asyncio
import json
import sys

from src.ws_proto import WSClient


async def main(port: int = 9988) -> None:
    client = WSClient(f"ws://127.0.0.1:{port}/?client_id=cli-demo")
    await client.connect()

    # 1) 握手后第一件事：收 ready，拿 chat_id
    ready = await asyncio.wait_for(client.recv_json(), timeout=5)
    print("服务端 ready →", json.dumps(ready, ensure_ascii=False))
    chat_id = ready["chat_id"]

    # 2) 发一条消息（信封结构与真实前端一致）
    await client.send_text(json.dumps(
        {"type": "message", "chat_id": chat_id, "content": "你好，迷你 nanobot", "turn_id": "t-1"},
        ensure_ascii=False,
    ))
    print("已发送 ↑ type=message")

    # 3) 收下行事件，直到拿到最终 message
    while True:
        event = await asyncio.wait_for(client.recv_json(), timeout=10)
        if event is None:
            print("连接已关闭")
            break
        print("  ←", json.dumps(event, ensure_ascii=False))
        if event.get("event") == "message" and not event.get("kind"):
            break

    await client.close()
    print("完成")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 9988))
