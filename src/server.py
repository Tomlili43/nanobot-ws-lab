"""把四层组装起来跑：传输层 + 总线 + 通道 + Agent 循环。

对应 nanobot 的组装顺序：
    nanobot/gateway/runtime.py   创建 MessageBus / 各通道 / AgentLoop
    nanobot/channels/manager.py:598 start_all()        启动全部通道
    nanobot/cli/tui_launcher.py:409 _ensure_gateway()  本地起一个 gateway

运行方式（在 nanobot-ws-lab 目录下）：
    python run.py            # 默认 127.0.0.1:9988
    python run.py 9000       # 换端口
然后浏览器打开 http://127.0.0.1:9988/ ；或另开终端跑 python client_demo.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from .agent import MiniAgentLoop
from .bus import MessageBus
from .channel import LocalConfig, MiniWebSocketChannel

_WEB_DIR = Path(__file__).resolve().parent.parent / "web"


def _static_responder(request_line: str, headers: dict[str, str]) -> tuple[int, str, bytes] | None:
    """把 web/ 下的静态文件通过同端口 HTTP 提供出去。

    这就是 nanobot 在 8765 端口上同时跑 HTTP 接口 + WebSocket 的真实用法
    （runtime.py 的 process_request → gateway.endpoint.process_request）。
    """
    parts = request_line.split(" ")
    target = parts[1] if len(parts) > 1 else "/"
    path = target.split("?")[0]
    if path in ("/", "/index.html"):
        path = "/index.html"

    file_path = (_WEB_DIR / path.lstrip("/")).resolve()
    if _WEB_DIR.resolve() not in file_path.parents and file_path != _WEB_DIR.resolve():
        return (403, "text/plain; charset=utf-8", b"forbidden")
    if not file_path.is_file():
        return None

    suffix = file_path.suffix.lower()
    content_type = {
        ".html": "text/html; charset=utf-8",
        ".js": "text/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8",
    }.get(suffix, "application/octet-stream")
    return (200, content_type, file_path.read_bytes())


async def main(port: int = 9988) -> None:
    bus = MessageBus()
    config = LocalConfig(port=port)

    channel = MiniWebSocketChannel(config, bus)
    agent = MiniAgentLoop(bus)

    await channel.start(http_responder=_static_responder)
    print(f"[lab] 浏览器打开 http://{config.host}:{port}/ 即可对话")
    print("[lab] 按 Ctrl+C 退出")

    try:
        await agent.run()  # 主循环（agent 阻塞在这里）
    except asyncio.CancelledError:
        pass
    finally:
        await agent.stop()
        await channel.stop()
        print("[lab] 已停止")


def cli() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9988
    try:
        asyncio.run(main(port))
    except KeyboardInterrupt:
        print("\n[lab] 收到 Ctrl+C，退出")


if __name__ == "__main__":
    cli()
