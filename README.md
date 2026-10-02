# nanobot WebSocket 通道实验室（nanobot-ws-lab）

一个**零依赖、可直接运行**的迷你实现，用来吃透 nanobot WebUI 的 WebSocket 通道。
代码全部带中文注释，并在每个文件头部标注了对应的 nanobot 真实源码位置。

> 只读研究用：本目录是新增的独立实验室，**没有修改 nanobot 仓库里的任何现有代码**。

## 30 秒跑起来

```bash
cd nanobot-ws-lab
python run.py            # 默认监听 127.0.0.1:9988
```

然后浏览器打开 <http://127.0.0.1:9988/>，在输入框敲一句话回车。
页面右侧是**协议示波器**，会把每一条原始事件原样打出来——这是理解协议最快的途径。

不想开浏览器：

```bash
python client_demo.py 9988    # 另开一个终端
```

跑测试：

```bash
python -m unittest discover -s tests -v   # 15 个用例，只用标准库，零依赖
pytest tests/ -v                          # 29 个用例（含下面的 pytest 版订阅测试）
pytest tests/test_channel_subs.py -v      # 只跑订阅簿记那 14 个用例
```

> `tests/test_channel_subs.py` 是 pytest 版，专测 `src/channel.py` 里的
> `self._subs.setdefault(chat_id, set()).add(connection)` 这行订阅簿记：
> 建桶 / 去重 / 扇出 / 空桶清理 / 快照拷贝。
> 它不需要 `pytest-asyncio`（异步用例内部用 `asyncio.run` 驱动），装个 pytest 就能跑。

## 顺手练手：裸 TCP 回显（和 WebSocket 通道无关，但对照着看更清楚）

`test1_server.py` / `test1_client.py` 是一对最小 TCP 收发示例，用来体会
"裸 TCP 没有消息边界"这件事——正是 WebSocket 要加帧头的原因。

```bash
python test1_server.py          # 终端 A：监听 127.0.0.1:8899
python test1_client.py          # 终端 B：连接并收发
python test1_client.py 9000 "你好"   # 换端口 / 换内容
```

`test1.py` 是更早的手写版，**故意保留原始写法**：没有异常处理，
服务端没启动时会直接抛 `ConnectionRefusedError`。两者对比着跑一遍，
就能明白"为什么生产代码必须把连接失败翻译成人话"。

## 目录结构

| 文件 | 作用 | 对应 nanobot 源码 |
|---|---|---|
| `src/ws_proto.py` | 标准库手写 WebSocket 握手 + 帧编解码 + 极简客户端 | `websockets` 库所做的事 / `runtime.py` 的 `process_request` |
| `src/bus.py` | 入站/出站双队列，解耦通道与 Agent | `nanobot/bus/queue.py` |
| `src/channel.py` | **核心**：订阅路由、`ready` 握手、信封分发、出站背压 | `nanobot/channels/websocket/runtime.py` + `channels/base.py` |
| `src/projector.py` | 内部事件 → 线上 JSON 字段（稳定契约层） | `nanobot/webui/outbound_projection.py` + `outbound_wire.py` |
| `src/agent.py` | 迷你 Agent 循环，用真实事件序列产出流式回复 | `nanobot/agent/loop.py` + `runner.py` |
| `src/server.py` | 组装四层 + 同端口静态 HTTP | `nanobot/gateway/runtime.py` |
| `web/index.html` | 浏览器客户端（含原始事件日志面板） | `webui/src/` + `tui/src/app.ts` 的事件 switch |
| `tests/test_lab.py` | 握手/信封/扇出/背压/清理/端到端 六类测试（unittest） | — |
| `tests/test_channel_subs.py` | 订阅簿记 `_subs` 的 pytest 专项测试（14 例） | `runtime.py:451-509` |

## 学完能回答的问题

1. 为什么服务端要先发 `ready` 再登记连接？顺序反了会怎样？
2. 一个 `chat_id` 同时开三个浏览器标签，消息为什么都能收到？
3. 慢客户端（比如断点后不继续的浏览器）为什么不会拖垮整个 gateway？
4. `delta` / `stream_end` / `message` 三个事件为什么要一起用，只发 `delta` 行不行？
5. 连接断开后，`_subs` 表里如果残留会怎样？

完整讲解见 **[tutorial.html](./tutorial.html)**（浏览器打开即可，图文对照）。

## 建议的实验顺序

1. 先跑 `python run.py` + 浏览器，在右侧日志里观察一次完整对话的事件序列。
2. 读 `src/ws_proto.py`，理解 `Sec-WebSocket-Accept` 和帧头两个字节。
3. 读 `src/channel.py` 的 `_safe_send_to`，把 `max_frames` 改成 `2`，然后在浏览器里连发多轮消息，观察 `[backpressure]` 日志。
4. 读 `tests/test_lab.py`，把每个断言和 `src/channel.py` 里的函数对应起来；
   想更细地抠订阅表，再读 `tests/test_channel_subs.py`。
5. 最后回到 `nanobot/channels/websocket/runtime.py` 对照阅读，会发现只是多了鉴权、临时会话、transcript 持久化等工程化细节。
