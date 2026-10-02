# nanobot-ws-lab 项目长期笔记

## 项目性质

教学实验室：用零依赖的迷你实现还原 nanobot WebUI 的 WebSocket 通道，
每个文件头部标注对应的 nanobot 真实源码位置（`runtime.py` 等行号）。
原则：**不改 nanobot 主仓库**，本目录独立演进。

## 代码约定

- 注释、文档、测试说明一律用中文；文件头写"对标 nanobot 哪个文件/哪一行"。
- 运行时代码保持零第三方依赖（只用标准库），方便 `python run.py` 直接跑。
- 测试可用 pytest，但不引入 `pytest-asyncio` 之类的必需插件：
  异步用例内部用 `asyncio.run(...)` 驱动，保持"装了 pytest 就能跑"。

## 测试与运行

- 全量：`pytest tests/ -q` ｜ 原套件：`python -m unittest discover -s tests -v`
- 解释器：`C:\Users\magic\.workbuddy\binaries\python\envs\default\Scripts\python.exe`（已装 pytest）
- 关键测试文件：`tests/test_lab.py`（协议/背压/端到端）、`tests/test_channel_subs.py`（`_subs` 订阅簿记）
