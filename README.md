# rpcproxy

基于 **WebSocket** 传输的 **JSON-RPC 2.0** 异步客户端，用于向远端发送可批量的命令调用，并在本地以队列与超时策略管理在途请求与计算任务。

## 运行环境假设

客户端需**同时**在 **Windows** 与 **Linux** 上可运行、可部署。实现与依赖选择应避免绑定单一操作系统能力（例如避免未经抽象的 POSIX 专有 API、硬编码路径分隔符或仅在某平台可用的子进程/信号语义）；**`asyncio`、WebSocket 与纯 Python 依赖**在两类系统上通常行为一致，但仍建议在 Windows 与 Linux 上各跑通连接、batch、超时与队列相关用例。CI 中若条件允许，可对两个平台各建一条流水线做回归。

## 目标

- 通过 **JSON-RPC 2.0** 在单条 WebSocket 连接上调用远端方法（含 **batch**：单帧发送多个带 `id` 的请求）。
- **异步 I/O**：单连接、单读循环分发消息，支持多调用并发等待。
- **标准 WebSocket Ping**：依赖传输层（如 [websockets](https://pypi.org/project/websockets/)）对 [RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455) 控制帧的响应，业务层专注 JSON 文本帧。
- **计算与排队**：重 CPU 或需限流的工作通过队列与 worker（线程/进程池视场景而定），避免阻塞事件循环，保证收包与心跳相关逻辑及时执行。
- **超时与丢弃**：为每个请求（含 batch 中的子请求）维护 `id` 与截止时间；超时后从待决表中移除，**迟到的响应必须丢弃**，避免错绑到其他调用。

## 非目标

本项目**不**以「代理中间件」为定位，默认不包含鉴权、访问日志、限流、监听面板等横切能力；如需可自行在外层集成。

## 协议与实现取向

| 层次 | 选择 |
|------|------|
| 传输 | WebSocket（文本帧承载 JSON） |
| 应用消息 | JSON-RPC 2.0；以及与 [fastapi-websocket-rpc](https://github.com/permitio/fastapi_websocket_rpc) 一致的 **RpcMessage**（`request` / `response` + `call_id` + `result_type`） |
| 推荐依赖 | `asyncio` + `websockets`（或 `aiohttp` 的 WebSocket 客户端）；JSON 序列化以标准库为主 |

与 **fastapi-websocket-rpc** 对齐的线格式由包内模块 **`rpcproxy.fastapi_ws_rpc`** 统一提供（`TypedDict`、是否待应答、构造 `RpcResponse`、内置 `_ping_` / `_get_channel_id_` 的默认结果等），业务代码与 CLI demo 应通过该模块拼包/解析，避免在多处手写字段名。

读循环建议作为**唯一**的 WebSocket 文本读入口：根据 payload 区分 **JSON-RPC**（按 `id` 唤醒 `pending`）、**RpcMessage 入站调用**（用 `fastapi_ws_rpc` 构造回包）、以及对端的响应帧，并与超时、丢弃策略一致。

### 客户端基类（RpcMessage）

继承 **`RpcProxyClientBase`**（[`src/rpcproxy/client/base.py`](src/rpcproxy/client/base.py) 或 `from rpcproxy import RpcProxyClientBase`），实现抽象方法 **`receive_envelope`**。调用 **`await connect(ws_url)`** 建立连接后：

- 对端发起的 **`_ping_`**、**`_get_channel_id_`** 由基类按 fastapi-websocket-rpc 约定自动应答；
- **`await set_state(key, value)`** 向对端发起 **`set_state`** RPC（参数为 `arguments: {key, value}`），返回对端 `response.result`；超时由构造参数 **`default_call_timeout`** 控制（默认 `30.0` 秒，`None` 表示不超时）。
- **`await wait_until_disconnected()`** 在 **`connect`** 之后阻塞，直到读循环结束（对端关连接或 **`close()`**）；CLI demo 用其保持进程存活。
- 可重写 **`on_unmatched_message`**，处理非入站调用、亦非本端 pending 应答的 JSON 对象（demo 将其打印到 stderr）。

线格式由 **`rpcproxy.fastapi_ws_rpc`**（含 `call_request_message` 等）统一构造；**不**安装 `fastapi-websocket-rpc` 运行时依赖。

## 开发环境

- Python **≥ 3.11**（Windows / Linux 均可，与 [uv](https://docs.astral.sh/uv/) 支持的平台一致）
- 包与虚拟环境由 **[uv](https://docs.astral.sh/uv/)** 管理

### 克隆与安装

```bash
git clone <repository-url>
cd rpcproxy
uv sync
```

### 常用命令

| 操作 | 命令 |
|------|------|
| 同步依赖（含可编辑安装本包） | `uv sync` |
| 新增依赖 | `uv add <package>` |
| 在虚拟环境中执行命令 | `uv run python ...` |
| 运行测试（配置好后） | `uv run pytest` |

可选：使用 `uv python pin <version>` 固定解释器版本并将 `.python-version` 纳入版本控制。

### Demo（最小命令行）

安装后可用 **`rpcproxy demo <WS_URL>`** 连接 `ws://` 或 `wss://` 服务端：实现为 **`DemoRpcProxyClient`**（继承 [`RpcProxyClientBase`](src/rpcproxy/client/base.py)），仅处理 **fastapi-websocket-rpc 线格式的 RpcMessage**。入站 **`receive_envelope`** 将解析后的参数打印到标准输出并回复 `{"ok": true}`；**`_ping_`** / **`_get_channel_id_`** 由基类自动应答；无法识别的 JSON 对象（例如仅有 `response` 且未匹配本端 pending）打印到标准错误。传输层 **WebSocket Ping** 由 `websockets` 自动应答。使用 **Ctrl+C** 或等待对端关闭连接后退出；**`finally` 中会 `close()`** 释放连接。

```bash
uv run rpcproxy demo ws://127.0.0.1:8080/rpc
```

## 项目结构

```text
rpcproxy/
├── pyproject.toml      # 项目元数据与构建配置（Hatchling / uv）
├── uv.lock             # 锁定的依赖版本（建议提交）
├── README.md
├── src/
│   └── rpcproxy/
│       ├── fastapi_ws_rpc/   # 与 fastapi-websocket-rpc 一致的 RpcMessage 线格式
│       ├── client/           # RpcProxyClientBase
│       ├── demo_loop.py
│       └── cli.py
└── tests/
```

## 规范参考

- [JSON-RPC 2.0 Specification](https://www.jsonrpc.org/specification)
- WebSocket 协议：RFC 6455
- [fastapi-websocket-rpc](https://github.com/permitio/fastapi_websocket_rpc)（RpcMessage / RpcRequest / RpcResponse 语义）
