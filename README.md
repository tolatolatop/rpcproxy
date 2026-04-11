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
| 应用消息 | JSON-RPC 2.0（单条请求、响应、错误对象及 **batch 数组**） |
| 推荐依赖 | `asyncio` + `websockets`（或 `aiohttp` 的 WebSocket 客户端）；JSON 序列化可用标准库或轻量库（如 `jsonrpcclient`）辅助构造/解析消息体 |

读循环建议作为**唯一**的 WebSocket 文本读入口：根据 payload 区分 **对本客户端的 response**（按 `id` 唤醒 `pending`）、**对端发起的 JSON-RPC request**（若有，需按 method 处理并回写），并与上述超时、丢弃策略一致。

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

## 项目结构

```text
rpcproxy/
├── pyproject.toml      # 项目元数据与构建配置（Hatchling / uv）
├── uv.lock             # 锁定的依赖版本（建议提交）
├── README.md
├── src/
│   └── rpcproxy/       # 包源码
└── tests/
```

## 规范参考

- [JSON-RPC 2.0 Specification](https://www.jsonrpc.org/specification)
- WebSocket 协议：RFC 6455
