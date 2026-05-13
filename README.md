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
- **`await post_message(receiver="", body=None, request_id="")`** 向对端发起 **`post_message`** RPC；`body` 为 `None` 时按 `{}` 发送；返回值对对端 `result` 做 **`str()`** 以匹配 `-> str`。若对端随后以 **`receive_envelope`** 回推同一条 **`request_id`**，基类会在调用子类 **`receive_envelope`** 之前登记 **「已送达回执」**；本端可用 **`await wait_relay_predicate(request_id, timeout)`** 等待该回执（`timeout` 为 **`None`** 表示无限等待，与 **`set_state`** 一致）。
- **`await post_message_auto(receiver="", body=None, request_id="", auto_chunk_threshold=256*1024, chunk_size=256*1024, compress=False)`** 是推荐默认接口：先估算 `body` 作为 UTF-8 JSON 文本的字节数；**小于 256KiB** 时直接走普通 `post_message`，**达到/超过 256KiB** 时自动切到 `post_message_chunked`。返回值为 `AutoPostMessageResult`：`chunked=False` 时看 `response`，`chunked=True` 时看 `chunk_report`。之所以选 **256KiB** 作为默认阈值，是因为它足够保守，能显著降低被 WebSocket / 反向代理 / 上游消息大小限制拦截的概率，同时对普通 JSON 命令又不会过早分片。
- **`await post_message_chunked(receiver="", body=None, request_id="", chunk_size=256*1024, compress=False)`** 用于发送大 JSON object：先把原始 `body` 序列化为 UTF-8 JSON（可选 gzip 压缩），再拆成多个 `post_message`。每片 `body` 均为下列结构，其中 **`data_b64`** 为当前分片字节的 Base64，**`request_id`** 为业务级原始请求号，传输层实际 `request_id` 则自动变成 `原request_id#chunk:i/N`：

  ```json
  {
    "__rpcproxy_chunk__": {
      "version": 1,
      "transfer_id": "7f6c...",
      "request_id": "job-42",
      "index": 0,
      "count": 4,
      "total_bytes": 523001,
      "sha256": "<assembled-bytes sha256>",
      "content_type": "application/json",
      "content_encoding": "identity"
    },
    "data_b64": "eyJiaWciOiAiLi4uIn0="
  }
  ```

  对端若也使用当前版本 `rpcproxy`，会在 **`receive_envelope`** 进入业务处理前自动按 `transfer_id` 聚合、校验 `sha256`、解压（若 `gzip`）并恢复出原始 `body`；中间分片只 ACK、不下发给业务 handler。组装完成后，业务侧拿到的是**完整原始 body**，同时额外可见 `chunk_transfer_id`、`chunk_count`、`chunk_total_bytes`、`chunk_sha256`、`chunk_content_encoding` 这些扩展字段。
- **`wait_relay_predicate`** 成功时返回 **`{"ok": True, "arguments": {...}}`**：其中 **`arguments`** 为入站 RPC **`arguments`** 的浅拷贝（业务字段如 **`body`**、**`message_type`** 等在其中读取）；同一 **`request_id`** 仅允许一个并发等待，第二个等待者会触发 **`RuntimeError`**。连接关闭或读循环结束时，未完成的等待会被 **`cancel`**，stash 会清空。
- 尚未被取走的回执缓存在 **有界 LRU stash** 中，由构造参数 **`relay_stash_max_size`** 控制条目上限（默认 **`256`**）。超过上限时**最久未更新**的条目会被丢弃（**`DEBUG`** 日志含被驱逐的 **`request_id`**）；**`relay_stash_max_size=0`** 表示禁用 stash，仅当已调用 **`wait_relay_predicate`** 阻塞等待时才会通过 Future 收到回执（「信封先到、后调用等待」不再成立）。同一 **`request_id`** 再次入站会覆盖并视为最近使用。

- **`await wait_until_disconnected()`** 在 **`connect`** 之后阻塞，直到读循环结束（对端关连接或 **`close()`**）；CLI demo 用其保持进程存活。
- 可重写 **`on_unmatched_message`**，处理非入站调用、亦非本端 pending 应答的 JSON 对象（demo 以 **WARNING** 级别记日志）。

线格式由 **`rpcproxy.fastapi_ws_rpc`**（含 `call_request_message` 等）统一构造；**不**安装 `fastapi-websocket-rpc` 运行时依赖。

### Handler 客户端（`HandlerPostMessageClient`）

[`HandlerPostMessageClient`](src/rpcproxy/client/handler_client.py) 继承 **`RpcProxyClientBase`**，构造时注入异步 **`handler`**，用于「服务端 **`receive_envelope` 推任务 → 客户端异步处理 → **`post_message`** 把结果发回」**。入站 **`arguments`** 的标准字段由 **[`ReceiveEnvelopeArguments`](src/rpcproxy/client/envelope_types.py)**（**`TypedDict`**）描述，线路上仍可带额外键。

- **读循环不被阻塞**：入站 **`receive_envelope`** 在登记 relay 后**立即**返回 **`{"ok": True}`**（或校验失败时 **`{"ok": False}`**），真正的 **`handler`** 与 **`await post_message(...)`** 在 **`asyncio.create_task`** 的后台协程中执行；因此不得在 **`receive_envelope` 内直接 `await post_message`**（否则会卡住 **`_dispatch_inbound`**，其它入站 RPC 与心跳延迟）。
- **`HandlerResult`**：**`body`** 发往 **`post_message`**；**`request_id`** 可选，为空时使用入站 **`request_id`**。**`post_message` 的 `receiver` 固定为入站信封的 `sender`**（推 RPC 的一方），不再支持按 **`receiver` 字段或固定地址**回传。
- 入站 **`request_id`** **必须**非空（仅空白视为空）：否则**不启动** pipeline 并返回 **`{"ok": False}`**。
- **`skip_post=True`**：仅 ACK，不调用 **`post_message`**。
- **错误**：**`handler`** 抛错时调用 **`await self.on_handler_exception(exc, arguments)`**；基类默认仅 **`logger.error(..., exc_info=exc)`** 记日志。子类可**重写**该方法（通常先 **`await super().on_handler_exception(...)`**），再按需 **`await self.post_message(...)`**（**`receiver` 仍应为入站 `sender`**，勿把敏感栈信息写入 **`body`**）。
- **`max_inflight`**：默认 **`8`**，始终用 **`asyncio.Semaphore`** 限制并发 pipeline 数（须 **`>= 1`**），减轻对端与事件循环压力。
- 关闭连接时**已提交的**后台任务可能仍短暂运行或在 **`post_message`** 上失败，调用方应 **`await close()`** 并容忍收尾日志；与基类相同，仍可使用 **`wait_relay_predicate`** 与有界 stash。

### Playwright Handler（可选）

包内 **`rpcproxy.handlers`** 提供基于 [Playwright](https://playwright.dev/python/) 的 **`EnvelopeHandler`**：默认通过 Chromium 的 **`msedge`** 通道启动 **Edge**，默认 **无头**（`headless=True`）。**不**随核心依赖安装；需额外安装可选依赖并安装浏览器二进制：

```bash
uv sync --extra playwright
playwright install msedge
```

（或 `pip install 'rpcproxy[playwright]'` 后同样执行 `playwright install msedge`。）

对端通过 **`receive_envelope`** 推送的 **`body`** 须为对象，且包含 **`command`**（字符串）。成功或失败时，本端经 **`post_message`** 回传的 **`body`** 均含结构化字段（至少 **`ok`**；成功时通常带 **`command`** 回显）。

| `command` | 说明 | 主要字段 | 成功时 `post_message` body 要点 |
|-----------|------|----------|----------------------------------|
| `open_page` | 新开页并导航 | **`url`**（必填） | **`ok: true`**, **`page_id`**, **`url`** |
| `execute_js` | `page.evaluate` | **`page_id`**, **`script`**（必填） | **`ok: true`**, **`result`**（尽量 JSON 可序列化，否则转字符串） |
| `request` | 使用 **`BrowserContext.request`** 发 HTTP（非页面 DOM） | **`url`**；可选 **`method`**（默认 **`GET`**）、**`headers`**、**`json`** 或 **`data`** | **`ok: true`**, **`status`**, **`headers`**, **`body`**, **`body_encoding`**（`utf-8` 或 `base64`），**`truncated`**（响应体超过 256KiB 时截断） |
| `close_page` | 关闭页并从映射中移除 | **`page_id`**（必填） | **`ok: true`**, **`page_id`** |

未知命令、缺字段或 Playwright 报错时：**`ok: false`**, **`error`**, 可选 **`error_type`**。会话内对页面映射使用 **`asyncio.Lock`**，与 **`max_inflight`** 一并降低竞态。

**接入方式**：使用 **`make_playwright_handler(session)`** 配合自建 **`PlaywrightSession`**，或直接使用 **`PlaywrightRpcProxyClient`**（在 **`await close()`** 时会先关闭 WebSocket 再 **`await PlaywrightSession.close()`** 释放浏览器）。进程退出前务必 **`await client.close()`**，否则浏览器进程可能残留。

```python
from rpcproxy.handlers import PlaywrightRpcProxyClient

async def main():
    client = PlaywrightRpcProxyClient()
    try:
        await client.connect("ws://127.0.0.1:8080/rpc")
        await client.wait_until_disconnected()
    finally:
        await client.close()
```

### 日志

- 运行任意 **`rpcproxy`** CLI 子命令时会在入口调用 **`setup_logging()`**（见 [`src/rpcproxy/logging_config.py`](src/rpcproxy/logging_config.py)）：为 **`rpcproxy`** 日志树挂载 **轮转文件**（`rpcproxy.log`）与 **stderr** 流，两者共用同一格式与级别。
- 启动后立刻记一条 **INFO**：**`日志目录: <绝对路径>`**（便于确认落盘位置）。
- **默认目录**：[`platformdirs`](https://pypi.org/project/platformdirs/) 的 **`user_log_dir("rpcproxy")`**（随操作系统变化，一般在用户本机数据目录下）。可用 **`RPCPROXY_LOG_DIR`** 覆盖为自定义目录。
- **轮转**：单文件最大 **10MB**，**`backupCount=4`**（除当前文件外保留 4 个备份，即 `rpcproxy.log.1` … `.4`）。
- **`RPCPROXY_LOG_LEVEL`**：默认 **`INFO`**，可设为 **`DEBUG`**、**`WARNING`** 等标准级别名。

## 开发环境

- Python **≥ 3.11**（Windows / Linux 均可，与 [uv](https://docs.astral.sh/uv/) 支持的平台一致）
- 包与虚拟环境由 **[uv](https://docs.astral.sh/uv/)** 管理

### 克隆与安装

```bash
git clone <repository-url>
cd rpcproxy
uv sync
```

若需运行单元测试，请一并安装 **dev** 依赖组（**`pytest`**、**`pytest-asyncio`**，见 [`pyproject.toml`](pyproject.toml) 中 **`[dependency-groups]`**）：

```bash
uv sync --group dev
```

### 常用命令

| 操作 | 命令 |
|------|------|
| 同步依赖（含可编辑安装本包） | `uv sync` |
| 同步并包含开发与测试依赖 | `uv sync --group dev` |
| 新增依赖 | `uv add <package>` |
| 新增仅用于开发的依赖 | `uv add --group dev <package>` |
| 在虚拟环境中执行命令 | `uv run python ...` |
| 运行测试 | `uv run --group dev pytest` |
| 一次性 ``post_message`` + ``wait_relay_predicate``（JSON 输出） | `uv run rpcproxy post WS_URL -r R --body '{}'` |

可选：使用 `uv python pin <version>` 固定解释器版本并将 `.python-version` 纳入版本控制。

### 测试

- 配置见 **`[tool.pytest.ini_options]`**（**`asyncio_mode = auto`**，测试目录 **`tests/`**）。
- **[`tests/test_client_base.py`](tests/test_client_base.py)** 使用 **`unittest.mock.patch`** 将 **`rpcproxy.client.base.websockets.connect`** 替换为 **`AsyncMock`**，由假 **`recv` / `send`**（队列与列表）驱动读循环，无需真实网络即可覆盖 **`connect`**、**`set_state`**、**`post_message`**、入站 **`receive_envelope`**、**`wait_relay_predicate`** 与 **`close`** 清理等行为。
- **[`tests/test_handler_client.py`](tests/test_handler_client.py)** 覆盖 **`HandlerPostMessageClient`**（ACK 早于 **`post_message`**、读循环不阻塞、**`skip_post`**、空 **`request_id`** 拒绝、**`on_handler_exception`** 子类扩展、**`max_inflight`**）。
- **[`tests/test_demo_loop.py`](tests/test_demo_loop.py)** 覆盖 **`demo_echo_envelope_handler`**（**`is_echo`**、body 浅拷贝）与 **`DemoRpcProxyClient`** 的 **`post_message`** echo 往返。
- **[`tests/test_cli_post.py`](tests/test_cli_post.py)** 覆盖 **`rpcproxy post`** 的 **`--body`** / **`--timeout`** 参数校验（不连真实 WebSocket）。
- **[`tests/test_playwright_handler.py`](tests/test_playwright_handler.py)** 覆盖 **`PlaywrightSession.handle_command`** 与 **`make_playwright_handler`**（**mock** 浏览器上下文，不启动真实 Edge）。
- 仅跑基类测试：`uv run --group dev pytest tests/test_client_base.py -v`。

### Demo（最小命令行）

安装后可用 **`rpcproxy demo <WS_URL>`** 连接 `ws://` 或 `wss://` 服务端：实现为 **`DemoRpcProxyClient`**（继承 [`HandlerPostMessageClient`](src/rpcproxy/client/handler_client.py)），仅处理 **fastapi-websocket-rpc 线格式的 RpcMessage**。**连接成功后**会立即用 **`set_state("token", <随机 token>)`** 向对端上报一次（`secrets.token_urlsafe(32)`），并以 **INFO** 记日志。入站 **`receive_envelope`** 先快速 ACK（**`{"ok": true}`**），后台 **`demo_echo_envelope_handler`** 将解析后的参数以 **INFO** 记日志，再通过 **`post_message`** 把入站 **`body`** 的浅拷贝发回，并在 **`body`** 中写入 **`"is_echo": true`**；**`post_message` 的 `receiver` 为入站 `sender`**，故对端推信时应带非空 **`sender`** 与 **`request_id`**（无 **`request_id`** 时 pipeline 不启动，RPC 返回 **`{"ok": false}`**）。**`_ping_`** / **`_get_channel_id_`** 由基类自动应答；无法识别的 JSON 对象以 **WARNING** 记日志。CLI 启动时会初始化日志并输出 **日志目录**（见上文「日志」）。传输层 **WebSocket Ping** 由 `websockets` 自动应答。使用 **Ctrl+C** 或等待对端关闭连接后退出；**`finally` 中会 `close()`** 释放连接。

```bash
uv run rpcproxy demo ws://127.0.0.1:8080/rpc
```

**`rpcproxy post`**：使用最小 **`RpcProxyClientBase`** 子类连接 **`WS_URL`**，先启动 **`wait_relay_predicate(request_id, timeout)`** 等待，再发送 **`post_message(receiver, body, request_id)`**（与 relay 使用同一 **`request_id`**）。**`--request-id`** 可省略，省略时自动生成 UUID hex。标准输出为一行 **JSON**：**`request_id`**、**`post_message`**（RPC 返回字符串）、**`relay`**（**`{"ok": true, "arguments": {...}}`**）。**`--body`** 须为 **JSON object** 字符串（默认 **`{}`**）；**`--timeout`** 对 **`post_message`** 与 **`wait_relay_predicate`** 各自生效（默认 **`30`** 秒，须为正数），任一环节超时则 **stderr** 报错并以非零码退出。示例（PowerShell 需注意引号转义）：

```bash
uv run rpcproxy post ws://127.0.0.1:8080/rpc -r peer --body "{\"msg\":\"hi\"}" --timeout 10
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
│       ├── client/           # RpcProxyClientBase、HandlerPostMessageClient、envelope_types
│       ├── handlers/         # 可选 Handler（Playwright 等）
│       ├── demo_loop.py      # DemoRpcProxyClient
│       └── cli.py
└── tests/
    ├── test_client_base.py      # RpcProxyClientBase（mock connect）
    ├── test_handler_client.py   # HandlerPostMessageClient
    ├── test_demo_loop.py        # demo echo / DemoRpcProxyClient
    ├── test_cli_post.py         # cli post 参数校验
    └── test_playwright_handler.py
```

## 规范参考

- [JSON-RPC 2.0 Specification](https://www.jsonrpc.org/specification)
- WebSocket 协议：RFC 6455
- [fastapi-websocket-rpc](https://github.com/permitio/fastapi_websocket_rpc)（RpcMessage / RpcRequest / RpcResponse 语义）
