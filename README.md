# 单盘灾备恢复服务（Disk Recovery API）

把灾备站收到的**增量包**还原成虚拟机的 **raw 磁盘镜像**。以 4 KiB 块为最小单位，
预置一份 raw 基线镜像（版本 1），之后每次提交只保存本批变动块，链式还原出任意最新版本。

## 核心语义

- **块大小固定 4 KiB**。每个块支持两种操作：
  - `write`：用提交的内容覆盖该块（base64 编码，解码后须为 4 KiB；镜像末块按真实长度）；
  - `discard`：丢弃该块，读取时返回零（按末块真实长度补零）。
- **未列出的块沿用父版**，沿版本链向上回溯，直到基线。
- **每批写块合并落盘为单个数据文件**：一批几百个写块只新增 1 个 `data.bin`
  （按块号升序拼接，meta 记录每块偏移与长度）；纯丢弃批次不产生数据文件。
  旧版逐块文件（`blocks/`）原样保留，同一镜像可混读两种布局，旧卷可直接用新布局续写。
- 提交体携带**目标整盘镜像的 SHA-256**：服务端流式重算整盘摘要（按块取数，不读整包进内存），
  **仅当摘要吻合且父版本仍为当前版本**时才发布新版本，返回新版号。
- **原子提交**：数据文件先写 `tmp/` 暂存并 fsync，再原子挪入版本目录、写元数据、
  原子替换版本指针；任何一步失败都保留原版，失败批次的暂存与数据文件会被清理，
  同一版号可用新数据直接重试。
- **崩溃恢复**：重启时清理暂存、自基线起逐版校验版本链（元数据 + 数据文件齐备且
  大小精确匹配，缺失或截短即视为不完整），只发布最后一个**完整提交**的版本；
  不完整的尾部版本不会被提供，下一批从最后完整版接着提交。
- **流式下载快照隔离**：`GET /image` 在响应开始时固定版本，下载期间即使有新版本提交，
  已开始的下载仍返回原版内容；新请求才返回新版。响应头带回版号与摘要。

## 存储布局

```
data/
├── baseline.raw          # 预置 raw 基线（版本 1 的完整内容）
├── current.json          # 指向最后完整提交版本的指针（原子替换）
├── v000001/meta.json     # 每版元数据：父版、镜像大小、本批变动块清单、整盘 SHA-256
├── v000002/
│   ├── meta.json         #   新布局：changes 里每个写块带 offset/length
│   └── data.bin          #   新布局：本批全部写块合并为单个数据文件（纯丢弃批次无此文件）
├── blocks/
│   └── b0000000005_v000002   # 旧布局：每个变动块单独落盘（历史数据，原样保留可读）
└── tmp/                  # 提交暂存，崩溃/重启即清理
```

每批只落盘**一个合并数据文件 + 一个 meta.json + current 指针**，不产生整盘副本，
也不会像旧布局那样每批冒出几百个小文件。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 服务监听 `http://localhost:8000`
- 交互式文档（Swagger UI，可直接提交）：`http://localhost:8000/docs`
- ReDoc：`http://localhost:8000/redoc`
- 数据持久化在命名卷 `recovery-data`（容器内 `/data`）
- 基线大小通过环境变量 `BASELINE_SIZE` 调整（默认 `16MiB`，支持 `KiB/MiB/GiB` 或字节数）

## 本地开发

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 1) 预置 raw 基线（幂等，已存在且大小一致时直接复用）
python -m app.init_baseline --data-dir ./data --size 16MiB

# 2) 启动 API
RECOVERY_DATA_DIR=./data uvicorn app.main:app --reload --port 8000
```

## API

### `POST /recover/commit` — 提交增量并发布新版本

```json
{
  "parent_version": 1,
  "target_sha256": "<还原后整盘镜像期望的 SHA-256，64 位十六进制>",
  "changes": [
    {"index": 3, "op": "write", "data_base64": "<base64，解码后 4096 字节>"},
    {"index": 10, "op": "discard"}
  ]
}
```

成功 `201`：

```json
{"version": 2, "parent_version": 1, "sha256": "...", "image_size": 16777216, "block_count": 4096, "data_files": 1}
```

`data_files` 是本批写块落盘的数据文件数：合并布局恒为 1，纯丢弃批次为 0。

错误语义：

| 情况 | 状态码 |
| --- | --- |
| 摘要不吻合 / 块越界 / 重复块号 / base64 或长度非法 | `400` |
| 父版本不是当前版本（并发冲突，需基于最新版重新生成增量包） | `409` |
| 请求体不符合 schema | `422` |

### `GET /image` — 流式下载当前 raw 镜像

响应头：`Content-Length`（镜像字节数）、`X-Image-Version`（版号）、
`X-Image-Sha256`（该版摘要）、`Content-Disposition`。下载开始即固定版本快照。

### 其它

- `GET /version`：当前版号、镜像大小、块数、SHA-256，以及当前版这批写块占用的
  数据文件数 `data_files`（合并布局为 1，纯丢弃批次为 0，旧布局等于该批写块数）
- `GET /health`：健康检查
- `GET /docs`、`GET /openapi.json`：交互式文档与 OpenAPI 描述

## 测试

```bash
pip install -r requirements.txt httpx pytest
python -m pytest -q
```

覆盖 29 个用例，包括：链式继承、丢弃读零、覆盖、丢弃后重写、过期父版 409、
摘要不符 400 且原版保留、写块合并为单个数据文件（含几百块大批次与非整块末块）、
纯丢弃批次无数据文件、新旧布局混读与旧卷续写、失败回滚后同版本号重试、读/提交并发安全、
崩溃暂存清理与版本回退、缺失/截短数据文件的版本被跳过并回退、
真实 uvicorn 下的下载快照隔离、重启后只提供最后完整版本。

另有不依赖测试框架的真实 HTTP 冒烟脚本：

```bash
python -m app.init_baseline --data-dir /tmp/demo --size 256KiB
RECOVERY_DATA_DIR=/tmp/demo uvicorn app.main:app --port 8000 &
python scripts/smoke_http.py            # 默认打 http://127.0.0.1:8011，可用 SMOKE_BASE 覆盖
```

## 设计取舍与限制

- 单进程单 worker（compose 已固定 `--workers 1`）。提交串行化依赖进程内锁，
  并辅以 `fcntl` 文件锁防止同机多进程写同一数据目录；多副本水平扩展需替换为外部分布式锁。
- 版本链顺序读取，单块读取最坏为 O(版本数)；读路径按文件缓存句柄（LRU），
  并用 `pread` 定位读取，并发读同一数据文件互不干扰。
  面向灾备恢复场景（版本增量提交、整盘流式下载为主），不做后台压实（compaction）。
- 整盘摘要在每次提交时按块流式重算，内存占用恒定为块级，不缓存整盘镜像。
- 基线镜像内容为确定性生成的可复现图案，便于核对；如需植入自有镜像，
  可直接替换 `baseline.raw` 并重建 `v000001/meta.json` 与 `current.json`。
