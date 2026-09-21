# 分片文件上传服务

基于 FastAPI + SQLite 的可本地运行分片上传 API。支持乱序上传、断点续传、分片级 SHA-256 校验与原子合并；服务重启后未完成的会话可继续。

## 功能特性

- **上传会话**：创建会话时声明文件名、总大小、分片大小与（可选的）整文件 SHA-256
- **分片接收**：按序号上传，支持乱序与重复重试（幂等覆盖），每个分片独立 SHA-256 校验
- **状态查询**：返回会话状态与缺失分片列表，客户端可据此实现断点续传
- **原子合并**：全部分片到齐后先写临时文件、校验整文件哈希，再 `os.replace` 原子落盘
- **崩溃恢复**：SQLite (WAL) 持久化会话与分片记录；重启时自动核对磁盘分片，合并中断的会话可重试
- **零外部依赖**：仅 Python + SQLite，单容器即可运行

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`，数据持久化在命名卷 `uploader-data` 中。

### Docker

```bash
docker build -t chunked-uploader .
docker run -d -p 8000:8000 -v uploader-data:/data/storage chunked-uploader
```

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

启动时自动初始化存储目录（默认 `./storage`，可用环境变量 `STORAGE_DIR` 覆盖）。

交互式 API 文档：http://localhost:8000/docs

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/uploads` | 创建上传会话 |
| `PUT` | `/api/uploads/{id}/chunks/{index}` | 上传一个分片（乱序/重试安全） |
| `GET` | `/api/uploads/{id}` | 查询会话状态与缺失分片 |
| `GET` | `/api/uploads` | 列出所有会话 |
| `POST` | `/api/uploads/{id}/retry` | 合并失败后重试 |
| `DELETE` | `/api/uploads/{id}` | 取消会话并清理分片 |
| `GET` | `/api/files/{filename}` | 下载合并完成的文件 |
| `GET` | `/api/health` | 健康检查 |

### 1. 创建会话

```bash
curl -s -X POST http://localhost:8000/api/uploads \
  -H 'Content-Type: application/json' \
  -d '{
        "filename": "video.mp4",
        "total_size": 10485760,
        "chunk_size": 1048576,
        "file_sha256": "<可选: 整个文件的 SHA-256>"
      }'
```

响应（`missing_chunks` 初始为全部序号）：

```json
{
  "id": "9f2c...",
  "filename": "video.mp4",
  "status": "pending",
  "total_size": 10485760,
  "chunk_size": 1048576,
  "total_chunks": 10,
  "received_chunks": 0,
  "missing_chunks": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  "file_sha256": null,
  "error": null,
  "download_url": null,
  "created_at": 1789950000.0,
  "updated_at": 1789950000.0
}
```

### 2. 上传分片

请求体为分片原始字节，SHA-256 通过查询参数 `sha256` 或请求头 `X-Chunk-SHA256` 提供：

```bash
# 计算分片哈希并上传第 3 片（可乱序）
HASH=$(sha256sum chunk_003.bin | cut -d' ' -f1)
curl -X PUT "http://localhost:8000/api/uploads/<id>/chunks/3" \
  -H "X-Chunk-SHA256: $HASH" \
  --data-binary @chunk_003.bin
```

- 哈希不匹配、大小不符、序号越界均返回 `422`，修正后可直接重试
- 同一序号重复上传是幂等的（后写覆盖）
- 最后一个分片到齐后自动触发合并

### 3. 查询状态 / 断点续传

```bash
curl -s http://localhost:8000/api/uploads/<id>
```

客户端重启后重新请求该接口，只需继续上传 `missing_chunks` 中列出的序号即可。

### 4. 下载合并结果

合并完成后 `status` 变为 `completed`，`download_url` 指向 `/api/files/<filename>`：

```bash
curl -OJ http://localhost:8000/api/files/video.mp4
```

### 完整客户端示例

```bash
python examples/client.py ./bigfile.bin --chunk-size 4194304
# 中断后再跑一次同一命令（加 --session <id>）即可续传
```

## 状态机

```
pending ──全部分片到齐──▶ merging ──校验通过──▶ completed
   ▲                        │
   └── retry ◀── 校验失败 ──┘ (failed)
```

- `pending`：等待分片
- `merging`：正在合并（服务重启时自动回退为 `pending`，下次触发重新合并）
- `completed`：合并完成，分片已清理，文件可下载
- `failed`：合并失败（如整文件哈希不匹配），`error` 字段含原因，可 `POST /retry` 重试

## 存储布局

```
$STORAGE_DIR/
├── uploads.db          # SQLite: 会话与分片记录 (WAL 模式)
├── chunks/<id>/        # 各会话的分片暂存 (00000000.part ...)
└── files/              # 合并完成的目标文件
```

## 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `8000` | 监听端口 |
| `STORAGE_DIR` | `./storage`（容器内 `/data/storage`） | 存储根目录，启动时自动创建 |
| `MAX_CHUNK_SIZE` | `67108864` (64 MiB) | 单个分片大小上限 |

## 可靠性设计

- **分片写入**：先写 `*.part.tmp` 再 `os.replace`，避免半截文件
- **合并**：写入 `.<filename>.<id>.tmp` → 校验整文件 SHA-256 → `os.replace` 原子替换目标文件
- **重启恢复**：启动时扫描数据库，将中断的 `merging` 会话回退为 `pending`；逐条核对分片记录与磁盘文件（存在且大小一致），记录与文件不一致时以磁盘为准清理孤儿数据
- **并发**：会话级互斥锁保护分片写入与合并；SQLite WAL 模式支持并发读写

## 运行测试

```bash
pip install -r requirements.txt httpx
python tests/smoke_test.py   # 端到端: 乱序/重试/坏哈希/重启续传/原子合并
```
