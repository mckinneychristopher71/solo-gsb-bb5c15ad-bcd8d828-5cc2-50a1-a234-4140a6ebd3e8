"""分片文件上传服务.

特性:
- 创建上传会话 (POST /api/uploads)
- 按序号上传分片, 带 SHA-256 校验, 支持乱序与重试 (PUT /api/uploads/{id}/chunks/{index})
- 查询会话状态与缺失分片 (GET /api/uploads/{id})
- 全部分片校验通过后原子合并为目标文件
- SQLite 持久化, 服务重启后可继续未完成的会话
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

STORAGE_DIR = Path(os.environ.get("STORAGE_DIR", "./storage")).resolve()
CHUNK_DIR = STORAGE_DIR / "chunks"      # 各会话的分片暂存: chunks/<session_id>/<index>.part
FILE_DIR = STORAGE_DIR / "files"        # 合并完成的目标文件
DB_PATH = STORAGE_DIR / "uploads.db"

MAX_CHUNK_SIZE = int(os.environ.get("MAX_CHUNK_SIZE", str(64 * 1024 * 1024)))  # 64 MiB

STATUS_PENDING = "pending"
STATUS_MERGING = "merging"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

# ---------------------------------------------------------------------------
# 数据库
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id           TEXT PRIMARY KEY,
    filename     TEXT NOT NULL,
    total_size   INTEGER NOT NULL,
    chunk_size   INTEGER NOT NULL,
    total_chunks INTEGER NOT NULL,
    file_sha256  TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',
    error        TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
    session_id  TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    received_at REAL NOT NULL,
    PRIMARY KEY (session_id, chunk_index)
);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_db() -> None:
    with _connect() as conn:
        conn.executescript(_SCHEMA)


# ---------------------------------------------------------------------------
# 启动恢复: 初始化目录 / 数据库,  reconciliate 中断的状态
# ---------------------------------------------------------------------------


def _chunk_path(session_id: str, index: int) -> Path:
    return CHUNK_DIR / session_id / f"{index:08d}.part"


def _recover_state() -> None:
    """服务重启后调用, 使未完成的会话可以继续上传."""
    with _connect() as conn:
        # 合并过程中断的会话回到 pending, 允许重新触发合并
        conn.execute(
            "UPDATE sessions SET status=?, updated_at=? WHERE status=?",
            (STATUS_PENDING, time.time(), STATUS_MERGING),
        )

        # 校验数据库中记录的分片在磁盘上真实存在且大小一致, 否则删除记录
        rows = conn.execute(
            """
            SELECT c.session_id AS sid, c.chunk_index AS idx, c.size AS size
            FROM chunks c
            JOIN sessions s ON s.id = c.session_id
            WHERE s.status != ?
            """,
            (STATUS_COMPLETED,),
        ).fetchall()
        for row in rows:
            path = _chunk_path(row["sid"], row["idx"])
            if not path.is_file() or path.stat().st_size != row["size"]:
                conn.execute(
                    "DELETE FROM chunks WHERE session_id=? AND chunk_index=?",
                    (row["sid"], row["idx"]),
                )

        # 清理磁盘上没有对应数据库记录的分片文件
        valid_sessions = {
            r["id"]
            for r in conn.execute(
                "SELECT id FROM sessions WHERE status != ?", (STATUS_COMPLETED,)
            ).fetchall()
        }
        recorded = {
            (r["session_id"], r["chunk_index"])
            for r in conn.execute("SELECT session_id, chunk_index FROM chunks").fetchall()
        }

    if CHUNK_DIR.is_dir():
        for session_dir in CHUNK_DIR.iterdir():
            if not session_dir.is_dir():
                continue
            if session_dir.name not in valid_sessions:
                shutil.rmtree(session_dir, ignore_errors=True)
                continue
            for part in session_dir.glob("*.part"):
                try:
                    index = int(part.stem)
                except ValueError:
                    part.unlink(missing_ok=True)
                    continue
                if (session_dir.name, index) not in recorded:
                    part.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# 会话级锁 (合并与状态变更的互斥)
# ---------------------------------------------------------------------------

_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _session_lock(session_id: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(session_id, threading.Lock())


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------


class CreateSessionRequest(BaseModel):
    filename: str = Field(..., min_length=1, max_length=255)
    total_size: int = Field(..., ge=0)
    chunk_size: int = Field(..., gt=0)
    file_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-fA-F]{64}$",
        description="整个文件的 SHA-256, 可选; 提供时合并后会校验",
    )


class SessionResponse(BaseModel):
    id: str
    filename: str
    status: str
    total_size: int
    chunk_size: int
    total_chunks: int
    received_chunks: int
    missing_chunks: list[int]
    file_sha256: str | None
    error: str | None
    download_url: str | None
    created_at: float
    updated_at: float


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def _safe_filename(name: str) -> str:
    """防止路径穿越, 只保留文件名部分."""
    name = os.path.basename(name).strip()
    if not name or name in (".", ".."):
        raise HTTPException(status_code=422, detail="非法的文件名")
    return name


def _get_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="上传会话不存在")
    return row


def _session_response(conn: sqlite3.Connection, row: sqlite3.Row) -> SessionResponse:
    if row["status"] == STATUS_COMPLETED:
        # 合并完成后分片记录已清理, 直接视为全部收到
        received_count = row["total_chunks"]
        missing: list[int] = []
    else:
        received = {
            r["chunk_index"]
            for r in conn.execute(
                "SELECT chunk_index FROM chunks WHERE session_id=?", (row["id"],)
            ).fetchall()
        }
        received_count = len(received)
        missing = [i for i in range(row["total_chunks"]) if i not in received]
    return SessionResponse(
        id=row["id"],
        filename=row["filename"],
        status=row["status"],
        total_size=row["total_size"],
        chunk_size=row["chunk_size"],
        total_chunks=row["total_chunks"],
        received_chunks=received_count,
        missing_chunks=missing,
        file_sha256=row["file_sha256"],
        error=row["error"],
        download_url=f"/api/files/{row['filename']}" if row["status"] == STATUS_COMPLETED else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _expected_chunk_size(session: sqlite3.Row, index: int) -> int:
    if index < session["total_chunks"] - 1:
        return session["chunk_size"]
    # 最后一个分片 (当 total_size 恰好整除时与 chunk_size 相同)
    return session["total_size"] - session["chunk_size"] * (session["total_chunks"] - 1)


def _try_merge(session_id: str) -> None:
    """若所有分片均已收到, 则原子合并为目标文件.

    写临时文件 -> 校验整体 SHA-256 (如提供) -> os.replace 原子落盘.
    """
    lock = _session_lock(session_id)
    with lock:
        with _connect() as conn:
            session = _get_session(conn, session_id)
            if session["status"] != STATUS_PENDING:
                return
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM chunks WHERE session_id=?", (session_id,)
            ).fetchone()["c"]
            if count != session["total_chunks"]:
                return
            conn.execute(
                "UPDATE sessions SET status=?, updated_at=? WHERE id=?",
                (STATUS_MERGING, time.time(), session_id),
            )

        filename = session["filename"]
        tmp_path = FILE_DIR / f".{filename}.{session_id}.tmp"
        final_path = FILE_DIR / filename
        try:
            hasher = hashlib.sha256()
            with open(tmp_path, "wb") as out:
                for i in range(session["total_chunks"]):
                    with open(_chunk_path(session_id, i), "rb") as part:
                        while True:
                            block = part.read(1024 * 1024)
                            if not block:
                                break
                            hasher.update(block)
                            out.write(block)

            expected = session["file_sha256"]
            if expected and hasher.hexdigest().lower() != expected.lower():
                raise ValueError("合并后文件的整体 SHA-256 校验失败")

            os.replace(tmp_path, final_path)  # 原子替换

            with _connect() as conn:
                conn.execute(
                    "UPDATE sessions SET status=?, error=NULL, updated_at=? WHERE id=?",
                    (STATUS_COMPLETED, time.time(), session_id),
                )
                conn.execute("DELETE FROM chunks WHERE session_id=?", (session_id,))
            shutil.rmtree(CHUNK_DIR / session_id, ignore_errors=True)
        except Exception as exc:  # noqa: BLE001 - 需要记录任意失败原因
            tmp_path.unlink(missing_ok=True)
            with _connect() as conn:
                conn.execute(
                    "UPDATE sessions SET status=?, error=?, updated_at=? WHERE id=?",
                    (STATUS_FAILED, str(exc), time.time(), session_id),
                )


# ---------------------------------------------------------------------------
# FastAPI 应用
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时自动初始化存储目录 / 数据库, 并恢复未完成的会话
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    FILE_DIR.mkdir(parents=True, exist_ok=True)
    _init_db()
    _recover_state()
    yield


app = FastAPI(title="分片文件上传服务", version="1.0.0", lifespan=lifespan)


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/api/uploads", status_code=201, response_model=SessionResponse)
def create_session(body: CreateSessionRequest) -> SessionResponse:
    """创建上传会话, 返回会话 ID 与全部分片序号 (初始全部缺失)."""
    filename = _safe_filename(body.filename)
    if body.chunk_size > MAX_CHUNK_SIZE:
        raise HTTPException(status_code=422, detail=f"chunk_size 超过上限 {MAX_CHUNK_SIZE}")
    total_chunks = (body.total_size + body.chunk_size - 1) // body.chunk_size
    if body.total_size > 0 and total_chunks == 0:
        raise HTTPException(status_code=422, detail="total_size 与 chunk_size 不匹配")

    session_id = uuid.uuid4().hex
    now = time.time()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sessions
                (id, filename, total_size, chunk_size, total_chunks, file_sha256,
                 status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                filename,
                body.total_size,
                body.chunk_size,
                total_chunks,
                body.file_sha256.lower() if body.file_sha256 else None,
                STATUS_PENDING,
                now,
                now,
            ),
        )
        (CHUNK_DIR / session_id).mkdir(parents=True, exist_ok=True)
        row = _get_session(conn, session_id)
        response = _session_response(conn, row)
    if total_chunks == 0:
        # 零字节文件: 无分片可传, 立即合并完成
        _try_merge(session_id)
        with _connect() as conn:
            response = _session_response(conn, _get_session(conn, session_id))
    return response


@app.put("/api/uploads/{session_id}/chunks/{chunk_index}", response_model=SessionResponse)
async def upload_chunk(
    session_id: str,
    chunk_index: int,
    request: Request,
    sha256: str | None = None,
    x_chunk_sha256: str | None = Header(default=None),
) -> SessionResponse:
    """上传一个分片.

    请求体为分片原始字节; SHA-256 通过查询参数 `sha256` 或请求头
    `X-Chunk-SHA256` 提供. 校验失败返回 422, 可安全重试; 分片可乱序上传.
    """
    digest = (sha256 or x_chunk_sha256 or "").lower()
    if not digest or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise HTTPException(
            status_code=422,
            detail="必须通过查询参数 sha256 或请求头 X-Chunk-SHA256 提供分片的 SHA-256",
        )

    data = await request.body()

    with _session_lock(session_id):
        with _connect() as conn:
            session = _get_session(conn, session_id)
            if session["status"] == STATUS_COMPLETED:
                raise HTTPException(status_code=409, detail="会话已完成, 无法再上传分片")
            if session["status"] == STATUS_MERGING:
                raise HTTPException(status_code=409, detail="会话正在合并, 请稍后查询状态")
            if not 0 <= chunk_index < session["total_chunks"]:
                raise HTTPException(
                    status_code=422,
                    detail=f"分片序号越界, 合法范围 0..{session['total_chunks'] - 1}",
                )

            expected_size = _expected_chunk_size(session, chunk_index)
            if len(data) != expected_size:
                raise HTTPException(
                    status_code=422,
                    detail=f"分片大小不符: 期望 {expected_size} 字节, 实际 {len(data)} 字节",
                )
            if hashlib.sha256(data).hexdigest() != digest:
                raise HTTPException(status_code=422, detail="分片 SHA-256 校验失败")

            # 原子写入分片文件 (重试时直接覆盖)
            session_dir = CHUNK_DIR / session_id
            session_dir.mkdir(parents=True, exist_ok=True)
            part_path = _chunk_path(session_id, chunk_index)
            tmp_path = part_path.with_suffix(".part.tmp")
            with open(tmp_path, "wb") as f:
                f.write(data)
            os.replace(tmp_path, part_path)

            conn.execute(
                """
                INSERT INTO chunks (session_id, chunk_index, size, sha256, received_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (session_id, chunk_index)
                DO UPDATE SET size=excluded.size, sha256=excluded.sha256,
                              received_at=excluded.received_at
                """,
                (session_id, chunk_index, len(data), digest, time.time()),
            )
            conn.execute(
                "UPDATE sessions SET status=?, error=NULL, updated_at=? WHERE id=?",
                (STATUS_PENDING, time.time(), session_id),
            )

    _try_merge(session_id)
    # 合并可能改变了状态, 重新读取
    with _connect() as conn:
        return _session_response(conn, _get_session(conn, session_id))


@app.get("/api/uploads/{session_id}", response_model=SessionResponse)
def get_session(session_id: str) -> SessionResponse:
    """查询会话状态与缺失分片列表."""
    with _connect() as conn:
        return _session_response(conn, _get_session(conn, session_id))


@app.get("/api/uploads", response_model=list[SessionResponse])
def list_sessions() -> list[SessionResponse]:
    """列出所有上传会话."""
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM sessions ORDER BY created_at DESC").fetchall()
        return [_session_response(conn, row) for row in rows]


@app.post("/api/uploads/{session_id}/retry", response_model=SessionResponse)
def retry_session(session_id: str) -> SessionResponse:
    """合并失败的会话可通过该接口重置为 pending 并重新触发合并."""
    with _connect() as conn:
        session = _get_session(conn, session_id)
        if session["status"] != STATUS_FAILED:
            raise HTTPException(status_code=409, detail="仅合并失败的会话可以重试")
        conn.execute(
            "UPDATE sessions SET status=?, error=NULL, updated_at=? WHERE id=?",
            (STATUS_PENDING, time.time(), session_id),
        )
    _try_merge(session_id)
    with _connect() as conn:
        return _session_response(conn, _get_session(conn, session_id))


@app.delete("/api/uploads/{session_id}", status_code=204, response_class=Response)
def delete_session(session_id: str):
    """取消会话并清理其分片数据 (已完成的会话只删除记录, 保留文件)."""
    with _session_lock(session_id):
        with _connect() as conn:
            _get_session(conn, session_id)
            conn.execute("DELETE FROM chunks WHERE session_id=?", (session_id,))
            conn.execute("DELETE FROM sessions WHERE id=?", (session_id,))
        shutil.rmtree(CHUNK_DIR / session_id, ignore_errors=True)
        _locks.pop(session_id, None)


@app.get("/api/files/{filename}")
def download_file(filename: str):
    """下载合并完成的文件."""
    path = FILE_DIR / _safe_filename(filename)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="文件不存在")
    return FileResponse(path, filename=path.name)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
