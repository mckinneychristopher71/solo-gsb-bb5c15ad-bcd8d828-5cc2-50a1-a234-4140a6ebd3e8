#!/usr/bin/env python3
"""端到端冒烟测试: 乱序上传 / 重试 / 校验失败 / 重启续传 / 原子合并.

用法: python tests/smoke_test.py
依赖: httpx (pip install httpx)
"""

import hashlib
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import httpx

BASE = "http://127.0.0.1:8000"
ROOT = Path(__file__).resolve().parent.parent
STORAGE = Path("/tmp/test-storage")

ENV = {
    **os.environ,
    "STORAGE_DIR": str(STORAGE),
    "PORT": "8000",
    "PYTHONPATH": str(ROOT),
}


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def wait_up() -> None:
    for _ in range(50):
        try:
            if httpx.get(f"{BASE}/api/health", timeout=1).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError("server did not start")


def start_server() -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", "8000"],
        cwd=ROOT, env=ENV,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    wait_up()
    return proc


def stop_server(proc: subprocess.Popen) -> None:
    proc.terminate()
    proc.wait(timeout=10)


def main() -> None:
    shutil.rmtree(STORAGE, ignore_errors=True)

    chunk_size = 1024 * 1024
    data = os.urandom(chunk_size * 4 + 12345)  # 5 个分片, 最后一个不满
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    file_hash = sha(data)

    proc = start_server()
    try:
        c = httpx.Client(base_url=BASE, timeout=10)

        # 1. 创建会话
        r = c.post("/api/uploads", json={
            "filename": "test.bin", "total_size": len(data),
            "chunk_size": chunk_size, "file_sha256": file_hash,
        })
        assert r.status_code == 201, r.text
        s = r.json()
        sid = s["id"]
        assert s["total_chunks"] == 5 and s["missing_chunks"] == [0, 1, 2, 3, 4], s
        print("ok: create session, all chunks missing")

        # 2. 坏哈希被拒绝
        r = c.put(f"/api/uploads/{sid}/chunks/0", content=chunks[0],
                  headers={"X-Chunk-SHA256": "0" * 64})
        assert r.status_code == 422, r.text
        print("ok: bad sha256 rejected with 422")

        # 3. 乱序上传 (先 3 后 1)
        for i in (3, 1):
            r = c.put(f"/api/uploads/{sid}/chunks/{i}", content=chunks[i],
                      headers={"X-Chunk-SHA256": sha(chunks[i])})
            assert r.status_code == 200, r.text
        s = c.get(f"/api/uploads/{sid}").json()
        assert s["missing_chunks"] == [0, 2, 4] and s["status"] == "pending", s
        print("ok: out-of-order upload, missing = [0, 2, 4]")

        # 4. 重复上传同一分片幂等
        r = c.put(f"/api/uploads/{sid}/chunks/1", content=chunks[1],
                  headers={"X-Chunk-SHA256": sha(chunks[1])})
        assert r.status_code == 200, r.text
        s = c.get(f"/api/uploads/{sid}").json()
        assert s["received_chunks"] == 2, s
        print("ok: retry same chunk is idempotent")

        # 5. 重启服务, 会话可继续
        stop_server(proc)
        proc = start_server()
        s = c.get(f"/api/uploads/{sid}").json()
        assert s["missing_chunks"] == [0, 2, 4], s
        print("ok: session survives restart, missing = [0, 2, 4]")

        # 6. 上传剩余分片 (乱序 4, 0, 2), 最后一片触发自动合并
        for i in (4, 0, 2):
            r = c.put(f"/api/uploads/{sid}/chunks/{i}", content=chunks[i],
                      headers={"X-Chunk-SHA256": sha(chunks[i])})
            assert r.status_code == 200, r.text
        s = r.json()
        assert s["status"] == "completed" and s["missing_chunks"] == [], s
        assert s["download_url"] == "/api/files/test.bin", s
        print("ok: all chunks uploaded, auto-merged, status=completed")

        # 7. 合并结果字节一致
        merged = (STORAGE / "files" / "test.bin").read_bytes()
        assert merged == data and sha(merged) == file_hash
        print("ok: merged file matches original byte-for-byte")

        # 8. 下载接口
        r = c.get("/api/files/test.bin")
        assert r.status_code == 200 and r.content == data
        print("ok: download endpoint returns identical bytes")

        # 9. 已完成会话拒绝继续上传
        r = c.put(f"/api/uploads/{sid}/chunks/0", content=chunks[0],
                  headers={"X-Chunk-SHA256": sha(chunks[0])})
        assert r.status_code == 409, r.text
        print("ok: completed session rejects new chunks with 409")

        # 10. 序号越界与大小不符
        r = c.post("/api/uploads", json={
            "filename": "t2.bin", "total_size": 100, "chunk_size": 50})
        sid2 = r.json()["id"]
        r = c.put(f"/api/uploads/{sid2}/chunks/2", content=b"x" * 50,
                  headers={"X-Chunk-SHA256": sha(b"x" * 50)})
        assert r.status_code == 422, r.text
        r = c.put(f"/api/uploads/{sid2}/chunks/0", content=b"x" * 10,
                  headers={"X-Chunk-SHA256": sha(b"x" * 10)})
        assert r.status_code == 422, r.text
        print("ok: out-of-range index and wrong size rejected")

        # 11. 整文件哈希不匹配 -> failed, 可重试
        r = c.post("/api/uploads", json={
            "filename": "t3.bin", "total_size": 10, "chunk_size": 10,
            "file_sha256": "f" * 64})
        sid3 = r.json()["id"]
        r = c.put(f"/api/uploads/{sid3}/chunks/0", content=b"0123456789",
                  headers={"X-Chunk-SHA256": sha(b"0123456789")})
        assert r.json()["status"] == "failed", r.text
        r = c.post(f"/api/uploads/{sid3}/retry")
        assert r.json()["status"] == "failed"
        print("ok: whole-file hash mismatch -> failed, retry endpoint works")

        # 12. 删除会话清理分片
        r = c.delete(f"/api/uploads/{sid2}")
        assert r.status_code == 204
        assert not (STORAGE / "chunks" / sid2).exists()
        print("ok: delete session cleans up chunk files")

        print("\nALL TESTS PASSED")
    finally:
        stop_server(proc)


if __name__ == "__main__":
    main()
