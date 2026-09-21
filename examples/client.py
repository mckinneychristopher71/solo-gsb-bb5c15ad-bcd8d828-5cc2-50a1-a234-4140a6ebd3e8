#!/usr/bin/env python3
"""分片上传客户端示例: 支持断点续传 (仅上传缺失分片).

用法:
    python examples/client.py <文件路径> [--chunk-size 4194304] [--base http://127.0.0.1:8000]
"""

import argparse
import hashlib
import os
import sys

import httpx


def sha256_file(path: str, chunk_size: int) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description="分片上传客户端 (断点续传)")
    parser.add_argument("file", help="要上传的文件")
    parser.add_argument("--chunk-size", type=int, default=4 * 1024 * 1024, help="分片大小, 默认 4 MiB")
    parser.add_argument("--base", default="http://127.0.0.1:8000", help="服务地址")
    parser.add_argument("--session", default=None, help="已有会话 ID (续传时跳过创建)")
    args = parser.parse_args()

    path = args.file
    total_size = os.path.getsize(path)
    filename = os.path.basename(path)
    file_hash = sha256_file(path, args.chunk_size)

    with httpx.Client(base_url=args.base, timeout=60) as client:
        if args.session:
            session = client.get(f"/api/uploads/{args.session}").json()
        else:
            session = client.post("/api/uploads", json={
                "filename": filename,
                "total_size": total_size,
                "chunk_size": args.chunk_size,
                "file_sha256": file_hash,
            }).json()
        sid = session["id"]
        print(f"会话 {sid}  状态 {session['status']}  缺失分片 {len(session['missing_chunks'])} 个")

        with open(path, "rb") as f:
            for index in session["missing_chunks"]:
                f.seek(index * args.chunk_size)
                data = f.read(args.chunk_size)
                digest = hashlib.sha256(data).hexdigest()
                r = client.put(
                    f"/api/uploads/{sid}/chunks/{index}",
                    content=data,
                    headers={"X-Chunk-SHA256": digest},
                )
                r.raise_for_status()
                print(f"  分片 {index} 已上传 ({len(data)} 字节)")

        final = client.get(f"/api/uploads/{sid}").json()
        print(f"完成: 状态 {final['status']}", file=sys.stderr)
        if final["status"] != "completed":
            sys.exit(f"上传未完成: {final.get('error')}")
        print(f"下载地址: {args.base}{final['download_url']}")


if __name__ == "__main__":
    main()
