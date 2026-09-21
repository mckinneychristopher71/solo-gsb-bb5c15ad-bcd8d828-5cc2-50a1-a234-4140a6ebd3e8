FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STORAGE_DIR=/data/storage \
    PORT=8000

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 启动时自动初始化存储目录并恢复未完成的会话
RUN mkdir -p /data/storage

EXPOSE 8000

VOLUME ["/data/storage"]

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
