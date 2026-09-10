FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY trpc_service ./trpc_service
COPY config ./config
COPY data ./data

EXPOSE 8000
ENV PYTHONUNBUFFERED=1

# gateway 与 worker 同镜像，不同启动命令
CMD ["python", "-m", "trpc_service.web.app"]
