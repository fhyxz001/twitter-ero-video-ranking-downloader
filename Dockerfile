# ===== Stage 1: Build Vue frontend =====
FROM node:22-slim AS frontend-builder

WORKDIR /build

# Copy dependency manifests first for layer caching
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

# Copy source and build
COPY frontend/ .
RUN npm run build

# ===== Stage 2: Install Python dependencies =====
# 独立阶段安装依赖，运行时镜像不携带 pip
FROM python:3.11-slim AS python-builder

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --default-timeout=120 --retries=10 \
        --prefix=/install -r /tmp/requirements.txt

# ===== Stage 3: Python runtime =====
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system packages (curl 仅用于健康检查)
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' \
        /etc/apt/sources.list.d/*.list /etc/apt/sources.list 2>/dev/null || true; \
    apt-get update -o Acquire::Retries=3; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*

# 只拷贝依赖，并移除 pip（运行时用不到）
COPY --from=python-builder /install/lib/python3.11/site-packages \
     /usr/local/lib/python3.11/site-packages
RUN rm -rf /usr/local/lib/python3.11/site-packages/pip \
           /usr/local/lib/python3.11/site-packages/pip-*.dist-info \
           /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.11

# Copy backend code (entry + app package)
COPY main.py /app/main.py
COPY app /app/app

# Copy static assets from repo (set.png, etc.)
COPY static /app/static

# Copy built frontend from builder stage
COPY --from=frontend-builder /static/dist /app/static/dist

EXPOSE 2617

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:2617/health || exit 1

CMD ["python", "main.py"]
