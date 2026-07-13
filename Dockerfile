# ===== Stage 1: Build Vue frontend =====
FROM node:22-slim AS frontend-builder

WORKDIR /build

# Copy dependency manifests first for layer caching
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

# Copy source and build
COPY frontend/ .
RUN npm run build

# ===== Stage 2: Python runtime =====
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system packages
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' \
        /etc/apt/sources.list.d/*.list /etc/apt/sources.list 2>/dev/null || true; \
    apt-get update -o Acquire::Retries=3; \
    apt-get install -y --no-install-recommends curl ffmpeg; \
    rm -rf /var/lib/apt/lists/*

# Copy Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --default-timeout=120 --retries=10 \
    -r /app/requirements.txt

# Copy backend code
COPY main.py /app/main.py

# Copy static assets from repo (set.png, etc.)
COPY static /app/static

# Copy built frontend from builder stage
COPY --from=frontend-builder /static/dist /app/static/dist

EXPOSE 2617

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:2617/health || exit 1

CMD ["python", "main.py"]
