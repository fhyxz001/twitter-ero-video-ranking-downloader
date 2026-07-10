FROM ccr.ccs.tencentyun.com/library/python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system packages first (better layer caching, TLS verification kept on)
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' \
        /etc/apt/sources.list.d/*.list /etc/apt/sources.list 2>/dev/null || true; \
    apt-get update -o Acquire::Retries=3; \
    apt-get install -y --no-install-recommends curl ffmpeg; \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --default-timeout=120 --retries=10 \
    -r /app/requirements.txt

COPY main.py /app/main.py
COPY templates /app/templates
COPY static /app/static

EXPOSE 2617

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:2617/health || exit 1

CMD ["python", "main.py"]
