FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 \
    -r /app/requirements.txt

COPY main.py /app/main.py
COPY templates /app/templates
COPY static /app/static

RUN set -eux; \
    for i in 1 2 3; do \
        apt-get update -o Acquire::Retries=3 && break || sleep 5; \
    done; \
    apt-get install -y --no-install-recommends curl ffmpeg; \
    rm -rf /var/lib/apt/lists/*

EXPOSE 2617

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:2617/health || exit 1

CMD ["python", "main.py"]
