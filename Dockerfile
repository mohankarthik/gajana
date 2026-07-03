# Gajana - personal finance pipeline, run as a scheduled container.
# supercronic fires `python main.py --daily` / `--backup-db` on a baked crontab
# so the container behaves like every other long-running homelab service.
FROM python:3.12-slim

# TZ so cron fires at local (India) time; tzdata for zoneinfo.
ENV TZ=Asia/Kolkata \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# --- supercronic (a container-friendly cron) ---
ARG SUPERCRONIC_VERSION=v0.2.33
ARG TARGETARCH=amd64
RUN set -eux; \
    curl -fsSLO "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${TARGETARCH}"; \
    chmod +x "supercronic-linux-${TARGETARCH}"; \
    mv "supercronic-linux-${TARGETARCH}" /usr/local/bin/supercronic

WORKDIR /app

# Install Python deps first for layer caching.
COPY requirements.txt .
RUN pip install -r requirements.txt

# App code + parsing configs baked in; personal data (secrets/, settings.json,
# matchers.json, cache, state, backups) is bind-mounted at runtime.
COPY main.py run_gmail_fetcher.py ./
COPY src/ ./src/
COPY plugins/ ./plugins/
COPY data/configs/ ./data/configs/
COPY crontab ./crontab

CMD ["supercronic", "/app/crontab"]
