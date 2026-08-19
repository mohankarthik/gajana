# Gajana - personal finance pipeline, run as a scheduled container.
# supercronic fires `python main.py --daily` / `--backup-db` on a baked crontab
# so the container behaves like every other long-running homelab service.
#
# Python base + tzdata + supercronic live in cron-base:local (shared with
# nalam) -- see homelab/base-images/cron-base. deploy_gajana.yml builds it
# before this image.
FROM cron-base:local

WORKDIR /app

# Install Python deps first for layer caching.
COPY requirements.txt .
RUN pip install -r requirements.txt

# App code + parsing configs baked in; personal data (secrets/, settings.json,
# matchers.json, cache, state, backups) is bind-mounted at runtime.
COPY main.py run_gmail_fetcher.py run_salary_splitter.py run_telegram_bot.py ./
COPY src/ ./src/
COPY plugins/ ./plugins/
COPY data/configs/ ./data/configs/
COPY crontab ./crontab

CMD ["supercronic", "/app/crontab"]
