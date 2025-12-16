FROM python:3.11-slim

LABEL maintainer="Log Transporter"
LABEL description="Transfers logs from source servers to destination via intermediate host"

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN mkdir -p /app/config /app/keys /app/state

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY log_transporter.py .
RUN chmod +x log_transporter.py

RUN chmod 700 /app/keys

ENV PYTHONUNBUFFERED=1
ENV CONFIG_PATH=/app/config/config.yaml

HEALTHCHECK --interval=60s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f log_transporter || exit 1

ENTRYPOINT ["python", "/app/log_transporter.py"]
CMD ["--config", "/app/config/config.yaml", "--continuous"]
