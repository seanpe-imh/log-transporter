FROM python:3.11-slim

LABEL maintainer="Log Transporter"
LABEL description="Transfers logs from source servers to destination via intermediate host"

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-client \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create app directory structure
WORKDIR /app
RUN mkdir -p /app/config /app/keys /app/state

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY log_transporter.py .
RUN chmod +x log_transporter.py

# Set proper permissions for SSH keys directory
RUN chmod 700 /app/keys

# Default environment variables
ENV PYTHONUNBUFFERED=1
ENV CONFIG_PATH=/app/config/config.yaml

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f log_transporter || exit 1

# Run the transporter in continuous mode by default
ENTRYPOINT ["python", "/app/log_transporter.py"]
CMD ["--config", "/app/config/config.yaml", "--continuous"]
