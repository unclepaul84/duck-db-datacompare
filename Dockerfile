FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set up application directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY delta_lens/ /app/delta_lens/
COPY cli.py .

# Create directory for data
RUN mkdir -p /data

# Set environment variables with defaults
ENV DELTALENS_CONFIG=compare.config.json \
    DELTALENS_OUTPUT_DIR=output \
    DELTALENS_LOG_LEVEL=INFO \
    DELTALENS_EXPORT_SQLITE=true \
    DELTALENS_CONTINUE_ON_ERROR=true \
    DELTALENS_SQLITE_SAMPLE=10000

# Set volume for data and configuration
VOLUME ["/data"]

# Create wrapper script to change directory and run CLI
RUN echo '#!/bin/sh\ncd /data\npython /app/cli.py "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# Set entrypoint to our wrapper script
ENTRYPOINT ["/app/entrypoint.sh"]