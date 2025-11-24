FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     gcc \
#     g++ \
#     && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY to_parquet.py .
COPY data_streaming.py .

# Set environment variables
# Note: KAFKA_BROKER should be set via docker-compose.yaml
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["bash"]
