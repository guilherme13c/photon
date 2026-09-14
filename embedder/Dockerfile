FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (needed for confluent-kafka and ray)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# The Embedder's production image is CPU-constrained. Use the CPU wheel rather
# than silently downloading CUDA runtimes that cannot be used in this service.
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu -r requirements.txt

COPY src/ ./src/

ENV PYTHONPATH=/app
EXPOSE 8000 8002
CMD ["python", "src/main.py"]
