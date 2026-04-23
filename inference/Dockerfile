FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 && \
    rm -rf /var/lib/apt/lists/*

# ultralytics тянет torch как зависимость; для CPU-only этого достаточно
RUN pip install --no-cache-dir \
    ultralytics==8.3.0 \
    redis==5.0.1 \
    kafka-python==2.0.2 \
    opencv-python-headless==4.9.0.80 \
    numpy==1.26.4

WORKDIR /app
COPY worker.py .

CMD ["python", "-u", "worker.py"]
