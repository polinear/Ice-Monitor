FROM python:3.11-slim

RUN pip install --no-cache-dir \
    fastapi==0.110.0 \
    "uvicorn[standard]==0.27.1" \
    redis==5.0.1 \
    asyncpg==0.29.0 \
    websockets==12.0

WORKDIR /app
COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
