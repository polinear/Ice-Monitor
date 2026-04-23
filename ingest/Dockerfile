FROM python:3.11-slim

RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    opencv-python-headless==4.9.0.80 \
    redis==5.0.1 \
    kafka-python==2.0.2

WORKDIR /app
COPY main.py .

CMD ["python", "-u", "main.py"]
