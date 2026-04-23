FROM python:3.11-slim

RUN pip install --no-cache-dir \
    streamlit==1.31.0 \
    websocket-client==1.7.0 \
    numpy==1.26.4 \
    opencv-python-headless==4.9.0.80 \
    requests==2.31.0 \
    pandas==2.2.0

WORKDIR /app
COPY . .

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
