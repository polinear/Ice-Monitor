FROM python:3.10-slim-bullseye

# PySpark 3.3.x is more stable on Python 3.10 than on 3.11 for local jobs
# that create small helper DataFrames inside the container.

RUN printf 'Acquire::Retries "5";\nAcquire::http::Timeout "30";\nAcquire::https::Timeout "30";\n' >/etc/apt/apt.conf.d/80-retries && \
    apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    numpy==1.26.4 \
    pyspark==3.3.0

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ENV PYTHONUNBUFFERED=1

WORKDIR /jobs
COPY . .
