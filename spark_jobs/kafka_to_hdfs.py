"""
Spark Structured Streaming job that reads Kafka detections and writes Parquet
files to HDFS for the cold path.

Запускается в отдельном контейнере `kafka-to-hdfs` как standalone Python
процесс (Spark local mode). Это надёжнее, чем сабмитить в кластер Bitnami
workers, где нет pyspark-окружения и Kafka-коннектора.
"""
import os
import signal
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, from_unixtime, to_date
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
HDFS = os.environ.get("HDFS_URI", "hdfs://namenode:9000")
# Локальный режим самый надёжный: контейнер уже имеет pyspark + JVM,
# а Kafka-коннектор тянется через spark.jars.packages прямо в процесс.
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
KAFKA_PACKAGES = os.environ.get(
    "KAFKA_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
    "org.apache.commons:commons-pool2:2.11.1",
)
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ice.detections")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")
TRIGGER_SEC = os.environ.get("TRIGGER_SECONDS", "30")

FRAME_PATH = f"{HDFS}/raw/frames"
FRAME_CHECKPOINT = f"{HDFS}/tmp/checkpoints/ice_frames"
DETECTION_PATH = f"{HDFS}/raw/ice_detections"
DETECTION_CHECKPOINT = f"{HDFS}/tmp/checkpoints/ice_detections"


def wait_for_kafka(host_port: str, retries: int = 60, delay: int = 3) -> None:
    """Не запускаемся, пока брокер не доступен — иначе Spark падает сразу."""
    import socket

    host, port = host_port.split(":")
    port = int(port)
    for attempt in range(1, retries + 1):
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[kafka-to-hdfs] Kafka {host}:{port} reachable", flush=True)
                return
        except OSError as exc:
            print(
                f"[kafka-to-hdfs] Waiting for Kafka {host}:{port} "
                f"({attempt}/{retries}): {exc}",
                flush=True,
            )
            time.sleep(delay)
    raise SystemExit(f"Kafka {host_port} did not become reachable")


def wait_for_hdfs(uri: str, retries: int = 60, delay: int = 3) -> None:
    import socket
    from urllib.parse import urlparse

    parsed = urlparse(uri)
    host = parsed.hostname or "namenode"
    port = parsed.port or 9000
    for attempt in range(1, retries + 1):
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[kafka-to-hdfs] HDFS {host}:{port} reachable", flush=True)
                return
        except OSError as exc:
            print(
                f"[kafka-to-hdfs] Waiting for HDFS {host}:{port} "
                f"({attempt}/{retries}): {exc}",
                flush=True,
            )
            time.sleep(delay)
    raise SystemExit(f"HDFS {uri} did not become reachable")


wait_for_kafka(KAFKA)
wait_for_hdfs(HDFS)

spark = (
    SparkSession.builder.appName("kafka-to-hdfs")
    .master(SPARK_MASTER)
    .config("spark.jars.packages", KAFKA_PACKAGES)
    .config("spark.hadoop.fs.defaultFS", HDFS)
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.streaming.minBatchesToRetain", "10")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def graceful_exit(signum, _frame):
    print(f"[kafka-to-hdfs] received signal {signum}, stopping streams", flush=True)
    for q in spark.streams.active:
        try:
            q.stop()
        except Exception as exc:
            print(f"[kafka-to-hdfs] stop error: {exc}", flush=True)
    spark.stop()
    sys.exit(0)


for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, graceful_exit)


detection_schema = StructType(
    [
        StructField("class_name", StringType()),
        StructField("confidence", DoubleType()),
        StructField("area_pixels", IntegerType()),
        StructField("bbox", ArrayType(IntegerType())),
    ]
)

msg_schema = StructType(
    [
        StructField("ts", DoubleType()),
        StructField("cam_id", StringType()),
        StructField("ice_conc", DoubleType()),
        StructField("ice_severity", DoubleType()),
        StructField("vessel_present", BooleanType()),
        StructField("pixels", MapType(StringType(), IntegerType())),
        StructField("detections", ArrayType(detection_schema)),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "5000")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), msg_schema).alias("m"))
    .where(col("m").isNotNull())
    .select("m.*")
    .where(col("ts").isNotNull() & col("cam_id").isNotNull())
    .withColumn("event_time", from_unixtime(col("ts")).cast("timestamp"))
    .withColumn("dt", to_date(col("event_time")))
)

frame_stream = parsed.select(
    col("event_time").alias("ts"),
    col("cam_id"),
    col("ice_conc"),
    col("ice_severity"),
    col("vessel_present"),
    col("pixels"),
    col("dt"),
)

detection_stream = (
    parsed.withColumn("det", explode("detections"))
    .select(
        col("event_time").alias("ts"),
        col("cam_id"),
        col("det.class_name").alias("class_name"),
        col("det.confidence").alias("confidence"),
        col("det.area_pixels").alias("area_pixels"),
        col("det.bbox").getItem(0).alias("bbox_x1"),
        col("det.bbox").getItem(1).alias("bbox_y1"),
        col("det.bbox").getItem(2).alias("bbox_x2"),
        col("det.bbox").getItem(3).alias("bbox_y2"),
        col("ice_conc"),
        col("ice_severity"),
        col("dt"),
    )
)

frame_query = (
    frame_stream.writeStream.format("parquet")
    .option("path", FRAME_PATH)
    .option("checkpointLocation", FRAME_CHECKPOINT)
    .partitionBy("dt", "cam_id")
    .outputMode("append")
    .trigger(processingTime=f"{TRIGGER_SEC} seconds")
    .queryName("frames_to_hdfs")
    .start()
)

detection_query = (
    detection_stream.writeStream.format("parquet")
    .option("path", DETECTION_PATH)
    .option("checkpointLocation", DETECTION_CHECKPOINT)
    .partitionBy("dt", "cam_id")
    .outputMode("append")
    .trigger(processingTime=f"{TRIGGER_SEC} seconds")
    .queryName("detections_to_hdfs")
    .start()
)

print(
    f"[kafka-to-hdfs] streaming started. "
    f"Kafka={KAFKA} topic={KAFKA_TOPIC} HDFS={HDFS} master={SPARK_MASTER}",
    flush=True,
)

# awaitAnyTermination корректно дожидается падения любого из стримов,
# иначе один query блокирует второй навсегда.
spark.streams.awaitAnyTermination()
