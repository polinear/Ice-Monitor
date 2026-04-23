"""
Batch ETL — запускается периодически (см. etl_runner.py).
Читает сырые Parquet из HDFS, чистит, обогащает, пишет в Hive-таблицы.

Запуск вручную:
    docker compose exec spark-master spark-submit \
        --master local[*] \
        /jobs/etl_to_hive.py
"""
import os
import socket
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_trunc,
    max as smax,
    lit,
    sum as ssum,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def resolve_hdfs_uri() -> str:
    host = os.environ.get("HDFS_HOST", "namenode")
    port = os.environ.get("HDFS_PORT", "9000")
    override = os.environ.get("HDFS_URI")
    if override:
        return override
    try:
        host = socket.gethostbyname(host)
    except OSError:
        pass
    return f"hdfs://{host}:{port}"


HDFS = resolve_hdfs_uri()
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
HIVE_METASTORE_URIS = os.environ.get(
    "HIVE_METASTORE_URIS", "thrift://hive-metastore:9083"
)

RAW_DETECTION_SCHEMA = StructType(
    [
        StructField("ts", TimestampType(), True),
        StructField("cam_id", StringType(), True),
        StructField("class_name", StringType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("area_pixels", IntegerType(), True),
        StructField("bbox_x1", IntegerType(), True),
        StructField("bbox_y1", IntegerType(), True),
        StructField("bbox_x2", IntegerType(), True),
        StructField("bbox_y2", IntegerType(), True),
        StructField("ice_conc", DoubleType(), True),
        StructField("ice_severity", DoubleType(), True),
        StructField("dt", DateType(), True),
    ]
)

RAW_FRAME_SCHEMA = StructType(
    [
        StructField("ts", TimestampType(), True),
        StructField("cam_id", StringType(), True),
        StructField("ice_conc", DoubleType(), True),
        StructField("ice_severity", DoubleType(), True),
        StructField("vessel_present", BooleanType(), True),
        StructField("pixels", MapType(StringType(), IntegerType()), True),
        StructField("dt", DateType(), True),
    ]
)


spark = (
    SparkSession.builder.appName("etl-to-hive")
    .master(SPARK_MASTER)
    .config("spark.hadoop.hadoop.security.token.service.use_ip", "true")
    .config("spark.hadoop.fs.defaultFS", HDFS)
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.warehouse.dir", f"{HDFS}/user/hive/warehouse")
    .config("hive.metastore.uris", HIVE_METASTORE_URIS)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.hive.convertMetastoreParquet", "true")
    .config("spark.sql.legacy.parquet.nanosAsLong", "false")
    .config("spark.ui.enabled", "false")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def read_parquet_or_empty(path: str, schema: StructType, label: str):
    try:
        df = spark.read.schema(schema).parquet(path)
        if df.rdd.isEmpty():
            print(f"{label}: empty dataset at {path}", flush=True)
        else:
            print(f"{label}: data loaded from {path}", flush=True)
        return df
    except Exception as exc:
        print(
            f"{label}: source is not ready yet at {path}; "
            f"using empty dataframe ({exc})",
            flush=True,
        )
        select_expr = ", ".join(
            f"CAST(NULL AS {field.dataType.simpleString()}) AS `{field.name}`"
            for field in schema.fields
        )
        # Build the empty DataFrame on the JVM side so ETL stays compatible
        # with PySpark environments that are sensitive to Python pickling.
        return spark.sql(f"SELECT {select_expr} WHERE 1 = 0")


# === 1. Целевые Hive-таблицы ===
spark.sql("CREATE DATABASE IF NOT EXISTS ice")
spark.sql("USE ice")

spark.sql(
    """
CREATE TABLE IF NOT EXISTS ice.detections (
    ts             TIMESTAMP,
    class_name     STRING,
    confidence     DOUBLE,
    area_pixels    INT,
    bbox_x1        INT,
    bbox_y1        INT,
    bbox_x2        INT,
    bbox_y2        INT,
    ice_conc       DOUBLE,
    ice_severity   DOUBLE
)
PARTITIONED BY (dt DATE, cam_id STRING)
STORED AS PARQUET
"""
)

spark.sql(
    """
CREATE TABLE IF NOT EXISTS ice.frames (
    ts              TIMESTAMP,
    ice_conc        DOUBLE,
    ice_severity    DOUBLE,
    vessel_present  BOOLEAN,
    vessel_area     INT,
    ice_field_area  INT,
    broken_ice_area INT,
    slush_ice_area  INT
)
PARTITIONED BY (dt DATE, cam_id STRING)
STORED AS PARQUET
"""
)

spark.sql(
    """
CREATE TABLE IF NOT EXISTS ice.hourly_summary (
    hour            TIMESTAMP,
    cam_id          STRING,
    avg_ice_conc    DOUBLE,
    max_ice_conc    DOUBLE,
    avg_severity    DOUBLE,
    vessel_frames   BIGINT,
    total_frames    BIGINT,
    danger_level    STRING
)
STORED AS PARQUET
"""
)

# === 2. Чтение сырых данных ===
raw_detections = read_parquet_or_empty(
    f"{HDFS}/raw/ice_detections",
    RAW_DETECTION_SCHEMA,
    "detections",
)
raw_frames = read_parquet_or_empty(
    f"{HDFS}/raw/frames",
    RAW_FRAME_SCHEMA,
    "frames",
)

if raw_frames.limit(1).count() == 0:
    print("No frame-level parquet data yet. ETL skipped.", flush=True)
    spark.stop()
    sys.exit(0)

# === 3. Очистка ===
cleaned_det = (
    raw_detections.filter(col("confidence") > 0.3)
    .dropDuplicates(["ts", "cam_id", "class_name", "bbox_x1", "bbox_y1"])
    .select(
        "ts",
        "class_name",
        "confidence",
        "area_pixels",
        "bbox_x1",
        "bbox_y1",
        "bbox_x2",
        "bbox_y2",
        "ice_conc",
        "ice_severity",
        "dt",
        "cam_id",
    )
)

cleaned_frames = (
    raw_frames.withColumn("vessel_area", col("pixels").getItem("vessel"))
    .withColumn("ice_field_area", col("pixels").getItem("ice_field"))
    .withColumn("broken_ice_area", col("pixels").getItem("broken_ice"))
    .withColumn("slush_ice_area", col("pixels").getItem("slush_ice"))
    .select(
        "ts",
        "ice_conc",
        "ice_severity",
        "vessel_present",
        "vessel_area",
        "ice_field_area",
        "broken_ice_area",
        "slush_ice_area",
        "dt",
        "cam_id",
    )
)


def insert_overwrite_partitioned(df, table: str, partition_cols):
    """
    Пишет в Hive-таблицу с динамическим overwrite только тех партиций,
    которые есть во входном DataFrame. Сохраняет схему и партиционирование.
    insertInto использует ПОЗИЦИОННЫЙ матчинг, поэтому партиционные колонки
    должны быть в конце и в правильном порядке.
    """
    ordered = df.select(
        *[c for c in df.columns if c not in partition_cols],
        *partition_cols,
    )
    ordered.write.insertInto(table, overwrite=True)


# === 4. Запись в Hive ===
if cleaned_det.limit(1).count() > 0:
    print("Запись очищенных детекций...", flush=True)
    insert_overwrite_partitioned(cleaned_det, "ice.detections", ["dt", "cam_id"])
else:
    print("Детекций после фильтрации нет — пропускаем таблицу ice.detections", flush=True)

print("Запись кадровой статистики...", flush=True)
insert_overwrite_partitioned(cleaned_frames, "ice.frames", ["dt", "cam_id"])

# === 5. Агрегация по часам ===
print("Строю часовую агрегацию...", flush=True)
hourly = (
    cleaned_frames.groupBy(date_trunc("hour", col("ts")).alias("hour"), "cam_id")
    .agg(
        avg("ice_conc").alias("avg_ice_conc"),
        smax("ice_conc").alias("max_ice_conc"),
        avg("ice_severity").alias("avg_severity"),
        lit(0).cast("bigint").alias("vessel_frames"),
        count("*").alias("total_frames"),
    )
    .withColumn(
        "danger_level",
        when(col("avg_ice_conc") > 0.7, "danger")
        .when(col("avg_ice_conc") > 0.4, "caution")
        .otherwise("safe"),
    )
    .select(
        "hour",
        "cam_id",
        "avg_ice_conc",
        "max_ice_conc",
        "avg_severity",
        "vessel_frames",
        "total_frames",
        "danger_level",
    )
)

# hourly_summary непартиционированная — spark просто переписывает таблицу
hourly.write.mode("overwrite").format("parquet").saveAsTable("ice.hourly_summary")

# === 6. Статистика ===
print("\n=== Результаты ETL ===", flush=True)
for table in ("ice.detections", "ice.frames", "ice.hourly_summary"):
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()[0]["n"]
        print(f"  {table:<25} {cnt:>12,} rows", flush=True)
    except Exception as exc:
        print(f"  {table:<25} не доступна: {exc}", flush=True)

spark.stop()
