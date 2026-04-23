"""
Обучает RandomForestClassifier из Spark MLlib для предсказания уровня
опасности (safe/caution/danger).

При наличии достаточного числа строк использует часовую агрегацию.
Если hourly_summary ещё мала для демо, автоматически переключается на
кадровую таблицу ice.frames, чтобы компонент можно было запустить уже
через несколько минут после старта стенда.
"""
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

spark_master = os.environ.get("SPARK_MASTER", "local[*]")

builder = (
    SparkSession.builder
    .appName("mllib-danger")
    .master(spark_master)
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
)

spark = builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(f"Spark master: {spark_master}", flush=True)

MIN_TRAIN_ROWS = 20
MIN_TEST_ROWS = 5


def load_training_data():
    hourly_df = spark.sql("""
        SELECT avg_ice_conc, max_ice_conc, avg_severity,
               total_frames, danger_level
        FROM ice.hourly_summary
        WHERE avg_ice_conc IS NOT NULL
    """).cache()
    hourly_total = hourly_df.count()

    if hourly_total >= 100:
        print(f"Использую hourly_summary: {hourly_total} строк", flush=True)
        return hourly_df, hourly_total, [
            "avg_ice_conc",
            "max_ice_conc",
            "avg_severity",
            "total_frames",
        ], "ice.hourly_summary"

    print(
        f"hourly_summary пока мала ({hourly_total} строк), "
        "переключаюсь на кадровую витрину ice.frames",
        flush=True,
    )
    hourly_df.unpersist()

    frames_df = (
        spark.table("ice.frames")
        .where(
            col("ice_conc").isNotNull()
            & col("ice_severity").isNotNull()
        )
        .withColumn(
            "danger_level",
            when((col("ice_conc") > 0.7) | (col("ice_severity") > 0.55), "danger")
            .when((col("ice_conc") > 0.4) | (col("ice_severity") > 0.3), "caution")
            .otherwise("safe"),
        )
        .select(
            "ice_conc",
            "ice_severity",
            "ice_field_area",
            "broken_ice_area",
            "slush_ice_area",
            "danger_level",
        )
        .cache()
    )
    total = frames_df.count()
    print(f"Использую ice.frames: {total} строк", flush=True)
    return frames_df, total, [
        "ice_conc",
        "ice_severity",
        "ice_field_area",
        "broken_ice_area",
        "slush_ice_area",
    ], "ice.frames"


df, total, feature_cols, source_table = load_training_data()

if total < 40:
    print("⚠ Слишком мало данных, подожди ещё немного и повтори запуск")
    spark.stop()
    raise SystemExit()

# Пайплайн
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
indexer = StringIndexer(inputCol="danger_level", outputCol="label")
rf = RandomForestClassifier(featuresCol="features", labelCol="label",
                             numTrees=50, maxDepth=8, seed=42)
pipeline = Pipeline(stages=[assembler, indexer, rf])
evaluator = MulticlassClassificationEvaluator(metricName="accuracy",
                                               labelCol="label")

# Эксперимент: разные доли обучающей выборки
fractions = [0.1, 0.3, 0.6, 1.0]
print(f"\n{'Доля':<8} {'Строк':<10} {'Время (с)':<12} {'Accuracy':<10}")
print("-" * 44)

results = []
for frac in fractions:
    sample = df.sample(withReplacement=False, fraction=frac, seed=42)
    train, test = sample.randomSplit([0.8, 0.2], seed=42)
    train_size = train.count()
    test_size = test.count()

    if train_size < MIN_TRAIN_ROWS or test_size < MIN_TEST_ROWS:
        print(
            f"{frac*100:>4.0f}%   пропуск: train={train_size}, test={test_size}",
            flush=True,
        )
        continue

    t0 = time.time()
    model = pipeline.fit(train)
    pred = model.transform(test)
    acc = evaluator.evaluate(pred)
    duration = time.time() - t0

    print(f"{frac*100:>4.0f}%   {train_size:<10} {duration:<12.2f} {acc:<10.4f}")
    results.append({
        "fraction": frac,
        "rows": train_size,
        "test_rows": test_size,
        "time_s": duration,
        "accuracy": acc,
        "source_table": source_table,
    })

if not results:
    print("⚠ После разбиения не осталось достаточно данных для обучения", flush=True)
    spark.stop()
    raise SystemExit()

# Сохраняем метрики эксперимента как Hive-таблицу для отчёта
results_df = spark.createDataFrame(results)
results_df.write.mode("overwrite").saveAsTable("ice.model_scaling_experiment")

print(
    "\n✓ Результаты сохранены в Hive: ice.model_scaling_experiment "
    f"(source={source_table})",
    flush=True,
)
spark.stop()
