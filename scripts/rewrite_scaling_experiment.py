from __future__ import annotations

import json
import os
from pathlib import Path


NOTEBOOK_PATH = Path(__file__).resolve().parents[1] / "notebooks" / "scaling_experiment.ipynb"


def replace_cell_source(cell: dict, new_source: str) -> None:
    cell["source"] = [line + "\n" for line in new_source.rstrip().splitlines()]
    if cell.get("cell_type") == "code":
        cell["execution_count"] = None
        cell["outputs"] = []


def main() -> None:
    nb = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))

    md_plan = """# Эксперимент по масштабируемости Spark

Задача ТЗ: *"Провести эксперимент по масштабируемости. Насколько увеличится время выполнения при увеличении количества данных. Зависимость должна быть близка к линейной."*

**Честная методика:**
1. Сгенерировать синтетические детекции в HDFS
2. Выполнить одну и ту же агрегацию на каждом объёме
3. Сделать warm-up прогон и несколько повторов в случайном порядке
4. Считать медиану, минимум, максимум и разброс, а не один лучший запуск
5. Построить графики по медиане и показать raw points
"""

    md_method = """## Шаг 2. Функция замера времени одной задачи

Задача - реалистичная агрегация: группировка по `cam_id` и `class_name`,
подсчёт сумм и средних. Для правдивого теста используем:

- один warm-up прогон для прогрева JVM и планировщика Spark;
- несколько повторов для каждой точки;
- случайный порядок запусков, чтобы не подгонять результат под рост объёма;
- медиану и разброс вместо одного "лучшего" времени.
"""

    md_run = """## Шаг 3. Гоняем эксперимент

Сначала выполняется warm-up на минимальном наборе, потом все прогоны перемешиваются.
Такой режим лучше отражает реальную работу Spark, чем одиночный запуск по возрастанию данных.
"""

    md_plot = """## Шаг 4. Графики

На графиках показываем не только агрегированную линию, но и отдельные измерения.
Если где-то скорость не растёт, это тоже полезный результат: он показывает накладные расходы.
"""

    md_speedup = """### 4.2 Ускорение (speedup) от количества executors

Speedup считаем по медианным временам: `baseline / current`.
Если speedup < 1, это означает, что дополнительные executors не окупили накладные расходы.
"""

    md_linear = """### 4.3 Проверка линейности - коэффициент R²

Оцениваем линейность по медианным точкам. Это не "подгонка" результата, а способ понять,
насколько близка зависимость времени к прямой линии в условиях реального кластера.
"""

    setup_source = """import os, time, json, random
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, lit, expr, when, col, floor, sum as ssum, avg

SPARK_MASTER = "spark://spark-master:7077"
HDFS = "hdfs://namenode:9000"

SIZES = [int(x) for x in os.environ.get("SCALING_SIZES", "1000000,5000000,20000000,50000000").split(",") if x.strip()]
EXECUTOR_COUNTS = [int(x) for x in os.environ.get("SCALING_EXECUTORS", "1,2").split(",") if x.strip()]
REPEATS = int(os.environ.get("SCALING_REPEATS", "3"))
WARMUP = int(os.environ.get("SCALING_WARMUP", "1"))
GEN_CHUNK_ROWS = int(os.environ.get("SCALING_GEN_CHUNK_ROWS", "250000"))


def make_session(app_name: str, executors: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.executor.instances", str(executors))
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1500m")
        .config("spark.sql.shuffle.partitions", str(max(4, executors * 4)))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.files.maxRecordsPerFile", str(GEN_CHUNK_ROWS))
        .getOrCreate()
    )


def generate_detections(spark: SparkSession, num_rows: int, out_path: str) -> None:
    classes = ["vessel", "ice_field", "broken_ice", "slush_ice"]
    class_literal = ",".join([chr(39) + c + chr(39) for c in classes])
    wrote_any = False

    for start in range(0, num_rows, GEN_CHUNK_ROWS):
        batch_rows = min(GEN_CHUNK_ROWS, num_rows - start)
        partitions = max(4, min(16, max(1, batch_rows // 50000)))
        batch = (
            spark.range(start, start + batch_rows, 1, partitions)
            .withColumn("cam_id", when(col("id") % 2 == 0, "cam_a").otherwise("cam_b"))
            .withColumn("class_idx", floor(rand(seed=42 + start) * len(classes)).cast("int"))
            .withColumn("class_name", expr(f"element_at(array({class_literal}), class_idx + 1)"))
            .withColumn("confidence", rand(seed=123 + start) * 0.6 + 0.4)
            .withColumn("area_pixels", floor(rand(seed=456 + start) * 50000 + 100).cast("int"))
            .withColumn("ice_conc", rand(seed=789 + start))
            .withColumn("ice_severity", rand(seed=101112 + start))
            .withColumn("dt", lit("2026-04-18"))
            .drop("id", "class_idx")
        )
        mode = "overwrite" if not wrote_any else "append"
        batch.write.mode(mode).parquet(out_path)
        wrote_any = True


def run_query(spark: SparkSession, data_path: str):
    df = spark.read.parquet(data_path)
    result = (
        df.groupBy("cam_id", "class_name")
        .agg(
            ssum("area_pixels").alias("sum_area_pixels"),
            avg("confidence").alias("avg_confidence"),
            avg("ice_conc").alias("avg_ice_conc"),
        )
    )
    return result.collect()


def benchmark_executor_count(num_executors: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    spark = make_session(f"scaling-{num_executors}", num_executors)
    trials = []
    try:
        warmup_path = f"{HDFS}/experiments/det_{min(SIZES)}"
        if WARMUP:
            _ = run_query(spark, warmup_path)
            spark.catalog.clearCache()

        schedule = [(size, rep) for size in SIZES for rep in range(REPEATS)]
        random.Random(42 + num_executors).shuffle(schedule)

        for size, rep in schedule:
            path = f"{HDFS}/experiments/det_{size}"
            spark.catalog.clearCache()
            t0 = time.perf_counter()
            result_rows = run_query(spark, path)
            elapsed = time.perf_counter() - t0
            trials.append(
                {
                    "rows": size,
                    "executors": num_executors,
                    "trial": rep + 1,
                    "time_s": elapsed,
                    "result_rows": len(result_rows),
                }
            )
            print(
                f"executors={num_executors} rows={size:>10,} trial={rep + 1} "
                f"time={elapsed:.2f}s result_rows={len(result_rows)}"
            )

        trial_df = pd.DataFrame(trials).sort_values(["rows", "trial"]).reset_index(drop=True)
        summary_df = (
            trial_df.groupby(["rows", "executors"], as_index=False)
            .agg(
                runs=("time_s", "count"),
                time_median_s=("time_s", "median"),
                time_mean_s=("time_s", "mean"),
                time_std_s=("time_s", "std"),
                time_min_s=("time_s", "min"),
                time_max_s=("time_s", "max"),
            )
            .sort_values("rows")
            .reset_index(drop=True)
        )
        summary_df["time_std_s"] = summary_df["time_std_s"].fillna(0.0)
        return trial_df, summary_df
    finally:
        spark.stop()


def linear_r2(x: np.ndarray, y: np.ndarray) -> float:
    coeffs = np.polyfit(x, y, 1)
    predicted = np.polyval(coeffs, x)
    ss_res = float(np.sum((y - predicted) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    return 1.0 if ss_tot == 0 else 1 - ss_res / ss_tot
"""

    helper_source = """def make_session(app_name: str, executors: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.executor.instances", str(executors))
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1500m")
        .config("spark.sql.shuffle.partitions", str(max(4, executors * 4)))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.files.maxRecordsPerFile", str(GEN_CHUNK_ROWS))
        .getOrCreate()
    )


def generate_detections(spark: SparkSession, num_rows: int, out_path: str) -> None:
    classes = ["vessel", "ice_field", "broken_ice", "slush_ice"]
    class_literal = ",".join([chr(39) + c + chr(39) for c in classes])
    wrote_any = False

    for start in range(0, num_rows, GEN_CHUNK_ROWS):
        batch_rows = min(GEN_CHUNK_ROWS, num_rows - start)
        partitions = max(4, min(16, max(1, batch_rows // 50000)))
        batch = (
            spark.range(start, start + batch_rows, 1, partitions)
            .withColumn("cam_id", when(col("id") % 2 == 0, "cam_a").otherwise("cam_b"))
            .withColumn("class_idx", floor(rand(seed=42 + start) * len(classes)).cast("int"))
            .withColumn("class_name", expr(f"element_at(array({class_literal}), class_idx + 1)"))
            .withColumn("confidence", rand(seed=123 + start) * 0.6 + 0.4)
            .withColumn("area_pixels", floor(rand(seed=456 + start) * 50000 + 100).cast("int"))
            .withColumn("ice_conc", rand(seed=789 + start))
            .withColumn("ice_severity", rand(seed=101112 + start))
            .withColumn("dt", lit("2026-04-18"))
            .drop("id", "class_idx")
        )
        mode = "overwrite" if not wrote_any else "append"
        batch.write.mode(mode).parquet(out_path)
        wrote_any = True


def run_query(spark: SparkSession, data_path: str):
    df = spark.read.parquet(data_path)
    result = (
        df.groupBy("cam_id", "class_name")
        .agg(
            ssum("area_pixels").alias("sum_area_pixels"),
            avg("confidence").alias("avg_confidence"),
            avg("ice_conc").alias("avg_ice_conc"),
        )
    )
    return result.collect()


def benchmark_executor_count(num_executors: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    spark = make_session(f"scaling-{num_executors}", num_executors)
    trials = []
    try:
        warmup_path = f"{HDFS}/experiments/det_{min(SIZES)}"
        if WARMUP:
            _ = run_query(spark, warmup_path)
            spark.catalog.clearCache()

        schedule = [(size, rep) for size in SIZES for rep in range(REPEATS)]
        random.Random(42 + num_executors).shuffle(schedule)

        for size, rep in schedule:
            path = f"{HDFS}/experiments/det_{size}"
            spark.catalog.clearCache()
            t0 = time.perf_counter()
            result_rows = run_query(spark, path)
            elapsed = time.perf_counter() - t0
            trials.append(
                {
                    "rows": size,
                    "executors": num_executors,
                    "trial": rep + 1,
                    "time_s": elapsed,
                    "result_rows": len(result_rows),
                }
            )
            print(
                f"executors={num_executors} rows={size:>10,} trial={rep + 1} "
                f"time={elapsed:.2f}s result_rows={len(result_rows)}"
            )

        trial_df = pd.DataFrame(trials).sort_values(["rows", "trial"]).reset_index(drop=True)
        summary_df = (
            trial_df.groupby(["rows", "executors"], as_index=False)
            .agg(
                runs=("time_s", "count"),
                time_median_s=("time_s", "median"),
                time_mean_s=("time_s", "mean"),
                time_std_s=("time_s", "std"),
                time_min_s=("time_s", "min"),
                time_max_s=("time_s", "max"),
            )
            .sort_values("rows")
            .reset_index(drop=True)
        )
        summary_df["time_std_s"] = summary_df["time_std_s"].fillna(0.0)
        return trial_df, summary_df
    finally:
        spark.stop()


def linear_r2(x: np.ndarray, y: np.ndarray) -> float:
    coeffs = np.polyfit(x, y, 1)
    predicted = np.polyval(coeffs, x)
    ss_res = float(np.sum((y - predicted) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    return 1.0 if ss_tot == 0 else 1 - ss_res / ss_tot
"""

    run_source = """executor_counts = EXECUTOR_COUNTS
all_trials = []
summary_parts = []

spark = make_session("gen-data", 2)
print(f"Sizes: {SIZES}")
print(f"Repeat count: {REPEATS}")
print(f"Chunk size: {GEN_CHUNK_ROWS:,} rows")
for n in SIZES:
    path = f"{HDFS}/experiments/det_{n}"
    print(f"Generating {n:,} rows -> {path}")
    generate_detections(spark, n, path)
print("Synthetic datasets are ready")
spark.stop()

for n_ex in executor_counts:
    trial_df, summary_df = benchmark_executor_count(n_ex)
    all_trials.append(trial_df)
    summary_parts.append(summary_df)

trial_df = pd.concat(all_trials, ignore_index=True).sort_values(["executors", "rows", "trial"]).reset_index(drop=True)
summary_df = pd.concat(summary_parts, ignore_index=True).sort_values(["executors", "rows"]).reset_index(drop=True)

display(trial_df)
display(summary_df)
"""

    plot_source = """fig, ax = plt.subplots(figsize=(9, 5))
for n_ex in EXECUTOR_COUNTS:
    sub_summary = summary_df[summary_df.executors == n_ex].sort_values("rows")
    sub_trials = trial_df[trial_df.executors == n_ex]
    ax.scatter(sub_trials.rows / 1e6, sub_trials.time_s, alpha=0.22, s=22)
    ax.errorbar(
        sub_summary.rows / 1e6,
        sub_summary.time_median_s,
        yerr=[sub_summary.time_median_s - sub_summary.time_min_s, sub_summary.time_max_s - sub_summary.time_median_s],
        marker="o",
        capsize=4,
        linewidth=2,
        label=f"{n_ex} executors (median ± range)",
    )
ax.set_xlabel("Объём данных (млн строк)")
ax.set_ylabel("Время (секунды)")
ax.set_title("Зависимость времени выполнения от объёма данных")
ax.grid(True, alpha=0.3)
ax.legend()
plt.tight_layout()
plt.savefig("scaling_time_vs_rows.png", dpi=150, bbox_inches="tight")
plt.show()
"""

    speedup_source = """pivot = summary_df.pivot(index="rows", columns="executors", values="time_median_s").sort_index()
baseline_exec = EXECUTOR_COUNTS[0]
baseline = pivot[baseline_exec]

speedup = pd.DataFrame(index=pivot.index)
for n_ex in EXECUTOR_COUNTS[1:]:
    speedup[n_ex] = baseline / pivot[n_ex]

fig, ax = plt.subplots(figsize=(9, 5))
for col_name in speedup.columns:
    ax.plot(
        speedup.index / 1e6,
        speedup[col_name],
        marker="s",
        label=f"{col_name} executors",
    )
ax.axhline(1.0, color="gray", linestyle="--", alpha=0.5, label="baseline")
ax.set_xlabel("Объём данных (млн строк)")
ax.set_ylabel("Speedup")
ax.set_title("Ускорение относительно 1 executor")
ax.grid(True, alpha=0.3)
ax.legend()
plt.tight_layout()
plt.savefig("scaling_speedup.png", dpi=150, bbox_inches="tight")
plt.show()
"""

    linear_source = """for n_ex in EXECUTOR_COUNTS:
    sub = summary_df[summary_df.executors == n_ex].sort_values("rows")
    x = sub.rows.to_numpy(dtype=float)
    y = sub.time_median_s.to_numpy(dtype=float)
    r2 = linear_r2(x, y)
    print(f"executors={n_ex}: R² = {r2:.4f}")
"""

    replacements = {
        "Задача ТЗ: *\"Провести эксперимент по масштабируемости. Насколько увеличится время выполнения при увеличении количества данных. Зависимость должна быть близка к линейной.\"*": md_plan,
        "Задача — реалистичная агрегация: группировка по `cam_id` и `class_name`,": md_method,
        "Сначала выполняется warm-up на минимальном наборе, потом все прогоны перемешиваются.": md_run,
        "На графиках показываем не только агрегированную линию, но и отдельные измерения.": md_plot,
        "Speedup считаем по медианным временам: `baseline / current`.": md_speedup,
        "Оцениваем линейность по медианным точкам. Это не \"подгонка\" результата, а способ понять,": md_linear,
    }

    for cell in nb["cells"]:
        if cell.get("cell_type") == "markdown":
            src = "".join(cell.get("source", []))
            for needle, new_src in replacements.items():
                if needle in src:
                    replace_cell_source(cell, new_src)
                    break
        elif cell.get("cell_type") == "code":
            src = "".join(cell.get("source", []))
            if "GEN_CHUNK_ROWS" in src and "SIZES =" in src:
                replace_cell_source(cell, setup_source)
            elif "def run_job" in src:
                replace_cell_source(cell, helper_source)
            elif "executor_counts = [1, 2]" in src:
                replace_cell_source(cell, run_source)
            elif "fig, ax = plt.subplots(figsize=(9, 5))" in src and "ax.plot(sub.rows / 1e6" in src:
                replace_cell_source(cell, plot_source)
            elif "pivot = df.pivot(index=\"rows\"" in src:
                replace_cell_source(cell, speedup_source + "\n\n" + linear_source)
            elif "### 4.3 Проверка линейности" in src:
                replace_cell_source(cell, linear_source)

    # Append a short note if notebook has not already been updated.
    NOTEBOOK_PATH.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Updated notebook: {NOTEBOOK_PATH}")


if __name__ == "__main__":
    main()
