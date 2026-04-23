.PHONY: help up down logs clean etl mllib hive-cli analytics \
        check-hdfs topics-list init-data benchmark-workers hotpath-benchmark

help:
	@echo "Ice Monitor commands:"
	@echo "  make up                - build and start all services"
	@echo "  make down              - stop everything"
	@echo "  make clean             - stop everything and remove volumes"
	@echo "  make logs SVC=xxx      - show service logs"
	@echo "  make etl               - run batch ETL HDFS -> Hive"
	@echo "  make mllib             - train MLlib model and run scaling experiment"
	@echo "  make hive-cli          - open HiveQL shell"
	@echo "  make analytics         - run Hive analytics queries"
	@echo "  make check-hdfs        - inspect HDFS raw data"
	@echo "  make topics-list       - list Kafka topics"
	@echo "  make benchmark-workers - start benchmark worker services"
	@echo "  make hotpath-benchmark - measure hot-path latency and throughput"

up:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f $(SVC)

# --- Big data ---

etl:
	docker compose exec -T spark-master /spark/bin/spark-submit \
		--master local[*] \
		/jobs/etl_to_hive.py

mllib:
	docker compose exec -T -e SPARK_MASTER=local[*] etl-runner \
		python /jobs/mllib_danger_model.py

hive-cli:
	docker compose exec hive-server beeline -u jdbc:hive2://localhost:10000

analytics:
	docker compose exec hive-server beeline -u jdbc:hive2://localhost:10000 \
		-f /jobs/analytics_queries.sql

# --- Checks ---

check-hdfs:
	@echo "=== HDFS: /raw ==="
	docker compose exec namenode hdfs dfs -ls -R /raw 2>/dev/null | head -30
	@echo "=== HDFS: /user/hive/warehouse ==="
	docker compose exec namenode hdfs dfs -ls /user/hive/warehouse 2>/dev/null

topics-list:
	docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

benchmark-workers:
	docker compose up -d --build worker_bench_1 worker_bench_2 worker_bench_3 worker_bench_4

hotpath-benchmark: benchmark-workers
	python scripts/hotpath_benchmark.py
