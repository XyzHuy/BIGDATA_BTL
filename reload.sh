#!/bin/bash
set -e  # dừng ngay nếu có lỗi
echo "[Reload] Resetting simulation..."

# 1 Dừng tất cả producers và Spark app
docker compose stop producer-1 producer-2 producer-3 producer-4 spark-app
docker compose rm -f producer-1 producer-2 producer-3 producer-4 spark-app

# 2 Xóa Kafka topics cũ
echo "[Reload] Deleting old Kafka topics..."
# Xóa từng topic (Kafka không hỗ trợ xóa nhiều topic bằng wildcard trong CLI)
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic icu_data_1 || true
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic icu_data_2 || true
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic icu_data_3 || true
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic icu_data_4 || true

# 3 Tạo lại Kafka topics
echo "[Reload] Recreating Kafka topics via kafka-init..."
docker compose run --rm kafka-init

# 4 Xóa checkpoint (sau khi Spark đã dừng)
echo "[Reload] Clearing Spark checkpoints..."
docker compose run --rm spark-app bash -c "rm -rf /tmp/checkpoint" || true
# 5 Xóa dữ liệu Cassandra
echo "[Reload] Clearing Cassandra data..."
docker exec cassandra cqlsh -e "TRUNCATE sepsis_monitoring.icu_readings;"

# 6 Khởi động lại Spark app
echo "[Reload] Starting Spark app..."
docker compose up -d spark-app


# 7 Khi Spark healthy -> khởi động lại producers
echo "[Reload] Starting producers..."
docker compose up -d producer-1 producer-2 producer-3 producer-4

echo "[Reload] Done "

# 6 Báo lại cho Flask host
curl -s -X POST http://host.docker.internal:6000/reload-done \
     -H "Content-Type: application/json" \
     -d '{"status": "done"}' \
     && echo "[Reload] Notified Flask host."