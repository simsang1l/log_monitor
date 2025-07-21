#!/bin/bash

# Spark 애플리케이션 실행 스크립트
echo "Kafka to Elasticsearch Spark 애플리케이션을 시작합니다..."

# Spark 마스터 URL
SPARK_MASTER="spark://spark-master:7077"

# 애플리케이션 실행
docker exec -it spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.executor.memory=2g" \
    --conf "spark.driver.memory=2g" \
    /opt/bitnami/spark/work/spark/kafka_to_elasticsearch.py

echo "Spark 애플리케이션이 종료되었습니다." 