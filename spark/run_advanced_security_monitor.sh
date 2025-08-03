#!/bin/bash

# 고급 보안 모니터링 시스템 실행 스크립트

echo "🚀 고급 보안 모니터링 시스템을 시작합니다..."

# 환경 변수 설정
export BRUTE_FORCE_THRESHOLD=5
export DDOS_THRESHOLD=100
export SUSPICIOUS_USER_THRESHOLD=10
export TIME_WINDOW_MINUTES=10
export SLIDE_INTERVAL_MINUTES=5
export ALERT_COOLDOWN_MINUTES=30

# Elasticsearch 설정
export ES_HOST=elasticsearch
export ES_PORT=9200
export ES_USERNAME=elastic
export ES_PASSWORD=changeme

# Kafka 설정
export KAFKA_BOOTSTRAP_SERVERS=kafka1:29092,kafka2:29093,kafka3:29094

# 알림 설정
export CRITICAL_COOLDOWN_MINUTES=5
export HIGH_COOLDOWN_MINUTES=15
export MEDIUM_COOLDOWN_MINUTES=30

echo "📊 설정값:"
echo "  - Brute Force 임계값: ${BRUTE_FORCE_THRESHOLD}회"
echo "  - DDoS 임계값: ${DDOS_THRESHOLD}회"
echo "  - 의심스러운 사용자 임계값: ${SUSPICIOUS_USER_THRESHOLD}회"
echo "  - 시간 윈도우: ${TIME_WINDOW_MINUTES}분"
echo "  - 슬라이드 간격: ${SLIDE_INTERVAL_MINUTES}분"

# Spark 애플리케이션 실행
echo "🔍 Spark Streaming 애플리케이션을 시작합니다..."

spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.elasticsearch:elasticsearch-hadoop:8.8.0 \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.sql.adaptive.skewJoin.enabled=true" \
    --conf "spark.sql.adaptive.localShuffleReader.enabled=true" \
    --conf "spark.sql.shuffle.partitions=2" \
    --conf "spark.default.parallelism=2" \
    --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=32MB" \
    --conf "spark.sql.adaptive.coalescePartitions.minPartitionNum=1" \
    --conf "spark.sql.adaptive.coalescePartitions.initialPartitionNum=1" \
    --conf "spark.sql.streaming.statefulOperator.checkCorrectness.enabled=false" \
    --conf "spark.sql.streaming.unsupportedOperationCheck.enabled=false" \
    --conf "spark.sql.adaptive.autoBroadcastJoinThreshold=10MB" \
    --conf "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB" \
    --conf "spark.sql.adaptive.skewJoin.skewedPartitionFactor=5" \
    --conf "spark.sql.adaptive.coalescePartitions.parallelismFirst=false" \
    --conf "spark.sql.adaptive.coalescePartitions.minPartitionSize=1MB" \
    --conf "spark.executor.memory=1g" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.executor.cores=1" \
    --conf "spark.driver.cores=1" \
    --conf "spark.sql.streaming.checkpointLocation=/tmp/checkpoint/advanced-security-monitor" \
    /opt/bitnami/spark/work/advanced_security_monitor.py

echo "✅ 고급 보안 모니터링 시스템이 종료되었습니다." 