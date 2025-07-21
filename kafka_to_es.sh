#!/bin/bash
# 전체 파이프라인을 순차적으로 실행

echo "[1/2] Logstash → Kafka"
./create_ssh_topic.sh

sleep 10

echo "[2/2] Spark → Elasticsearch"
./spark/spark_kafka_ssh_to_es.sh