#!/bin/bash

# SSH 로그 토픽 삭제
docker exec -it kafka1 kafka-topics --delete \
    --topic ssh-log \
    --bootstrap-server kafka1:29092 

# logstash 컨테이너 재시작
docker compose down logstash -v

docker compose up logstash -d

# SSH 로그 토픽 생성 스크립트

echo "🔧 Kafka 토픽을 생성합니다..."

# 기존 토픽 생성 (Elasticsearch 저장용)
echo "📊 ssh-log 토픽 생성 (Elasticsearch 저장용)..."
docker exec -it kafka1 kafka-topics --create \
    --topic ssh-log \
    --bootstrap-server kafka1:29092 \
    --partitions 3 \
    --replication-factor 3

echo "✅ 모든 토픽이 생성되었습니다."

# 토픽 목록 확인
echo "📋 생성된 토픽 목록:"
docker exec -it kafka1 kafka-topics --list --bootstrap-server kafka1:29092 