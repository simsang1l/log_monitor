#!/bin/bash

echo "Creating SSH logs topic..."

docker exec -it kafka1 kafka-topics --delete --topic ssh-log --bootstrap-server kafka1:29092

# SSH 로그 토픽 생성
docker exec -it kafka1 kafka-topics --create \
  --topic ssh-log \
  --bootstrap-server kafka1:29092 \
  --partitions 3 \
  --replication-factor 3

docker compose restart logstash

echo "SSH topic created successfully!"
echo "Listing all topics:"
docker exec -it kafka1 kafka-topics --list --bootstrap-server kafka1:29092 