# log_monitor
서버ㆍ어플리케이션 로그와 시스템 자원 사용량을 **실시간**으로 수집·가공·저장하여 운영 상태를 시각화하는 데이터 엔지니어링 프로젝트입니다. “운영 중 장애를 조기에 감지하고, 근본 원인을 분석하기 쉬운 로그 플랫폼”을 목표로 합니다.

## 목차
1. [프로젝트 Goal](#프로젝트-goal)
2. [시스템 아키텍처](#시스템-아키텍처)
3. [데이터셋](#데이터셋)
4. [데이터 수집](#데이터-수집)
5. [Kafka 구성](#kafka-구성)
6. [데이터 처리](#데이터-처리)
7. [데이터 저장](#데이터-저장)
8. [리포팅 & 알림](#리포팅--알림)
9. [장애 처리 및 운영](#장애-처리-및-운영)
10. [성능 및 테스트](#성능-및-테스트)
11. [데모](#데모)

---

## 프로젝트 Goal
- 다중 서버 SSH 로그를 **중앙 집중식**으로 모니터링
- 실시간 이상 징후(로그인 실패 폭주 등) 탐지 후 이메일 알림
- Kibana 대시보드로 검색·시각화, Airflow 일일 리포트 자동 발송

## 시스템 아키텍처
```mermaid
graph TD;
  subgraph Collect
    A[Filebeat \n(Log shipper)] --> B[Kafka]
  end
  subgraph Process
    B --> C[Spark Structured Streaming]
  end
  subgraph Storage
    C --> E[(ClickHouse)]
    D --> E
    E --> F[Grafana Dashboard]
  end
```

## 데이터셋
- **OpenSSH** 서버 로그 (형식: `/var/log/auth.log`)
- 샘플 데이터: [loghub/OpenSSH](https://github.com/logpai/loghub)
- 추후 사내 WAS/Nginx 로그로 확장 예정

## 데이터 수집
| 단계 | 도구 | 설명 |
| --- | --- | --- |
| 1 | Logstash File Input | `/data/ssh-*.log` 파일 tail |
| 2 | Grok & Ruby Filter | 타임스탬프 변환, 필드 추출, fingerprint(`row_hash`) 생성 |
| 3 | Kafka Output | `ssh-log` 토픽(JSON)으로 전송 |

## Kafka 구성
- **토픽**: `ssh-log`
- **파티션 키**: `host_name`
- **복제 계수**: 3 (`docker-compose` 기준)
- **중복 방지**: `row_hash`를 message key 로 사용 → idempotent consumer 가능

## 데이터 처리 (Spark)
| 모드 | Trigger | 엔진 | 기능 |
| --- | --- | --- | --- |
| 실시간 | 30초 마이크로 배치 | Spark Structured Streaming | JSON 파싱, 파생 필드(`user`, `ip`), 이상탐지(로그인 실패 N회) |
| 배치 | 일간 Airflow DAG | Spark | 지표 집계·PDF/HTML 리포트 생성 |

**최적화**
- `maxOffsetsPerTrigger=1000`, `shuffle.partitions=1` 로 단일 노드에서도 3초 내 처리
- 체크포인트: `/tmp/checkpoint/*` 를 Docker 볼륨으로 분리해 장애 후 재시작 시 복구

## 데이터 저장 (Elasticsearch)
| 인덱스 | 샤드 | 기능 |
| --- | --- | --- |
| `ssh-log-YYYY.MM.DD` | 1 Primary / 1 Replica | 원본+파싱 로그 저장 |
| `ssh-bruteforce` | 1 Primary / 1 Replica | 이상탐지 결과 |

예시 매핑
```json
{
  "mappings": {
    "properties": {
      "@timestamp": {"type":"date"},
      "host_name":  {"type":"keyword"},
      "user":       {"type":"keyword"},
      "ip":         {"type":"ip"},
      "log_level":  {"type":"keyword"},
      "error_type": {"type":"keyword"},
      "message":    {"type":"text"}
    }
  }
}
```

## 리포팅 & 알림
- **Email Notifier** (`email-notifier/advanced_alert_notifier.py`): Spark `foreachBatch` 결과 중 경고 발생 시 SMTP 발송
- **Airflow DAGs** (`airflow/dags/daily_ssh_report_dag.py`): 매일 09:00 KST, 일간 리포트 생성 후 이메일 전송

## 장애 처리 및 운영
- Kafka `replication.factor=3` / `min.insync.replicas=2` → 브로커 1대 장애 시에도 데이터 손실 없음
- Spark 체크포인트 + `es.mapping.id=row_hash` 로 **exactly-once** 보장
- Logstash sincedb로 파일 오프셋 저장 → 재시작 시 중단 지점부터 수집
- 컨테이너 Healthcheck + Prometheus Exporter 로 모니터링 & Alertmanager 연동

## 성능 및 테스트
- **Locust** 부하: 초당 5k 이벤트 → Kafka lag 0 / Elasticsearch TPS 3.2k
- End-to-end 지연시간(P95): 3.5 s (수집→Kibana 색인)
- Load test 스크립트: `reports/load_test_locustfile.py`

## 데모
```bash
# 1) 환경 기동
$ docker-compose up -d

# 2) 샘플 로그 삽입
$ ./create_ssh_topic.sh           # 토픽 생성
$ docker exec -it logstash logstash -f /pipeline/logstash.conf

# 3) Spark 스트리밍 시작
$ ./spark/spark_kafka_ssh_to_es.sh

# 4) Kibana ▶ http://localhost:5601  대시보드 확인
```

---
프로젝트 구조, 실행 방법, 기여 지침 등은 [CONTRIBUTING.md](CONTRIBUTING.md) 참고.

