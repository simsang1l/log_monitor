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
- 다중 서버 SSH 로그를 중앙 집중식으로 모니터링
- 실시간 이상 징후(로그인 실패 폭주 등) 탐지 후 이메일 알림
- Kibana 대시보드로 검색·시각화, Airflow 일일 리포트 자동 발송

## 시스템 아키텍처
![시스템 아키텍처](./architecture.png)

## 데이터셋
- **OpenSSH** 서버 로그 (형식: `/var/log/auth.log`)
- 샘플 데이터: [loghub/OpenSSH](https://github.com/logpai/loghub)
- 추후 사내 서버 로그로 확장 예정
```bash
Dec 10 06:55:46 LabSZ sshd[24200]: reverse mapping checking getaddrinfo for ns.marryaldkfaczcz.com [173.234.31.186] failed - POSSIBLE BREAK-IN ATTEMPT!
Dec 10 06:55:46 LabSZ sshd[24200]: Invalid user webmaster from 173.234.31.186
Dec 10 06:55:46 LabSZ sshd[24200]: input_userauth_request: invalid user webmaster [preauth]
```

## 데이터 수집
| 단계 | 도구 | 설명 |
| --- | --- | --- |
| 1 | Logstash File Input | `/data/ssh-*.log` 파일 read |
| 2 | Grok & Ruby Filter | 타임스탬프 변환, 필드 추출, fingerprint(`row_hash`) 생성 |
| 3 | Kafka Output | `ssh-log` 토픽(JSON)으로 전송 |

## Kafka 구성
| 항목 | 설정값 | 설명 |
| --- | --- | --- |
| **토픽** | `ssh-log` | SSH 로그 데이터 저장용 메인 토픽 |
| **파티션** | 3개 | 병렬 처리 및 확장성을 위한 파티션 수 |
| **복제 계수** | 3 | 데이터 안정성을 위한 복제본 수 |
| **메시지 키** | `row_hash` | 중복 방지를 위한 고유 식별자, `source_path + event_time + message` 조합 |
| **데이터 형식** | JSON | Logstash에서 전송하는 구조화된 로그 데이터 |

## 데이터 처리 (Spark)

### 클러스터 구성
| 컴포넌트 | 개수 | 리소스 | 역할 |
| --- | --- | --- | --- |
| **Master** | 1개 | - | 클러스터 관리, 작업 스케줄링 |
| **Worker** | 1개 | 1GB RAM, 2 Cores | 실제 작업 실행, Executor 호스팅 |
| **Executor** | 1개 | 1GB RAM, 2 Cores | Spark 작업 실행 엔진 |

### 처리 모드
| 모드 | Trigger | 엔진 | 기능 |
| --- | --- | --- | --- |
| 실시간 | 5초 마이크로 배치 | Spark Structured Streaming | JSON 파싱, 파생 필드(`user`, `ip`), 이상탐지(로그인 실패 N회) |
| 배치 | 일간 Airflow DAG | Spark | 지표 집계 리포트 생성 |

### 리소스 최적화
| 설정 | 값 | 목적 |
| --- | --- | --- |
| **Worker Memory** | 1GB | 워커당 사용 가능 메모리 |
| **Worker Cores** | 2 | 워커당 CPU 코어 수 |
| **Executor Memory** | 1GB | 실행자당 메모리 할당 |
| **Executor Cores** | 2 | 실행자당 CPU 코어 수 |
| **Shuffle Partitions** | 1 | 단일 노드 최적화 |
| **Max Offsets Per Trigger** | 10000 | 배치 크기 제한 |

### 확장성
- **수평 확장**: Worker 노드 추가로 처리량 선형 증가
- **메모리 확장**: Worker Memory 증가로 대용량 데이터 처리 가능
- **코어 확장**: Worker Cores 증가로 병렬 처리 성능 향상

## 데이터 저장 (Elasticsearch)
| 인덱스 | 기능 |
| --- | --- |
| `ssh-log` | 원본+파싱 로그 저장 |
| `metrics-daily` | 일간 집계 지표 |

**ssh-log**
| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `@timestamp` | date | 로그 수집 시간 |
| `error_type` | keyword | 오류 유형 |
| `event_time` | date | 로그 발생 시간 |
| `host_name` | keyword | 서버 호스트명 |
| `user` | keyword | 접근 사용자 |
| `ip` | ip | 접근 IP |
| `port` | integer | 접근 포트 |
| `log_level` | keyword | 로그 레벨 (INFO, ERROR, WARN) |
| `log_type` | keyword | 오류 유형 (ssh, ...) |
| `message` | text | 로그 메시지 |
| `row_hash` | keyword | 로그 해시 값 (message + raw경로 + 로그 발생 시간) |
| `source_file` | keyword | 로그 파일명 |
| `source_path` | keyword | 로그 파일 경로 |

**metrics-daily**
| 필드 | 타입 | 설명 |
| --- | --- | --- |
| `metric_date` | date | 집계 기준일 (YYYY-MM-DD) |
| `total_logs` | long | 하루 전체 로그 수 |
| `error_count` | long | log_level='ERROR' 건수 |
| `unique_ips` | long | 서로 다른 IP 개수 |
| `peak_hour` | integer | 24개 시간대 중 최대 건수 시간 |
| `peak_hour_logs` | long | 피크 시간대 로그 건수 |
| `top_ip_list` | nested | [{"ip":"1.2.3.4","cnt":123}, ...] 상위 3개 IP |
| `brute_force_ips` | long | 브루트포스 의심 IP 개수 (실패 10회 이상) |
| `top_brute_list` | nested | [{"ip":"1.2.3.4","cnt":123}, ...] 상위 3개 브루트포스 IP |
| `created_at` | date | 집계 생성 시간 |

## 리포팅 & 알림
- **실시간 알림** (`spark/kafka_to_elasticsearch.py`): Spark `foreachBatch`로 브루트포스 탐지 시 SMTP 발송
- **일간 리포트** (`airflow/dags/daily_ssh_report_dag.py`): 매일 09:00 KST, 일간 리포트 생성 후 이메일 전송

## 장애 처리 및 운영

### 장애 처리 전략
| 컴포넌트 | 장애 유형 | 처리 방법 | 복구 시간 |
| --- | --- | --- | --- |
| **Kafka** | 브로커 장애 | `replication.factor=3`, `min.insync.replicas=2` | 30초 내 자동 복구 |
| **Spark** | 애플리케이션 장애 | 체크포인트 기반 재시작, `es.mapping.id=row_hash` | 1-2분 |
| **Logstash** | 프로세스 장애 | sincedb 기반 오프셋 복구 | 즉시 재시작 |
| **Elasticsearch** | 노드 장애 | 단일 노드 구성, 데이터 백업 | 1-3분 |

### Retry 전략
| 서비스 | 재시도 횟수 | 간격 | 백오프 |
| --- | --- | --- | --- |
| **Airflow DAG** | 1회 | 5분 | 고정 |
| **Spark Streaming** | 무한 | 즉시 | 체크포인트 기반 |
| **Email 발송** | 3회 | 10초 | 지수 백오프 |

### 알림 전송
- **실시간 알림**: 브루트포스 탐지 시 즉시 SMTP 발송
- **성공 알림**: 일간 리포트 생성 완료 시 HTML 이메일
- **실패 알림**: DAG 실패 시 상세 오류 정보 포함 이메일
- **수신자**: 환경변수 `REPORT_RECIPIENTS`로 다중 수신자 지원

### 모니터링
- **Healthcheck**: 모든 컨테이너 30초 간격 상태 확인
- **로그 모니터링**: 각 서비스별 로그 수집 및 분석
- **메트릭 수집**: Kafka lag, Elasticsearch 성능 지표

## 성능 및 테스트

### 현재 성능 지표
| 지표 | 값 | 설명 |
| --- | --- | --- |
| **처리 지연시간** | 5초 | Spark 마이크로 배치 간격 |
| **메모리 사용량** | 1GB | Spark 워커 메모리 |
| **체크포인트 크기** | ~100MB | 스트리밍 상태 저장 |

### 개선 방안
| 영역 | 현재 | 개선안 | 예상 효과 |
| --- | --- | --- | --- |
| **처리 성능** | 단일 워커 | 멀티 워커 확장 | 2-3배 처리량 증가 |
| **메모리 최적화** | 1GB 고정 | 동적 메모리 할당 | 리소스 효율성 향상 |
| **장애 복구** | 수동 재시작 | 자동 재시작 스크립트 | 다운타임 50% 감소 |
| **모니터링** | 기본 로그 | Prometheus + Grafana | 실시간 알림 강화 |

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
