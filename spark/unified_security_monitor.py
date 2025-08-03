#!/usr/bin/env python3
"""
통합 보안 모니터링 시스템
- Elasticsearch 저장 + 보안 알림을 하나의 애플리케이션에서 처리
- 하나의 Kafka 토픽에서 읽어서 두 가지 작업을 병렬로 수행
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import os
import logging
import dotenv
import threading
import time
from datetime import datetime, timedelta

# 환경 변수 로드
dotenv.load_dotenv('/opt/bitnami/spark/work/configs/airflow_config.env')

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UnifiedSecurityMonitor:
    def __init__(self):
        """통합 보안 모니터링 시스템 초기화"""
        self.spark = None
        self.schema = None
        
        # 설정값
        self.brute_force_threshold = int(os.getenv("BRUTE_FORCE_THRESHOLD", "5"))
        self.ddos_threshold = int(os.getenv("DDOS_THRESHOLD", "100"))
        self.suspicious_user_threshold = int(os.getenv("SUSPICIOUS_USER_THRESHOLD", "10"))
        self.time_window_minutes = int(os.getenv("TIME_WINDOW_MINUTES", "10"))
        self.slide_interval_minutes = int(os.getenv("SLIDE_INTERVAL_MINUTES", "5"))
        
    def create_spark_session(self):
        """Spark 세션 생성"""
        logger.info("통합 보안 모니터링을 위한 Spark 세션을 생성합니다...")
        
        return SparkSession.builder \
            .appName("UnifiedSecurityMonitor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "32MB") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
            .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .config("spark.sql.streaming.unsupportedOperationCheck.enabled", "false") \
            .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB") \
            .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
            .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB") \
            .getOrCreate()

    def create_schema(self):
        """로그 데이터 스키마 정의"""
        logger.info("로그 데이터 스키마를 정의합니다...")
        return StructType([
            StructField("@timestamp", StringType(), True),
            StructField("host_name", StringType(), True),
            StructField("row_hash", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("event_time_kst_iso", StringType(), True),
            StructField("source_path", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("message", StringType(), True),
            StructField("log_type", StringType(), True),
            StructField("log_level", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("user", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("port", StringType(), True),
        ])

    def process_kafka_stream(self):
        """Kafka에서 스트림 데이터 읽기 및 처리"""
        logger.info("Kafka 스트림 처리를 시작합니다...")
        
        # Kafka에서 데이터 읽기
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:29092,kafka2:29093,kafka3:29094") \
            .option("subscribe", "ssh-log") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .option("kafka.max.poll.records", "500") \
            .option("kafka.fetch.min.bytes", "1") \
            .option("kafka.fetch.max.wait.ms", "500") \
            .load()
        
        logger.info("Kafka에서 데이터를 읽기 시작했습니다.")
        
        # JSON 파싱
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")
        
        # 타임스탬프 변환 및 데이터 정제
        processed_df = parsed_df.withColumn(
            "@timestamp", 
            to_timestamp(col("@timestamp"))
        ).withColumn(
            "event_time", 
            to_timestamp(col("event_time"))
        ).withColumn(
            "event_time_kst_iso", 
            to_timestamp(col("event_time_kst_iso"))
        ).withColumn(
            "log_level", 
            when(col("log_level").isNull(), "UNKNOWN").otherwise(col("log_level"))
        ).withColumn(
            "error_type",
            when(col("error_type").isNull(), "unknown").otherwise(col("error_type"))
        ).withColumn(
            "user",
            when(col("user").isNull(), "unknown").otherwise(col("user"))
        ).withColumn(
            "ip",
            when(col("ip").isNull(), "unknown").otherwise(col("ip"))
        )
        
        return processed_df

    def detect_brute_force_attacks(self, df):
        """Brute Force 공격 탐지"""
        logger.info("Brute Force 공격 탐지를 시작합니다...")
        
        # 실패한 로그인만 필터링
        failed_logins = df.filter(
            col("error_type").isin(["login failed", "invalid user", "authentication failure"])
        ).filter(col("ip").isNotNull() & (col("ip") != "unknown"))
        
        # 슬라이딩 윈도우로 IP별 집계
        windowed_counts = failed_logins \
            .withWatermark("event_time", f"{self.time_window_minutes + 5} minutes") \
            .groupBy(
                window("event_time", f"{self.time_window_minutes} minutes", f"{self.slide_interval_minutes} minutes"),
                "ip"
            ) \
            .agg(
                count("*").alias("failed_attempts"),
                collect_list("user").alias("attempted_users"),
                collect_list("message").alias("log_messages"),
                min("event_time").alias("first_attempt"),
                max("event_time").alias("last_attempt")
            )
        
        # 임계값 초과 시 알림 생성
        alerts = windowed_counts.filter(col("failed_attempts") >= self.brute_force_threshold) \
            .withColumn("alert_type", lit("brute_force_detected")) \
            .withColumn("source_ip", col("ip")) \
            .withColumn("detection_time", current_timestamp()) \
            .withColumn("time_window_minutes", lit(self.time_window_minutes)) \
            .withColumn("severity", 
                when(col("failed_attempts") >= self.brute_force_threshold * 3, "critical")
                .when(col("failed_attempts") >= self.brute_force_threshold * 2, "high")
                .otherwise("medium")
            ) \
            .withColumn("threshold", lit(self.brute_force_threshold)) \
            .withColumn("unique_users_attempted", size(array_distinct(col("attempted_users")))) \
            .withColumn("attack_duration_minutes", 
                unix_timestamp(col("last_attempt")) - unix_timestamp(col("first_attempt")) / 60
            ) \
            .select(
                "alert_type", "source_ip", "detection_time", "failed_attempts", 
                "time_window_minutes", "severity", "threshold", "log_messages",
                "unique_users_attempted", "attack_duration_minutes", "attempted_users"
            )
        
        logger.info(f"Brute Force 탐지 완료: 임계값 {self.brute_force_threshold}회 초과 시 알림 생성")
        return alerts

    def detect_ddos_attacks(self, df):
        """DDoS 공격 탐지"""
        logger.info("DDoS 공격 탐지를 시작합니다...")
        
        # 모든 로그인 시도 (성공/실패 모두)
        all_logins = df.filter(
            col("ip").isNotNull() & (col("ip") != "unknown")
        )
        
        # 슬라이딩 윈도우로 IP별 집계
        windowed_counts = all_logins \
            .withWatermark("event_time", f"{self.time_window_minutes + 5} minutes") \
            .groupBy(
                window("event_time", f"{self.time_window_minutes} minutes", f"{self.slide_interval_minutes} minutes"),
                "ip"
            ) \
            .agg(
                count("*").alias("total_attempts"),
                sum(when(col("log_level") == "ERROR", 1).otherwise(0)).alias("failed_attempts"),
                sum(when(col("log_level") == "INFO", 1).otherwise(0)).alias("successful_attempts"),
                collect_list("user").alias("attempted_users"),
                min("event_time").alias("first_attempt"),
                max("event_time").alias("last_attempt")
            )
        
        # DDoS 탐지 조건: 총 시도 횟수가 임계값을 초과하고 실패율이 높은 경우
        ddos_alerts = windowed_counts.filter(
            (col("total_attempts") >= self.ddos_threshold) &
            (col("failed_attempts") / col("total_attempts") >= 0.8)
        ) \
        .withColumn("alert_type", lit("ddos_attack_detected")) \
        .withColumn("source_ip", col("ip")) \
        .withColumn("detection_time", current_timestamp()) \
        .withColumn("time_window_minutes", lit(self.time_window_minutes)) \
        .withColumn("severity", 
            when(col("total_attempts") >= self.ddos_threshold * 3, "critical")
            .when(col("total_attempts") >= self.ddos_threshold * 2, "high")
            .otherwise("medium")
        ) \
        .withColumn("threshold", lit(self.ddos_threshold)) \
        .withColumn("failure_rate", col("failed_attempts") / col("total_attempts")) \
        .withColumn("unique_users_attempted", size(array_distinct(col("attempted_users")))) \
        .withColumn("attack_duration_minutes", 
            unix_timestamp(col("last_attempt")) - unix_timestamp(col("first_attempt")) / 60
        ) \
        .select(
            "alert_type", "source_ip", "detection_time", "total_attempts", 
            "failed_attempts", "successful_attempts", "failure_rate",
            "time_window_minutes", "severity", "threshold", 
            "unique_users_attempted", "attack_duration_minutes", "attempted_users"
        )
        
        logger.info(f"DDoS 탐지 완료: 임계값 {self.ddos_threshold}회 초과 시 알림 생성")
        return ddos_alerts

    def save_to_elasticsearch(self, df, index_name):
        """로그를 Elasticsearch에 저장"""
        logger.info(f"로그를 Elasticsearch 인덱스 '{index_name}'에 저장합니다...")
        
        # Elasticsearch 설정
        es_write_options = {
            "es.nodes": os.getenv('ES_HOST', 'elasticsearch'),
            "es.port": os.getenv('ES_PORT', '9200'),
            "es.net.http.auth.user": os.getenv('ES_USERNAME', 'elastic'),
            "es.net.http.auth.pass": os.getenv('ES_PASSWORD', 'changeme'),
            "es.nodes.wan.only": "true",
            "es.index.auto.create": "true",
            "es.mapping.id": "row_hash"
        }
        
        return df.writeStream \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_options) \
            .option("checkpointLocation", f"/tmp/checkpoint/{index_name}") \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .start()

    def save_alerts_to_kafka(self, alerts_df, topic_name):
        """알림을 Kafka로 전송"""
        logger.info(f"보안 알림을 Kafka 토픽 '{topic_name}'으로 전송합니다...")
        
        # JSON 형태로 변환하여 Kafka로 전송
        alerts_json = alerts_df.select(to_json(struct("*")).alias("value"))
        
        return alerts_json.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:29092,kafka2:29093,kafka3:29094") \
            .option("topic", topic_name) \
            .option("checkpointLocation", f"/tmp/checkpoint/{topic_name}") \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .start()

    def run_query(self, query, query_name):
        """개별 쿼리를 실행하는 함수"""
        try:
            logger.info(f"{query_name} 쿼리를 시작합니다...")
            query.awaitTermination()
            logger.info(f"{query_name} 쿼리가 정상적으로 종료되었습니다.")
        except Exception as e:
            logger.error(f"{query_name} 쿼리에서 오류 발생: {str(e)}")

    def start_monitoring(self):
        """모니터링 시작"""
        try:
            logger.info("통합 보안 모니터링 파이프라인을 시작합니다...")
            
            # Spark 세션 생성
            self.spark = self.create_spark_session()
            logger.info("Spark 세션이 성공적으로 생성되었습니다.")
            
            # 스키마 생성
            self.schema = self.create_schema()
            
            # Kafka 스트림 처리 (공통 데이터)
            processed_df = self.process_kafka_stream()
            
            # 브랜치 1: Elasticsearch에 로그 저장
            logger.info("Elasticsearch 저장 쿼리를 준비합니다...")
            es_query = self.save_to_elasticsearch(processed_df, "ssh-log")
            
            # 브랜치 2: Brute Force 공격 탐지
            logger.info("Brute Force 탐지 쿼리를 준비합니다...")
            brute_force_alerts = self.detect_brute_force_attacks(processed_df)
            brute_force_query = self.save_alerts_to_kafka(brute_force_alerts, "security-alerts")
            
            # 브랜치 3: DDoS 공격 탐지
            logger.info("DDoS 탐지 쿼리를 준비합니다...")
            ddos_alerts = self.detect_ddos_attacks(processed_df)
            ddos_query = self.save_alerts_to_kafka(ddos_alerts, "security-alerts")
            
            # 모든 쿼리를 별도 스레드에서 실행
            threads = [
                threading.Thread(target=self.run_query, args=(es_query, "Elasticsearch 저장")),
                threading.Thread(target=self.run_query, args=(brute_force_query, "Brute Force 탐지")),
                threading.Thread(target=self.run_query, args=(ddos_query, "DDoS 탐지"))
            ]
            
            # 스레드 시작
            for thread in threads:
                thread.start()
            
            logger.info("모든 스트리밍 쿼리가 병렬로 시작되었습니다.")
            logger.info("- Elasticsearch 저장: ssh-log 인덱스")
            logger.info("- 보안 알림: security-alerts 토픽")
            logger.info("  * Brute Force 공격 탐지")
            logger.info("  * DDoS 공격 탐지")
            
            # 메인 스레드에서 대기
            for thread in threads:
                thread.join()
                
        except Exception as e:
            logger.error(f"통합 보안 모니터링에서 오류 발생: {str(e)}")
            raise

def main():
    """메인 실행 함수"""
    monitor = UnifiedSecurityMonitor()
    monitor.start_monitoring()

if __name__ == "__main__":
    main() 