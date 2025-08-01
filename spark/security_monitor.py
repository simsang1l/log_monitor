from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
import logging
import dotenv
import threading
import time

dotenv.load_dotenv('/opt/bitnami/spark/work/configs/airflow_config.env')

ES_HOST = os.getenv('ES_HOST')
ES_PORT = os.getenv('ES_PORT')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Spark 세션 생성"""
    logger.info("Spark 세션을 생성합니다...")
    return SparkSession.builder \
        .appName("SecurityMonitor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
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

def create_schema():
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

def process_kafka_stream(spark, schema):
    """Kafka에서 스트림 데이터 읽기 및 처리"""
    logger.info("Kafka 스트림 처리를 시작합니다...")
    
    # Kafka에서 데이터 읽기
    df = spark.readStream \
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
        from_json(col("value").cast("string"), schema).alias("data")
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
        when(lower(col("message")).contains("failed"), "ERROR")
        .when(lower(col("message")).contains("invalid"), "WARN")
        .otherwise("INFO")
    ).withColumn(
        "error_type",
        when(lower(col("message")).contains("failed password"), "login failed")
        .when(lower(col("message")).contains("invalid user"), "invalid user")
        .when(lower(col("message")).contains("accepted password"), "login success")
        .when(lower(col("message")).contains("connection closed"), "connection closed")
        .otherwise("etc")
    ).withColumn("user", regexp_extract(col("message"), r"user ([^ ]+)", 1)) \
    .withColumn("ip", regexp_extract(col("message"), r"from ([0-9.]+)", 1)) \
    .withColumn("port", regexp_extract(col("message"), r"port ([0-9]+)", 1)) \
    .select(
        "@timestamp", "host_name", "row_hash", "event_time", "event_time_kst_iso", "source_path", "source_file", "message", "log_type", "log_level", "error_type", "user", "ip", "port"
    )
    
    # null 값 처리
    processed_df = processed_df.na.fill({
        "message": "",
        "log_level": "INFO"
    })
    
    logger.info("데이터 처리가 완료되었습니다.")
    return processed_df

def save_to_elasticsearch(df, index_name):
    """Elasticsearch에 데이터 저장"""
    logger.info(f"Elasticsearch에 데이터를 저장합니다. 인덱스: {index_name}")
    
    return df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "es") \
        .option("es.port", "9200") \
        .option("es.resource", index_name) \
        .option("es.mapping.id", "row_hash") \
        .option("es.nodes.wan.only", "true") \
        .option("es.net.http.auth.user", os.getenv("ES_USERNAME")) \
        .option("es.net.http.auth.pass", os.getenv("ES_PASSWORD")) \
        .option("es.index.auto.create", "true") \
        .option("es.mapping.date.rich", "true") \
        .option("es.net.ssl", "false") \
        .option("checkpointLocation", f"/tmp/checkpoint/{index_name}") \
        .option("es.batch.size.entries", "100") \
        .option("es.batch.size.bytes", "5mb") \
        .option("es.batch.write.refresh", "false") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

def detect_brute_force(df):
    """브루트 포스 공격 탐지"""
    logger.info("브루트 포스 탐지를 시작합니다...")
    
    # 실패한 로그인만 필터링
    failed_logins = df.filter(
        col("error_type").isin(["login failed", "invalid user"])
    ).filter(col("ip").isNotNull())
    
    # 10분 윈도우로 IP별 집계
    windowed_counts = failed_logins \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window("event_time", "10 minutes", "5 minutes"),
            "ip"
        ) \
        .agg(
            count("*").alias("failed_attempts"),
            collect_list("message").alias("log_messages")
        )
    
    # 임계값 초과 시 알림 생성 (기본값: 5회)
    threshold = int(os.getenv("BRUTE_FORCE_THRESHOLD", "5"))
    alerts = windowed_counts.filter(col("failed_attempts") > threshold) \
        .withColumn("alert_type", lit("brute_force_detected")) \
        .withColumn("source_ip", col("ip")) \
        .withColumn("detection_time", current_timestamp()) \
        .withColumn("time_window_minutes", lit(10)) \
        .withColumn("severity", lit("high")) \
        .withColumn("threshold", lit(threshold)) \
        .select("alert_type", "source_ip", "detection_time", "failed_attempts", "time_window_minutes", "severity", "threshold", "log_messages")
    
    logger.info(f"브루트 포스 탐지 완료: 임계값 {threshold}회 초과 시 알림 생성")
    return alerts

def save_alerts_to_kafka(df):
    """알림을 Kafka로 전송"""
    logger.info("보안 알림을 Kafka로 전송합니다...")
    
    # JSON 형태로 변환하여 Kafka로 전송
    alerts_json = df.select(to_json(struct("*")).alias("value"))
    
    return alerts_json.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:29092,kafka2:29093,kafka3:29094") \
        .option("topic", "security-alerts") \
        .option("checkpointLocation", "/tmp/checkpoint/security-alerts") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

def run_query(query, query_name):
    """개별 쿼리를 실행하는 함수"""
    try:
        logger.info(f"{query_name} 쿼리를 시작합니다...")
        query.awaitTermination()
        logger.info(f"{query_name} 쿼리가 정상적으로 종료되었습니다.")
    except Exception as e:
        logger.error(f"{query_name} 쿼리에서 오류 발생: {str(e)}")

def main():
    """메인 실행 함수 - 병렬 처리"""
    try:
        logger.info("보안 모니터링 파이프라인을 시작합니다...")
        
        # Spark 세션 생성
        spark = create_spark_session()
        logger.info("Spark 세션이 성공적으로 생성되었습니다.")
        
        # 스키마 생성
        schema = create_schema()
        
        # Kafka 스트림 처리 (공통 데이터)
        processed_df = process_kafka_stream(spark, schema)
        
        # 브랜치 1: Elasticsearch에 로그 저장
        logger.info("Elasticsearch 저장 쿼리를 준비합니다...")
        es_query = save_to_elasticsearch(processed_df, "ssh-log")
        
        # 브랜치 2: 브루트 포스 탐지 및 알림 전송
        logger.info("브루트 포스 탐지 쿼리를 준비합니다...")
        alerts_df = detect_brute_force(processed_df)
        alert_query = save_alerts_to_kafka(alerts_df)
        
        # 두 쿼리를 별도 스레드에서 실행
        es_thread = threading.Thread(
            target=run_query, 
            args=(es_query, "Elasticsearch 저장")
        )
        alert_thread = threading.Thread(
            target=run_query, 
            args=(alert_query, "보안 알림")
        )
        
        # 스레드 시작
        es_thread.start()
        alert_thread.start()
        
        logger.info("모든 스트리밍 쿼리가 병렬로 시작되었습니다.")
        logger.info("- Elasticsearch 저장: ssh-log 인덱스")
        logger.info("- 보안 알림: security-alerts 토픽")
        logger.info("종료하려면 Ctrl+C를 누르세요.")
        
        # 메인 스레드에서 모니터링
        try:
            while True:
                if not es_thread.is_alive() or not alert_thread.is_alive():
                    logger.warning("하나의 쿼리가 종료되었습니다. 애플리케이션을 종료합니다.")
                    break
                time.sleep(5)
        except KeyboardInterrupt:
            logger.info("사용자에 의해 애플리케이션을 종료합니다.")
            
    except Exception as e:
        logger.error(f"애플리케이션 실행 중 오류가 발생했습니다: {str(e)}")
        raise
    finally:
        # 쿼리 정리
        if 'es_query' in locals() and es_query.isActive:
            logger.info("Elasticsearch 쿼리를 정리합니다...")
            es_query.stop()
        if 'alert_query' in locals() and alert_query.isActive:
            logger.info("보안 알림 쿼리를 정리합니다...")
            alert_query.stop()
        
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark 세션이 종료되었습니다.")

if __name__ == "__main__":
    main() 