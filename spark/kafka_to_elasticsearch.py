from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
import logging
import dotenv
import smtplib
from email.mime.text import MIMEText
import requests
from requests.auth import HTTPBasicAuth

dotenv.load_dotenv('/opt/bitnami/spark/work/configs/airflow_config.env')

ES_HOST = os.getenv('ES_HOST')
ES_PORT = os.getenv('ES_PORT')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')

# ─────────────────────────────────────────────
#  ES 인덱스 존재 확인 및 매핑 자동 생성
# ─────────────────────────────────────────────

def ensure_es_index(index_name: str):
    """@timestamp, event_time 등을 date 타입으로 명시한 인덱스를 생성한다."""
    es_url = f"http://{ES_HOST}:{ES_PORT}"
    auth = HTTPBasicAuth(ES_USERNAME, ES_PASSWORD) if ES_USERNAME else None
    try:
        head = requests.head(f"{es_url}/{index_name}", auth=auth, timeout=5)
        if head.status_code == 404:
            logger.info("Elasticsearch 인덱스가 존재하지 않아 생성합니다 → %s", index_name)
            mapping = {
                "mappings": {
                    "properties": {
                        "@timestamp":            {"type": "date"},
                        "event_time":            {"type": "date"},
                        "event_time_kst_iso":    {"type": "date"},
                        "host_name":             {"type": "keyword"},
                        "row_hash":              {"type": "keyword"},
                        "source_path":           {"type": "keyword"},
                        "source_file":           {"type": "keyword"},
                        "message":               {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                        "log_type":              {"type": "keyword"},
                        "log_level":             {"type": "keyword"},
                        "error_type":            {"type": "keyword"},
                        "user":                  {"type": "keyword"},
                        "ip":                    {"type": "ip"},
                        "port":                  {"type": "integer"}
                    }
                }
            }
            resp = requests.put(f"{es_url}/{index_name}", json=mapping, auth=auth, timeout=10)
            resp.raise_for_status()
            logger.info("Elasticsearch 인덱스 %s 가 생성되었습니다 (status=%s)", index_name, resp.status_code)
        else:
            logger.info("Elasticsearch 인덱스 %s 가 이미 존재합니다 (status=%s)", index_name, head.status_code)
    except Exception as exc:
        logger.error("Elasticsearch 인덱스 확인/생성 중 오류: %s", exc)
        raise

# 이메일·탐지 설정 (환경변수)
MAIL_HOST = os.getenv("AIRFLOW__SMTP__SMTP_HOST")
MAIL_PORT = int(os.getenv("MAIL_PORT", "587"))
MAIL_USER = os.getenv("AIRFLOW__SMTP__SMTP_USER")
MAIL_PASS = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD")
MAIL_TO   = os.getenv("REPORT_RECIPIENTS")

THRESHOLD = int(os.getenv("BF_THRESHOLD", "30"))    # 실패 횟수
WIN_MIN   = int(os.getenv("BF_WINDOW_MIN", "10"))  # 윈도우 크기(분)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Spark 세션 생성"""
    logger.info("Spark 세션을 생성합니다...")
    return SparkSession.builder \
        .appName("KafkaToElasticsearch") \
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
        .config("spark.eventLog.enabled", "false") \
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
        .option("maxOffsetsPerTrigger", "10000") \
        .option("kafka.max.poll.records", "5000") \
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
    .withColumn("ip",   when(trim(col("ip")) == "", None).otherwise(col("ip"))) \
    .withColumn("port", when(trim(col("port")) == "", None).otherwise(col("port").cast("integer"))) \
    .select(
        "@timestamp", "host_name", "row_hash", "event_time", "event_time_kst_iso", "source_path", "source_file", "message", "log_type", "log_level", "error_type", "user", "ip", "port"
    )
    
    # null 값 처리
    processed_df = processed_df.na.fill({
        "message": "",
        "log_level": "INFO"
    })

    processed_df.printSchema()
    
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
        .trigger(processingTime="5 seconds") \
        .start()

# ─────────────────────────────────────────────
# ❶  브루트 포스 탐지 DataFrame 생성
def detect_brute_force(df):
    failed = (
        df.filter(col("error_type").isin(["login failed", "invalid user"]))
          .filter(col("ip").isNotNull())
    )
    return (
        failed.withWatermark("event_time", f"{WIN_MIN} minutes")
              .groupBy(window("event_time", f"{WIN_MIN} minutes"), "ip")
              .agg(count("*").alias("fail_cnt"))
              .filter(col("fail_cnt") >= THRESHOLD)
              .select("ip", "fail_cnt", current_timestamp().alias("detected_at"))
    )
# ─────────────────────────────────────────────
# ❷  foreachBatch → 이메일 발송
def email_alert(batch_df, _):
    rows = batch_df.collect()
    if not rows:
        print("[ALERT] 탐지된 행이 없어 메일을 보내지 않습니다.")
        return
    print(f"[ALERT] {len(rows)}개 IP 메일 발송 시도")   # ← 추가
    for r in rows:
        print("   >", r.asDict())                      # ← 추가
    body = "\n".join(
        f"IP: {r['ip']}  실패:{r['fail_cnt']}회  탐지:{r['detected_at']}"
        for r in rows
    )
    msg = MIMEText(body)
    msg["Subject"] = "[SSH Brute-Force 경고]"
    msg["From"], msg["To"] = MAIL_USER, MAIL_TO
    with smtplib.SMTP(MAIL_HOST, MAIL_PORT) as s:
        s.starttls()
        s.login(MAIL_USER, MAIL_PASS)
        s.send_message(msg)
    print(f"[ALERT] ✉ {len(rows)}개 IP 메일 발송")

def main():
    """메인 실행 함수"""
    try:
        logger.info("Kafka to Elasticsearch 파이프라인을 시작합니다...")
        
        # Spark 세션 생성
        spark = create_spark_session()
        logger.info("Spark 세션이 성공적으로 생성되었습니다.")
        
        # ES 인덱스 존재 확인/생성
        ensure_es_index("ssh-log")

        # 스키마 생성
        schema = create_schema()
        
        # Kafka 스트림 처리
        processed_df = process_kafka_stream(spark, schema)
        
        # Elasticsearch에 저장
        es_query = save_to_elasticsearch(processed_df, "ssh-log")
        
        logger.info("스트리밍 쿼리가 시작되었습니다. 종료하려면 Ctrl+C를 누르세요.")
        
        # ⓑ 브루트 포스 탐지 + 알림
        alert_df  = detect_brute_force(processed_df)
        alert_qry = (
            alert_df.writeStream
                    .foreachBatch(email_alert)
                    .outputMode("update")
                    .trigger(processingTime="10 seconds")
                    .option("checkpointLocation", "/tmp/chk/bruteforce")
                    .start()
        )
    
        es_query.awaitTermination()     # ES 저장 쿼리
        alert_qry.awaitTermination()    # 알림 쿼리
        
    except KeyboardInterrupt:
        logger.info("사용자에 의해 애플리케이션이 중단되었습니다.")
    except Exception as e:
        logger.error(f"애플리케이션 실행 중 오류가 발생했습니다: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark 세션이 종료되었습니다.")

if __name__ == "__main__":
    main() 