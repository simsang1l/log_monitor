from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Spark 세션 생성"""
    logger.info("Spark 세션을 생성합니다...")
    return SparkSession.builder \
        .appName("KafkaToElasticsearch") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6," +
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def create_schema():
    """로그 데이터 스키마 정의"""
    logger.info("로그 데이터 스키마를 정의합니다...")
    return StructType([
        StructField("@timestamp", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("message", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("log_type", StringType(), True),
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
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("Kafka에서 데이터를 읽기 시작했습니다.")
    
    # JSON 파싱
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # 타임스탬프 변환 및 데이터 정제
    processed_df = parsed_df.withColumn(
        "timestamp", 
        to_timestamp(col("@timestamp"))
    ).withColumn(
        "date", 
        date_format(col("timestamp"), "yyyy-MM-dd")
    ).withColumn(
        "hour", 
        hour(col("timestamp"))
    ).withColumn(
        "log_level", 
        when(lower(col("message")).contains("failed"), "ERROR")
        .when(lower(col("message")).contains("invalid"), "WARN")
        .when(lower(col("message")).contains("accepted"), "INFO")
        .when(lower(col("message")).contains("connection closed"), "INFO")
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
    .withColumn("event_time", to_timestamp(col("event_time")))
    
    # null 값 처리
    processed_df = processed_df.na.fill({
        "message": "",
        "source_path": "",
        "source_file": "",
        "log_type": "ssh",
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
        .option("es.mapping.id", "timestamp") \
        .option("es.nodes.wan.only", "true") \
        .option("es.net.http.auth.user", "elastic") \
        .option("es.net.http.auth.pass", "elastic123") \
        .option("es.index.auto.create", "true") \
        .option("es.mapping.date.rich", "true") \
        .option("es.net.ssl", "false") \
        .option("es.batch.size.entries", "10") \
        .option("checkpointLocation", f"/tmp/checkpoint/{index_name}") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

def main():
    """메인 실행 함수"""
    try:
        logger.info("Kafka to Elasticsearch 파이프라인을 시작합니다...")
        
        # Spark 세션 생성
        spark = create_spark_session()
        logger.info("Spark 세션이 성공적으로 생성되었습니다.")
        
        # 스키마 생성
        schema = create_schema()
        
        # Kafka 스트림 처리
        processed_df = process_kafka_stream(spark, schema)
        
        # Elasticsearch에 저장
        query = save_to_elasticsearch(processed_df, "ssh-log")
        
        logger.info("스트리밍 쿼리가 시작되었습니다. 종료하려면 Ctrl+C를 누르세요.")
        
        # 스트리밍 쿼리 시작
        query.awaitTermination()
        
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