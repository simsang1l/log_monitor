"""
컬럼명	타입	설명
metric_date	DATE	‘YYYY-MM-DD’
total_logs	LONG	하루 전체 로그 수
error_count	LONG	log_level='ERROR' 건수
unique_ips	LONG	서로 다른 IP 개수
peak_hour_logs	LONG	24개 시간대 중 최대 건수
top_ip_list	NESTED(JSON)	[{"ip":"1.2.3.4","cnt":123}, … ] 상위 3
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import argparse
import logging
import json
import dotenv
import os

dotenv.load_dotenv('/opt/airflow/configs/airflow_config.env')
ES_HOST = os.getenv('ES_HOST')
ES_PORT = os.getenv('ES_PORT')
ES_USERNAME = os.getenv('ES_USERNAME')
ES_PASSWORD = os.getenv('ES_PASSWORD')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    return (
        SparkSession.builder.appName("MetricsAggregator")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def load_source_dataframe(spark):
    return (
        spark.read.format("org.elasticsearch.spark.sql")
        .option("es.nodes", os.getenv("ES_HOST", "es"))
        .option("es.port", os.getenv("ES_PORT", "9200"))
        .option("es.resource", "ssh-log")
        .option("es.nodes.wan.only", "true")
        .option("es.net.http.auth.user", os.getenv("ES_USERNAME"))
        .option("es.net.http.auth.pass", os.getenv("ES_PASSWORD"))
        .option("es.net.ssl", "false")
        .load()
    )


def aggregate_metrics(df, metric_date: str):
    # epoch(ms) → timestamp 로 변환
    df = df.withColumn(
        "event_time",
        to_timestamp(from_unixtime(col("event_time") / 1000)),
    )

    start = f"{metric_date} 00:00:00"
    end = f"{metric_date} 23:59:59"

    daily = df.filter((col("event_time") >= start) & (col("event_time") <= end))

    total_logs = daily.count()
    error_count = daily.filter(col("log_level") == "ERROR").count()
    unique_ips = daily.select("ip").distinct().count()

    by_hour = (
        daily.withColumn("h", hour("event_time"))
        .groupBy("h")
        .count()
        .orderBy(col("count").desc())
    )
    if by_hour.count() >= 0:
        peak_row = by_hour.first()
        peak_hour = int(peak_row["h"])
        peak_hour_logs = int(peak_row["count"])
    else:
        peak_hour, peak_hour_logs = None, 0

    # ───────────────── 브루트포스 탐지 ─────────────────
    THRESHOLD = 10  # 실패 10회 이상이면 의심
    failed_df = daily.filter(col("error_type") == "ssh")
    brute_df = (
        failed_df.groupBy("ip")
        .count()
        .filter(col("count") >= THRESHOLD)
        .orderBy(col("count").desc())
    )

    brute_force_ips = brute_df.count()
    top_brute_json = [
        {"ip": r["ip"], "cnt": r["count"]} for r in brute_df.limit(3).collect()
    ]

    top_ip_list = (
        daily.groupBy("ip")
        .count()
        .orderBy(col("count").desc())
        .limit(3)
        .collect()
    )
    top_ip_json = [
        {"ip": row["ip"], "cnt": row["count"]} for row in top_ip_list if row["ip"]
    ]

    if not top_ip_json:
        top_ip_json = [{"ip": "N/A", "cnt": 0}]
    if not top_brute_json:
        top_brute_json = [{"ip": "N/A", "cnt": 0}]

    return {
        "metric_date": metric_date,
        "total_logs": total_logs,
        "error_count": error_count,
        "unique_ips": unique_ips,
        "peak_hour": peak_hour,
        "peak_hour_logs": peak_hour_logs,
        "top_ip_list": top_ip_json,
        "brute_force_ips": brute_force_ips,
        "top_brute_list": top_brute_json,
        "created_at": datetime.utcnow().isoformat(),
    }


def save_metrics(spark, metrics: dict):
    spark.createDataFrame([metrics]).write.format("org.elasticsearch.spark.sql").option(
        "es.nodes", os.getenv("ES_HOST", "es")
    ).option("es.port", os.getenv("ES_PORT", "9200")).option("es.resource", "metrics-daily").option(
        "es.nodes.wan.only", "true"
    ).option(
        "es.net.http.auth.user", os.getenv("ES_USERNAME", "elastic")
    ).option(
        "es.net.http.auth.pass", os.getenv("ES_PASSWORD", "elastic123")
    ).option(
        "es.index.auto.create", "true"
    ).option(
        "es.net.ssl", "false"
    ).mode("append").save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="집계 기준일 YYYY-MM-DD", required=False)
    args = parser.parse_args()

    metric_date = args.date or datetime.utcnow().strftime("%Y-%m-%d")
    logger.info(f"[MetricsAggregator] metric_date={metric_date}")

    spark = create_spark_session()
    try:
        src_df = load_source_dataframe(spark)
        metrics = aggregate_metrics(src_df, metric_date)
        logger.info(f"metrics: {json.dumps(metrics, ensure_ascii=False)}")
        save_metrics(spark, metrics)
        logger.info("저장 완료")
    finally:
        spark.stop()


if __name__ == "__main__":
    main() 