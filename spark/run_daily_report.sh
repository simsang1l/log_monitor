#!/bin/bash

# --- 파라미터 체크 -----------------------------------------------------------
if [ -z "$1" ]; then
  echo "Usage: $0 <YYYY-MM-DD>" >&2
  exit 1
fi

REPORT_DATE="$1"
DATE_OPT="--date $REPORT_DATE"
SPARK_MASTER="spark://spark-master:7077"

# Airflow 컨테이너는 TTY가 없으므로 -it 옵션 제거
echo "[run_daily_report] Spark 애플리케이션 제출 (report_date=$REPORT_DATE)"

# spark-submit 실행 (spark-master 컨테이너 내부에서 실행)
docker exec spark-master spark-submit \
  --master "$SPARK_MASTER" \
  --packages "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  /opt/bitnami/spark/work/spark/ssh_metrics_aggregator.py $DATE_OPT

exit_code=$?

if [ $exit_code -ne 0 ]; then
  echo "[run_daily_report] ❌ Spark job failed (exit_code=$exit_code)" >&2
  exit $exit_code
fi

echo "[run_daily_report] ✅ Spark job finished successfully" 