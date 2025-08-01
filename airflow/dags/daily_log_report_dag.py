from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
import requests, json
from datetime import datetime, timedelta
import os
import dotenv

dotenv.load_dotenv('/opt/airflow/configs/airflow_config.env')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

# 환경변수에서 설정 가져오기 (파일 우선, 시스템 환경변수 차선)
REPORT_RECIPIENTS = os.getenv('REPORT_RECIPIENTS')
ES_HOST = os.getenv('ES_HOST')
ES_PORT = os.getenv('ES_PORT')


def send_success_email(**context):
    """성공 이메일 전송"""
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    report_date = context['params']['report_date']
    
    subject = f'✅ 일일 로그 리포트 ({report_date})'

    # metrics-daily 인덱스에서 지표 조회
    try:
        es_url = f'http://{ES_HOST}:{ES_PORT}/metrics-daily/_search'
        query = {
            "size": 1,
            "query": {"term": {"metric_date": report_date}}
        }
        resp = requests.get(es_url, json=query, timeout=10)
        data = resp.json()
        hit = data.get('hits', {}).get('hits', [{}])[0].get('_source', {})
    except Exception as e:
        hit = {}

    def fmt(v):
        return f"{v:,}" if isinstance(v, (int, float)) else v

    total_logs = hit.get('total_logs', 'N/A')
    error_count = hit.get('error_count', 'N/A')
    unique_ips = hit.get('unique_ips', 'N/A')
    peak_hour_logs = hit.get('peak_hour_logs', 'N/A')
    peak_hour_time = hit.get('peak_hour', 'N/A')
    top_ip_list = hit.get('top_ip_list', [])
    brute_cnt   = hit.get('brute_force_ips', 'N/A')
    brute_list  = hit.get('top_brute_list', [])

    def ip_row(ip):
        ip_addr = ip.get('ip', 'N/A')
        cnt_val = ip.get('cnt', 'N/A')
        cnt_disp = fmt(cnt_val)
        return f"<tr><td>{ip_addr}</td><td>{cnt_disp}</td></tr>"

    top_ip_rows = ''.join(ip_row(ip) for ip in top_ip_list) or '<tr><td colspan="2">데이터 없음</td></tr>'

    def brute_row(b):
        ip_addr = b.get('ip', 'N/A')
        cnt_val = b.get('cnt', 'N/A')
        return f"<tr><td>{ip_addr}</td><td>{fmt(cnt_val)}</td></tr>"

    brute_rows = ''.join(brute_row(b) for b in brute_list) or '<tr><td colspan="2">없음</td></tr>'

    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            table {{ border-collapse: collapse; width: 60%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align:center; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h2>✅ 일일 로그 리포트 ({report_date})</h2>

        <h3>요약 지표</h3>
        <table>
          <tr><th>지표</th><th>값</th></tr>
          <tr><td>총 로그 수</td><td>{fmt(total_logs)}</td></tr>
          <tr><td>ERROR 수</td><td>{fmt(error_count)}</td></tr>
          <tr><td>고유 IP</td><td>{fmt(unique_ips)}</td></tr>
          <tr><td>피크 시간대 로그</td><td>{fmt(peak_hour_logs)} (시간대: {peak_hour_time if peak_hour_time!='N/A' else 'N/A'}시)</td></tr>
          <tr><td>브루트포스 의심 IP 수</td><td>{fmt(brute_cnt)}</td></tr>
        </table>

        <h3>Top IP 3</h3>
        <table>
          <tr><th>IP</th><th>건수</th></tr>
          {top_ip_rows}
        </table>

        <h3>Brute-force 의심 IP (Top3)</h3>
        <table>
          <tr><th>IP</th><th>실패건수</th></tr>
          {brute_rows}
        </table>

        <p style="margin-top:10px; font-size:12px;">브루트포스 임계값: 로그인 실패 20회 이상 동일 IP</p>
        <p style="margin-top:20px; font-size:12px;">실행 시각: {execution_date}</p>
    </body>
    </html>
    """
    
    send_email(
        to=REPORT_RECIPIENTS.split(','),
        subject=subject,
        html_content=html_content
    )
    
    return "성공 이메일 전송 완료"

def send_failure_callback(context):
    """실패 시 콜백 함수"""
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    report_date = context['params']['report_date']
    exception = context.get('exception')
    
    subject = f'❌ 일일 로그 리포트 생성 실패 - {report_date}'
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .error {{ color: #dc3545; background-color: #f8d7da; padding: 15px; border-radius: 5px; }}
            .info {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; margin: 10px 0; }}
            .details {{ background-color: #e9ecef; padding: 15px; border-radius: 5px; margin: 10px 0; }}
            .exception {{ background-color: #fff3cd; padding: 10px; border-radius: 5px; margin: 10px 0; font-family: monospace; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="error">
            <h2>❌ 일일 로그 리포트 생성 실패</h2>
        </div>
        
        <div class="info">
            <p><strong>실행 시간:</strong> {execution_date}</p>
            <p><strong>리포트 날짜:</strong> {report_date}</p>
            <p><strong>DAG ID:</strong> {context['dag'].dag_id}</p>
            <p><strong>Task ID:</strong> {context['task'].task_id}</p>
            <p><strong>재시도 횟수:</strong> {task_instance.try_number}</p>
        </div>
        
        <div class="details">
            <h3>🔍 오류 정보</h3>
            <p>일일 로그 리포트 생성 중 오류가 발생했습니다.</p>
        </div>
        
        {f'''
        <div class="exception">
            <h4>예외 정보:</h4>
            <pre>{exception}</pre>
        </div>
        ''' if exception else ''}
        
        <div class="info">
            <p><strong>확인 방법:</strong></p>
            <ul>
                <li>Airflow UI → DAG → Task 로그 확인</li>
                <li>Spark 애플리케이션 로그 확인</li>
                <li>Elasticsearch 연결 상태 확인</li>
            </ul>
        </div>
    </body>
    </html>
    """
    
    send_email(
        to=REPORT_RECIPIENTS.split(','),
        subject=subject,
        html_content=html_content
    )
    
    print(f"실패 이메일 전송 완료: {subject}")




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,  # 커스텀 이메일 사용
    'email_on_retry': False,    # 커스텀 이메일 사용
    'on_failure_callback': send_failure_callback,  # 실패 콜백 추가
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_log_report_20250731',
    default_args=default_args,
    description='일일 로그 리포트 생성',
    schedule_interval='0 1 * * *',  # 매일 새벽 1시
    catchup=False,
    params={'report_date': yesterday},
    tags=['log', 'report', 'daily']
)

# 리포트 생성 태스크
generate_report = BashOperator(
    task_id='generate_daily_report',
    bash_command='/opt/airflow/spark/run_daily_report.sh {{ params.report_date }} 2>&1',
    dag=dag
)

# 성공 이메일 전송 태스크
success_email = PythonOperator(
    task_id='send_success_email',
    python_callable=send_success_email,
    provide_context=True,
    dag=dag
)

# 태스크 의존성 설정
generate_report >> success_email 