from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

def generate_daily_summary():
    """간단한 일일 요약 데이터 생성"""
    summary = {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'total_logs': 15000,
        'error_count': 150,
        'success_rate': 99.0,
        'unique_users': 25,
        'peak_hour': 14
    }
    return summary

def send_daily_report(**context):
    """일일 보고서 이메일 발송"""
    try:
        from airflow.utils.email import send_email
        
        summary = context['task_instance'].xcom_pull(task_ids='generate_summary')
        email_recipients = os.getenv('REPORT_RECIPIENTS', 'simsang1db@gmail.com')
        
        html_content = f"""
        <html>
        <body>
            <h2> 일일 로그 분석 보고서</h2>
            <p><strong>날짜:</strong> {summary['date']}</p>
            
            <h3>📈 주요 지표</h3>
            <ul>
                <li>총 로그 수: {summary['total_logs']:,}개</li>
                <li>에러 로그: {summary['error_count']}개</li>
                <li>성공률: {summary['success_rate']}%</li>
                <li>고유 사용자: {summary['unique_users']}명</li>
                <li>피크 시간: {summary['peak_hour']}시</li>
            </ul>
            
            <h3>✅ 상태 요약</h3>
            <p>✅ 시스템 정상 운영 중</p>
            <p>✅ 로그 수집 및 분석 완료</p>
            <p>✅ 에러율 정상 범위 내</p>
            
            <hr>
            <p><small>이 보고서는 Airflow를 통해 자동 생성되었습니다.</small></p>
        </body>
        </html>
        """
        
        result = send_email(
            to=email_recipients.split(','),
            subject=f'📊 일일 로그 분석 보고서 - {summary["date"]}',
            html_content=html_content
        )
        
        print(f"이메일 발송 성공: {result}")
        return "success"
        
    except Exception as e:
        print(f"이메일 발송 실패: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_report_test_dag',
    default_args=default_args,
    description='일일 보고서 테스트용 DAG',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'daily-report']
)

# 1단계: 일일 요약 데이터 생성
generate_summary = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_daily_summary,
    dag=dag
)

# 2단계: 이메일 발송 (EmailOperator 대신 PythonOperator 사용)
send_report = PythonOperator(
    task_id='send_daily_report',
    python_callable=send_daily_report,
    dag=dag
)

# 태스크 순서
generate_summary >> send_report 