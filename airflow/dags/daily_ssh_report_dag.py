from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import smtplib
from email.mime.text import MIMEText

# utils 모듈 경로 추가
sys.path.append('/opt/airflow/utils')

from elasticsearch_utils import ElasticsearchClient

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,  # 지수 백오프 활성화
    'max_retry_delay': timedelta(minutes=10),  # 최대 재시도 간격
}

yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
# DAG 정의
dag = DAG(
    'daily_ssh_report',
    default_args=default_args,
    description='SSH 로그 일일 리포트 생성 및 이메일 발송',
    schedule_interval='0 8 * * *',  # 매일 8시에 실행
    catchup=False,
    params={'target_date': yesterday},
    tags=['ssh', 'report', 'daily', 'email']
)

def test_elasticsearch_connection(**context):
    """Elasticsearch 연결 테스트"""
    es_client = ElasticsearchClient()
    
    if es_client.test_connection():
        print("✅ Elasticsearch 연결 성공")
        return "SUCCESS"
    else:
        raise Exception("❌ Elasticsearch 연결 실패")

def extract_ssh_data(**context):
    """SSH 로그 데이터 추출"""
    # DAG 실행 시 전달된 날짜 또는 전날 사용
    dag_run = context['dag_run']
    
    # 1. DAG 실행 시 전달된 특정 날짜가 있는지 확인
    if dag_run.conf and 'target_date' in dag_run.conf:
        try:
            target_date = datetime.strptime(dag_run.conf['target_date'], '%Y-%m-%d').date()
            print(f"🎯 특정 날짜로 실행: {target_date}")
        except ValueError as e:
            print(f"❌ 날짜 형식 오류: {dag_run.conf['target_date']}. 기본값(전날) 사용")
            target_date = datetime.now().date() - timedelta(days=1)
    
    # 2. DAG 실행 시 전달된 날짜 오프셋이 있는지 확인 (예: 3일 전)
    elif dag_run.conf and 'days_ago' in dag_run.conf:
        try:
            days_ago = int(dag_run.conf['days_ago'])
            target_date = datetime.now().date() - timedelta(days=days_ago)
            print(f"📅 {days_ago}일 전 데이터로 실행: {target_date}")
        except ValueError as e:
            print(f"❌ days_ago 형식 오류: {dag_run.conf['days_ago']}. 기본값(전날) 사용")
            target_date = datetime.now().date() - timedelta(days=1)
    
    # 3. 기본값: 전날 데이터
    else:
        target_date = datetime.now().date() - timedelta(days=1)
        print(f"📅 기본값(전날) 데이터로 실행: {target_date}")
    
    date_str = target_date.strftime('%Y-%m-%d')
    
    print(f"📅 추출 대상 날짜: {date_str}")
    
    es_client = ElasticsearchClient()
    
    try:
        # ssh-log 인덱스에서 데이터 조회
        query = {
            "query": {
                "match_all": {}
            },
            "size": 10000
        }
        
        print(f"🔍 ssh-log 인덱스에서 데이터 조회 중...")
        response = es_client.es.search(index="ssh-log", body=query)
        hits = response['hits']['hits']
        
        print(f"📊 총 {response['hits']['total']['value']}건의 데이터 조회 완료")
        
        # DataFrame으로 변환
        data = []
        for hit in hits:
            source = hit['_source']
            source['_id'] = hit['_id']
            data.append(source)
        
        df = pd.DataFrame(data)
        
        if df.empty:
            print("❌ 조회된 데이터가 없습니다.")
            return "NO_DATA"
        
        # event_time 필드 처리
        if 'event_time' in df.columns:
            # event_time을 datetime으로 변환
            df['event_time'] = pd.to_datetime(df['event_time'], unit='ms', errors='coerce')
            
            # 2025-01-01 데이터만 필터링
            start_date = datetime.combine(target_date, datetime.min.time())
            end_date = datetime.combine(target_date, datetime.max.time())
            
            mask = (df['event_time'] >= start_date) & (df['event_time'] <= end_date)
            df_filtered = df[mask].copy()
            
            print(f"📈 {date_str} 데이터 {len(df_filtered)}건 필터링 완료")
            
            if len(df_filtered) == 0:
                print("⚠️ 해당 날짜의 데이터가 없어 전체 데이터 사용")
                df_filtered = df
        
        # 데이터를 XCom에 저장 (JSON 직렬화 가능하게)
        df_serializable = df_filtered.copy()
        for col in df_serializable.columns:
            if df_serializable[col].dtype == 'datetime64[ns]':
                df_serializable[col] = df_serializable[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        context['task_instance'].xcom_push(key='ssh_data', value=df_serializable.to_dict('records'))
        context['task_instance'].xcom_push(key='date_str', value=date_str)
        context['task_instance'].xcom_push(key='data_count', value=len(df_filtered))
        
        print(f"✅ 데이터 추출 완료: {len(df_filtered)}건")
        return "SUCCESS"
        
    except Exception as e:
        print(f"❌ 데이터 추출 실패: {e}")
        return "FAILED"

def generate_ssh_report(**context):
    """SSH 로그 리포트 생성 (간단한 요약만)"""
    # XCom에서 데이터 가져오기
    ssh_data = context['task_instance'].xcom_pull(key='ssh_data')
    date_str = context['task_instance'].xcom_pull(key='date_str')
    
    if not ssh_data:
        print("❌ 데이터가 없어 리포트를 생성할 수 없습니다.")
        return "NO_DATA"
    
    # DataFrame 생성
    df = pd.DataFrame(ssh_data)
    
    # SSH 로그 요약 정보 생성
    summary = {
        'total_records': len(df),
        'date': date_str,
        'unique_sources': df['source'].nunique() if 'source' in df.columns else 0,
        'failed_login_attempts': len(df[df['message'].str.contains('Failed password', na=False)]) if 'message' in df.columns else 0,
        'successful_logins': len(df[df['message'].str.contains('Accepted password', na=False)]) if 'message' in df.columns else 0,
        'invalid_users': len(df[df['message'].str.contains('Invalid user', na=False)]) if 'message' in df.columns else 0,
        'connection_closed': len(df[df['message'].str.contains('Connection closed', na=False)]) if 'message' in df.columns else 0,
        'sessions_opened': len(df[df['message'].str.contains('session opened', na=False)]) if 'message' in df.columns else 0,
        'sessions_closed': len(df[df['message'].str.contains('session closed', na=False)]) if 'message' in df.columns else 0
    }
    
    # 간단한 리포트 결과만 저장
    report_result = {
        'summary': summary
    }
    
    # 리포트 결과를 XCom에 저장
    context['task_instance'].xcom_push(key='report_paths', value=report_result)
    
    print(f"📊 SSH 리포트 요약 생성 완료:")
    print(f"  - 총 로그: {summary['total_records']}건")
    print(f"  - 실패한 로그인: {summary['failed_login_attempts']}건")
    print(f"  - 성공한 로그인: {summary['successful_logins']}건")
    print(f"  - 잘못된 사용자: {summary['invalid_users']}건")
    
    return "SUCCESS"

def send_ssh_report_email(**context):
    """SSH 리포트 이메일 발송 (간단한 요약만)"""
    report_paths = context['task_instance'].xcom_pull(key='report_paths')
    date_str = context['task_instance'].xcom_pull(key='date_str')
    data_count = context['task_instance'].xcom_pull(key='data_count')
    
    if not report_paths:
        print("❌ 리포트 데이터가 없어 이메일을 발송할 수 없습니다.")
        return "NO_REPORT"
    
    summary = report_paths['summary']
    
    # 이메일 제목
    subject = f"🔒 SSH 보안 리포트 - {date_str}"
    
    # 간단한 텍스트 이메일 내용
    text_content = f"""
        SSH 보안 일일 리포트 - {date_str}
        =====================================

        📊 분석 결과:
        - 총 로그 수: {summary['total_records']}건
        - 성공한 로그인: {summary['successful_logins']}건
        - 실패한 로그인 시도: {summary['failed_login_attempts']}건
        - 잘못된 사용자 시도: {summary['invalid_users']}건
        - 접근 소스: {summary['unique_sources']}개

        🔒 보안 상태:
        - 새로운 세션: {summary['sessions_opened']}건
        - 종료된 세션: {summary['sessions_closed']}건
        - 연결 종료: {summary['connection_closed']}건

        ⚠️ 주의사항:
        {summary['failed_login_attempts'] + summary['invalid_users']}건의 의심스러운 접근 시도가 감지되었습니다.

        💡 권장사항:
        - SSH 키 기반 인증 사용
        - 기본 포트 변경
        - fail2ban 설치 고려
        - IP 차단 설정 검토

        생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    
    # 실제 이메일 발송
    try:
        # Airflow SMTP 설정에서 가져오기
        smtp_server = os.getenv('AIRFLOW__SMTP__SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(os.getenv('AIRFLOW__SMTP__SMTP_PORT', '587'))
        smtp_username = os.getenv('AIRFLOW__SMTP__SMTP_USER', 'your-email@gmail.com')
        smtp_password = os.getenv('AIRFLOW__SMTP__SMTP_PASSWORD', 'your-app-password')
        smtp_from = os.getenv('AIRFLOW__SMTP__SMTP_MAIL_FROM', smtp_username)
        
        # 수신자 설정
        recipients_str = os.getenv('REPORT_RECIPIENTS', 'admin@example.com')
        recipient_emails = [email.strip() for email in recipients_str.split(',')]
        
        # 보안 위험도 계산
        security_risk = summary['failed_login_attempts'] + summary['invalid_users']
        risk_level = '높음' if security_risk > 5 else '보통' if security_risk > 0 else '낮음'
        
        print(f"📧 이메일 발송 시도: {subject}")
        print(f"📊 처리된 데이터: {data_count}건")
        print(f"🔒 보안 위험도: {risk_level}")
        
        # 이메일 내용을 로그로 출력
        print("\n📧 이메일 내용:")
        print("=" * 50)
        print(text_content)
        print("=" * 50)
        
        # 실제 SMTP 발송
        if smtp_username != 'your-email@gmail.com' and smtp_password != 'your-app-password':
            for recipient_email in recipient_emails:
                # 간단한 텍스트 이메일 생성
                msg = MIMEText(text_content, 'plain', 'utf-8')
                msg['Subject'] = subject
                msg['From'] = smtp_from
                msg['To'] = recipient_email
                
                # SMTP 서버 연결 및 발송
                with smtplib.SMTP(smtp_server, smtp_port) as server:
                    server.starttls()
                    server.login(smtp_username, smtp_password)
                    server.send_message(msg)
                
                print(f"✅ 이메일 발송 성공: {recipient_email}")
            
            print(f"📧 총 {len(recipient_emails)}명에게 이메일 발송 완료")
        else:
            print("⚠️ SMTP 설정이 완료되지 않아 이메일 발송을 건너뜁니다.")
        
        return "SUCCESS"
        
    except Exception as e:
        print(f"❌ 이메일 발송 실패: {e}")
        return "FAILED"

# 태스크 정의
test_connection_task = PythonOperator(
    task_id='test_elasticsearch_connection',
    python_callable=test_elasticsearch_connection,
    dag=dag
)

extract_data_task = PythonOperator(
    task_id='extract_ssh_data',
    python_callable=extract_ssh_data,
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_ssh_report',
    python_callable=generate_ssh_report,
    dag=dag
)

send_email_task = PythonOperator(
    task_id='send_ssh_report_email',
    python_callable=send_ssh_report_email,
    dag=dag
)

# 태스크 의존성 설정
# test_connection_task >> extract_data_task >> generate_report_task >> send_email_task 
extract_data_task >> generate_report_task >> send_email_task 