import os
import json
import logging
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
from datetime import datetime
import redis

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmailNotifier:
    def __init__(self):
        """이메일 알림 서비스 초기화"""
        # Kafka 설정
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:29092,kafka2:29093,kafka3:29094')
        self.kafka_topic = os.getenv('KAFKA_TOPIC_ALERTS', 'security-alerts')
        
        # 이메일 설정
        self.smtp_host = os.getenv('EMAIL_SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('EMAIL_SMTP_PORT', '587'))
        self.email_from = os.getenv('EMAIL_FROM', 'your-email@gmail.com')
        self.email_to = os.getenv('EMAIL_TO', 'admin@company.com')
        self.email_password = os.getenv('EMAIL_PASSWORD', 'your-app-password')
        
        # Redis 설정 (중복 알림 방지)
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_db = int(os.getenv('REDIS_DB', '0'))
        
        # 알림 설정
        self.duplicate_window_minutes = int(os.getenv('DUPLICATE_WINDOW_MINUTES', '30'))
        
        # Kafka Consumer 초기화
        self.consumer = None
        self.redis_client = None
        
    def init_kafka_consumer(self):
        """Kafka Consumer 초기화"""
        try:
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers.split(','),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='email-notifier-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000
            )
            logger.info(f"Kafka Consumer가 초기화되었습니다. 토픽: {self.kafka_topic}")
        except Exception as e:
            logger.error(f"Kafka Consumer 초기화 실패: {str(e)}")
            raise
    
    def init_redis_client(self):
        """Redis Client 초기화"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                decode_responses=True
            )
            # Redis 연결 테스트
            self.redis_client.ping()
            logger.info("Redis Client가 초기화되었습니다.")
        except Exception as e:
            logger.warning(f"Redis 연결 실패, 중복 방지 기능이 비활성화됩니다: {str(e)}")
            self.redis_client = None
    
    def is_duplicate_alert(self, alert_data):
        """중복 알림인지 확인"""
        if not self.redis_client:
            return False
        
        try:
            source_ip = alert_data.get('source_ip', '')
            alert_type = alert_data.get('alert_type', '')
            
            # Redis 키 생성
            redis_key = f"alert:{alert_type}:{source_ip}"
            
            # 중복 확인
            if self.redis_client.exists(redis_key):
                logger.info(f"중복 알림 감지: {source_ip}")
                return True
            
            # 중복 방지 윈도우 설정
            self.redis_client.setex(
                redis_key, 
                self.duplicate_window_minutes * 60, 
                datetime.now().isoformat()
            )
            
            return False
        except Exception as e:
            logger.error(f"중복 확인 중 오류: {str(e)}")
            return False
    
    def create_email_content(self, alert_data):
        """이메일 내용 생성"""
        source_ip = alert_data.get('source_ip', 'Unknown')
        failed_attempts = alert_data.get('failed_attempts', 0)
        detection_time = alert_data.get('detection_time', 'Unknown')
        threshold = alert_data.get('threshold', 5)
        time_window = alert_data.get('time_window_minutes', 10)
        
        # HTML 이메일 템플릿
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .alert {{ background-color: #ffebee; border: 1px solid #f44336; padding: 15px; border-radius: 5px; }}
                .header {{ color: #d32f2f; font-size: 18px; font-weight: bold; margin-bottom: 15px; }}
                .detail {{ margin: 10px 0; }}
                .label {{ font-weight: bold; color: #333; }}
                .value {{ color: #666; }}
                .recommendation {{ background-color: #e3f2fd; padding: 10px; border-radius: 3px; margin-top: 15px; }}
            </style>
        </head>
        <body>
            <div class="alert">
                <div class="header">🚨 보안 경고: 브루트 포스 공격 탐지</div>
                
                <div class="detail">
                    <span class="label">공격 IP:</span>
                    <span class="value">{source_ip}</span>
                </div>
                
                <div class="detail">
                    <span class="label">탐지 시간:</span>
                    <span class="value">{detection_time}</span>
                </div>
                
                <div class="detail">
                    <span class="label">실패 시도 횟수:</span>
                    <span class="value">{failed_attempts}회 (임계값: {threshold}회)</span>
                </div>
                
                <div class="detail">
                    <span class="label">시간 범위:</span>
                    <span class="value">{time_window}분</span>
                </div>
                
                <div class="detail">
                    <span class="label">위험도:</span>
                    <span class="value">높음 (High)</span>
                </div>
                
                <div class="recommendation">
                    <strong>권장 조치사항:</strong><br>
                    • 해당 IP 주소를 방화벽에서 차단<br>
                    • SSH 접근 로그를 추가로 모니터링<br>
                    • 시스템 보안 점검 수행<br>
                    • 필요시 법적 조치 고려
                </div>
            </div>
        </body>
        </html>
        """
        
        # 텍스트 이메일 (HTML을 지원하지 않는 클라이언트용)
        text_content = f"""
보안 경고: 브루트 포스 공격 탐지

공격 IP: {source_ip}
탐지 시간: {detection_time}
실패 시도 횟수: {failed_attempts}회 (임계값: {threshold}회)
시간 범위: {time_window}분
위험도: 높음 (High)

권장 조치사항:
• 해당 IP 주소를 방화벽에서 차단
• SSH 접근 로그를 추가로 모니터링
• 시스템 보안 점검 수행
• 필요시 법적 조치 고려
        """
        
        return html_content, text_content
    
    def send_email(self, alert_data):
        """이메일 발송"""
        try:
            # 중복 알림 확인
            if self.is_duplicate_alert(alert_data):
                logger.info("중복 알림으로 인해 이메일 발송을 건너뜁니다.")
                return False
            
            # 이메일 내용 생성
            html_content, text_content = self.create_email_content(alert_data)
            
            # 이메일 메시지 생성
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[보안 경고] 브루트 포스 공격 탐지 - {alert_data.get('source_ip', 'Unknown')}"
            msg['From'] = self.email_from
            msg['To'] = self.email_to
            
            # HTML 및 텍스트 내용 추가
            text_part = MIMEText(text_content, 'plain', 'utf-8')
            html_part = MIMEText(html_content, 'html', 'utf-8')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            # SMTP 서버 연결 및 이메일 발송
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_from, self.email_password)
                server.send_message(msg)
            
            logger.info(f"보안 알림 이메일이 성공적으로 발송되었습니다. 수신자: {self.email_to}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 발송 실패: {str(e)}")
            return False
    
    def process_alerts(self):
        """알림 메시지 처리"""
        logger.info("알림 메시지 처리를 시작합니다...")
        
        try:
            for message in self.consumer:
                try:
                    # 메시지 파싱
                    alert_data = message.value
                    logger.info(f"새로운 보안 알림 수신: {alert_data.get('source_ip', 'Unknown')}")
                    
                    # 이메일 발송
                    success = self.send_email(alert_data)
                    
                    if success:
                        logger.info("알림 처리가 완료되었습니다.")
                    else:
                        logger.warning("알림 처리에 실패했습니다.")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"JSON 파싱 오류: {str(e)}")
                except Exception as e:
                    logger.error(f"알림 처리 중 오류: {str(e)}")
                    
        except Exception as e:
            logger.error(f"메시지 처리 중 오류: {str(e)}")
    
    def run(self):
        """서비스 실행"""
        logger.info("이메일 알림 서비스를 시작합니다...")
        
        try:
            # 초기화
            self.init_kafka_consumer()
            self.init_redis_client()
            
            logger.info("모든 초기화가 완료되었습니다.")
            logger.info(f"Kafka 토픽: {self.kafka_topic}")
            logger.info(f"이메일 수신자: {self.email_to}")
            
            # 알림 처리 시작
            self.process_alerts()
            
        except KeyboardInterrupt:
            logger.info("사용자에 의해 서비스가 중단되었습니다.")
        except Exception as e:
            logger.error(f"서비스 실행 중 오류: {str(e)}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka Consumer가 종료되었습니다.")

def main():
    """메인 실행 함수"""
    notifier = EmailNotifier()
    notifier.run()

if __name__ == "__main__":
    main() 