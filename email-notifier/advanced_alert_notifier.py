#!/usr/bin/env python3
"""
고급 보안 알림 시스템
- 다양한 보안 이벤트에 대한 세분화된 알림
- 우선순위별 알림 처리
- 알림 중복 방지 및 쿨다운 관리
- Slack, 이메일, 웹훅 등 다중 채널 지원
"""

import os
import json
import logging
import smtplib
import time
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import redis
from typing import Dict, List, Optional
import threading

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AdvancedAlertNotifier:
    def __init__(self):
        """고급 알림 시스템 초기화"""
        # Kafka 설정
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:29092,kafka2:29093,kafka3:29094')
        self.kafka_topic = os.getenv('KAFKA_TOPIC_ALERTS', 'security-alerts')
        
        # 이메일 설정
        self.smtp_host = os.getenv('EMAIL_SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('EMAIL_SMTP_PORT', '587'))
        self.email_from = os.getenv('EMAIL_FROM', 'your-email@gmail.com')
        self.email_to = os.getenv('EMAIL_TO', 'admin@company.com')
        self.email_password = os.getenv('EMAIL_PASSWORD', 'your-app-password')
        
        # Slack 설정
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', '')
        self.slack_channel = os.getenv('SLACK_CHANNEL', '#security-alerts')
        
        # 웹훅 설정
        self.webhook_url = os.getenv('WEBHOOK_URL', '')
        
        # Redis 설정 (중복 알림 방지)
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', '6379'))
        self.redis_db = int(os.getenv('REDIS_DB', '0'))
        
        # 알림 설정
        self.duplicate_window_minutes = int(os.getenv('DUPLICATE_WINDOW_MINUTES', '30'))
        self.critical_cooldown_minutes = int(os.getenv('CRITICAL_COOLDOWN_MINUTES', '5'))
        self.high_cooldown_minutes = int(os.getenv('HIGH_COOLDOWN_MINUTES', '15'))
        self.medium_cooldown_minutes = int(os.getenv('MEDIUM_COOLDOWN_MINUTES', '30'))
        
        # 알림 우선순위 설정
        self.alert_priorities = {
            'brute_force_detected': 'high',
            'ddos_attack_detected': 'critical',
            'suspicious_user_detected': 'medium'
        }
        
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
                group_id='advanced-alert-notifier-group',
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
    
    def is_duplicate_alert(self, alert_data: Dict) -> bool:
        """중복 알림인지 확인"""
        if not self.redis_client:
            return False
        
        try:
            source_ip = alert_data.get('source_ip', '')
            alert_type = alert_data.get('alert_type', '')
            severity = alert_data.get('severity', 'medium')
            
            # Redis 키 생성
            redis_key = f"alert:{alert_type}:{source_ip}:{severity}"
            
            # 중복 확인
            if self.redis_client.exists(redis_key):
                logger.info(f"중복 알림 감지: {source_ip} ({alert_type})")
                return True
            
            # 우선순위별 쿨다운 설정
            cooldown_minutes = self.get_cooldown_minutes(severity)
            
            # 중복 방지 윈도우 설정
            self.redis_client.setex(
                redis_key, 
                cooldown_minutes * 60, 
                datetime.now().isoformat()
            )
            
            return False
        except Exception as e:
            logger.error(f"중복 알림 확인 중 오류: {str(e)}")
            return False
    
    def get_cooldown_minutes(self, severity: str) -> int:
        """우선순위별 쿨다운 시간 반환"""
        cooldown_map = {
            'critical': self.critical_cooldown_minutes,
            'high': self.high_cooldown_minutes,
            'medium': self.medium_cooldown_minutes,
            'low': self.duplicate_window_minutes
        }
        return cooldown_map.get(severity, self.duplicate_window_minutes)
    
    def create_email_content(self, alert_data: Dict) -> str:
        """이메일 내용 생성"""
        alert_type = alert_data.get('alert_type', 'unknown')
        severity = alert_data.get('severity', 'medium')
        source_ip = alert_data.get('source_ip', 'unknown')
        detection_time = alert_data.get('detection_time', 'unknown')
        
        # HTML 이메일 템플릿
        html_template = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .alert {{ border: 2px solid; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .critical {{ border-color: #ff0000; background-color: #ffe6e6; }}
                .high {{ border-color: #ff6600; background-color: #fff2e6; }}
                .medium {{ border-color: #ffcc00; background-color: #ffffe6; }}
                .low {{ border-color: #00cc00; background-color: #e6ffe6; }}
                .details {{ background-color: #f9f9f9; padding: 10px; margin: 10px 0; border-radius: 3px; }}
                .timestamp {{ color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <h2>🚨 보안 알림</h2>
            <div class="alert {severity}">
                <h3>알림 유형: {self.get_alert_type_korean(alert_type)}</h3>
                <p><strong>심각도:</strong> {severity.upper()}</p>
                <p><strong>발견 시간:</strong> {detection_time}</p>
                <p><strong>소스 IP:</strong> {source_ip}</p>
            </div>
            
            <div class="details">
                <h4>상세 정보:</h4>
                {self.create_alert_details_html(alert_data)}
            </div>
            
            <div class="timestamp">
                <p>이 알림은 자동으로 생성되었습니다.</p>
                <p>생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </body>
        </html>
        """
        
        return html_template
    
    def get_alert_type_korean(self, alert_type: str) -> str:
        """알림 유형을 한국어로 변환"""
        type_map = {
            'brute_force_detected': 'Brute Force 공격 탐지',
            'ddos_attack_detected': 'DDoS 공격 탐지',
            'suspicious_user_detected': '의심스러운 사용자 탐지'
        }
        return type_map.get(alert_type, alert_type)
    
    def create_alert_details_html(self, alert_data: Dict) -> str:
        """알림 상세 정보 HTML 생성"""
        details = []
        
        if alert_data.get('alert_type') == 'brute_force_detected':
            details.extend([
                f"<p><strong>실패 시도 횟수:</strong> {alert_data.get('failed_attempts', 0)}회</p>",
                f"<p><strong>임계값:</strong> {alert_data.get('threshold', 0)}회</p>",
                f"<p><strong>시도한 사용자 수:</strong> {alert_data.get('unique_users_attempted', 0)}명</p>",
                f"<p><strong>공격 지속 시간:</strong> {alert_data.get('attack_duration_minutes', 0):.1f}분</p>"
            ])
        elif alert_data.get('alert_type') == 'ddos_attack_detected':
            details.extend([
                f"<p><strong>총 시도 횟수:</strong> {alert_data.get('total_attempts', 0)}회</p>",
                f"<p><strong>실패 횟수:</strong> {alert_data.get('failed_attempts', 0)}회</p>",
                f"<p><strong>성공 횟수:</strong> {alert_data.get('successful_attempts', 0)}회</p>",
                f"<p><strong>실패율:</strong> {alert_data.get('failure_rate', 0):.1%}</p>",
                f"<p><strong>시도한 사용자 수:</strong> {alert_data.get('unique_users_attempted', 0)}명</p>"
            ])
        elif alert_data.get('alert_type') == 'suspicious_user_detected':
            details.extend([
                f"<p><strong>의심스러운 사용자:</strong> {alert_data.get('suspicious_user', 'unknown')}</p>",
                f"<p><strong>실패 시도 횟수:</strong> {alert_data.get('failed_attempts', 0)}회</p>",
                f"<p><strong>소스 IP 수:</strong> {alert_data.get('unique_source_ips', 0)}개</p>",
                f"<p><strong>공격 지속 시간:</strong> {alert_data.get('attack_duration_minutes', 0):.1f}분</p>"
            ])
        
        return '\n'.join(details)
    
    def send_email(self, alert_data: Dict) -> bool:
        """이메일 알림 전송"""
        try:
            # 이메일 내용 생성
            html_content = self.create_email_content(alert_data)
            
            # 이메일 메시지 생성
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[보안 알림] {self.get_alert_type_korean(alert_data.get('alert_type', ''))} - {alert_data.get('severity', 'medium').upper()}"
            msg['From'] = self.email_from
            msg['To'] = self.email_to
            
            # HTML 내용 추가
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)
            
            # SMTP 서버 연결 및 이메일 전송
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_from, self.email_password)
                server.send_message(msg)
            
            logger.info(f"이메일 알림 전송 완료: {alert_data.get('alert_type')}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 전송 실패: {str(e)}")
            return False
    
    def send_slack_notification(self, alert_data: Dict) -> bool:
        """Slack 알림 전송"""
        if not self.slack_webhook_url:
            logger.warning("Slack 웹훅 URL이 설정되지 않았습니다.")
            return False
        
        try:
            alert_type = alert_data.get('alert_type', 'unknown')
            severity = alert_data.get('severity', 'medium')
            source_ip = alert_data.get('source_ip', 'unknown')
            
            # Slack 메시지 생성
            severity_emoji = {
                'critical': '🔴',
                'high': '🟠',
                'medium': '🟡',
                'low': '🟢'
            }
            
            emoji = severity_emoji.get(severity, '⚪')
            
            slack_message = {
                "channel": self.slack_channel,
                "text": f"{emoji} 보안 알림",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{emoji} 보안 알림 - {self.get_alert_type_korean(alert_type)}"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*심각도:*\n{severity.upper()}"
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*소스 IP:*\n{source_ip}"
                            }
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*발견 시간:* {alert_data.get('detection_time', 'unknown')}"
                        }
                    }
                ]
            }
            
            # Slack으로 전송
            response = requests.post(self.slack_webhook_url, json=slack_message)
            response.raise_for_status()
            
            logger.info(f"Slack 알림 전송 완료: {alert_type}")
            return True
            
        except Exception as e:
            logger.error(f"Slack 알림 전송 실패: {str(e)}")
            return False
    
    def send_webhook_notification(self, alert_data: Dict) -> bool:
        """웹훅 알림 전송"""
        if not self.webhook_url:
            logger.warning("웹훅 URL이 설정되지 않았습니다.")
            return False
        
        try:
            # 웹훅 페이로드 생성
            webhook_payload = {
                "timestamp": datetime.now().isoformat(),
                "alert": alert_data,
                "source": "advanced-security-monitor"
            }
            
            # 웹훅으로 전송
            response = requests.post(self.webhook_url, json=webhook_payload)
            response.raise_for_status()
            
            logger.info(f"웹훅 알림 전송 완료: {alert_data.get('alert_type')}")
            return True
            
        except Exception as e:
            logger.error(f"웹훅 알림 전송 실패: {str(e)}")
            return False
    
    def process_alert(self, alert_data: Dict):
        """개별 알림 처리"""
        try:
            # 중복 알림 확인
            if self.is_duplicate_alert(alert_data):
                logger.info("중복 알림이므로 처리하지 않습니다.")
                return
            
            alert_type = alert_data.get('alert_type', 'unknown')
            severity = alert_data.get('severity', 'medium')
            
            logger.info(f"알림 처리 시작: {alert_type} (심각도: {severity})")
            
            # 우선순위별 알림 채널 선택
            if severity == 'critical':
                # Critical: 모든 채널로 알림
                self.send_email(alert_data)
                self.send_slack_notification(alert_data)
                self.send_webhook_notification(alert_data)
            elif severity == 'high':
                # High: 이메일 + Slack
                self.send_email(alert_data)
                self.send_slack_notification(alert_data)
            elif severity == 'medium':
                # Medium: 이메일만
                self.send_email(alert_data)
            else:
                # Low: 로그만 기록
                logger.info(f"낮은 우선순위 알림: {alert_type}")
            
            logger.info(f"알림 처리 완료: {alert_type}")
            
        except Exception as e:
            logger.error(f"알림 처리 중 오류 발생: {str(e)}")
    
    def process_alerts(self):
        """Kafka에서 알림을 지속적으로 처리"""
        logger.info("알림 처리를 시작합니다...")
        
        try:
            for message in self.consumer:
                try:
                    alert_data = message.value
                    logger.info(f"새로운 알림 수신: {alert_data.get('alert_type', 'unknown')}")
                    
                    # 별도 스레드에서 알림 처리 (비동기)
                    alert_thread = threading.Thread(
                        target=self.process_alert, 
                        args=(alert_data,)
                    )
                    alert_thread.start()
                    
                except Exception as e:
                    logger.error(f"메시지 처리 중 오류: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"알림 처리 루프에서 오류: {str(e)}")
    
    def run(self):
        """알림 시스템 실행"""
        try:
            logger.info("고급 알림 시스템을 시작합니다...")
            
            # 초기화
            self.init_kafka_consumer()
            self.init_redis_client()
            
            # 알림 처리 시작
            self.process_alerts()
            
        except KeyboardInterrupt:
            logger.info("알림 시스템이 중단되었습니다.")
        except Exception as e:
            logger.error(f"알림 시스템 실행 중 오류: {str(e)}")
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("알림 시스템이 종료되었습니다.")

def main():
    """메인 실행 함수"""
    notifier = AdvancedAlertNotifier()
    notifier.run()

if __name__ == "__main__":
    main() 