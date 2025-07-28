import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import logging
from fpdf import FPDF
import json

class DailyReportGenerator:
    def __init__(self, output_dir='reports'):
        """리포트 생성기 초기화"""
        # 현재 작업 디렉토리 기준으로 상대 경로 사용
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        
        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)
        
        # 한글 폰트 설정
        plt.rcParams['font.family'] = 'DejaVu Sans'
        sns.set_style("whitegrid")
    
    def create_log_level_chart(self, df, date_str):
        """로그 레벨별 차트 생성"""
        if 'level' not in df.columns or df.empty:
            return None
        
        # 로그 레벨별 카운트
        level_counts = df['level'].value_counts()
        
        # Plotly 파이 차트 생성
        fig = px.pie(
            values=level_counts.values,
            names=level_counts.index,
            title=f'로그 레벨별 분포 ({date_str})',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        # 차트 저장
        chart_path = os.path.join(self.output_dir, f'log_level_chart_{date_str}.html')
        fig.write_html(chart_path)
        
        return chart_path
    
    def create_time_series_chart(self, df, date_str):
        """시간별 로그 발생 추이 차트 생성"""
        if 'event_time' not in df.columns or df.empty:
            return None
        
        try:
            # 시간별 로그 카운트
            df['hour'] = pd.to_datetime(df['event_time']).dt.hour
            hourly_counts = df.groupby('hour').size().reset_index(name='count')
            
            # Plotly 라인 차트 생성
            fig = px.line(
                hourly_counts,
                x='hour',
                y='count',
                title=f'시간별 로그 발생 추이 ({date_str})',
                labels={'hour': '시간', 'count': '로그 수'}
            )
            
            # 차트 저장
            chart_path = os.path.join(self.output_dir, f'time_series_chart_{date_str}.html')
            fig.write_html(chart_path)
            
            return chart_path
        except Exception as e:
            self.logger.warning(f"시간별 차트 생성 실패: {e}")
            return None
    
    def create_source_chart(self, df, date_str):
        """소스별 로그 분포 차트 생성"""
        if 'source' not in df.columns or df.empty:
            return None
        
        try:
            # 소스별 카운트 (상위 10개)
            source_counts = df['source'].value_counts().head(10)
            
            # Plotly 바 차트 생성
            fig = px.bar(
                x=source_counts.values,
                y=source_counts.index,
                orientation='h',
                title=f'소스별 로그 분포 (상위 10개) ({date_str})',
                labels={'x': '로그 수', 'y': '소스'}
            )
            
            # 차트 저장
            chart_path = os.path.join(self.output_dir, f'source_chart_{date_str}.html')
            fig.write_html(chart_path)
            
            return chart_path
        except Exception as e:
            self.logger.warning(f"소스별 차트 생성 실패: {e}")
            return None
    
    def create_message_pattern_chart(self, df, date_str):
        """메시지 패턴별 차트 생성 (SSH 로그용)"""
        if 'message' not in df.columns or df.empty:
            return None
        
        try:
            # SSH 로그 메시지 패턴 분석
            message_patterns = []
            for message in df['message']:
                if 'Failed password' in message:
                    message_patterns.append('Failed password')
                elif 'Accepted password' in message:
                    message_patterns.append('Accepted password')
                elif 'Invalid user' in message:
                    message_patterns.append('Invalid user')
                elif 'Connection closed' in message:
                    message_patterns.append('Connection closed')
                elif 'session opened' in message:
                    message_patterns.append('Session opened')
                elif 'session closed' in message:
                    message_patterns.append('Session closed')
                else:
                    message_patterns.append('Other')
            
            pattern_counts = pd.Series(message_patterns).value_counts()
            
            # Plotly 파이 차트 생성
            fig = px.pie(
                values=pattern_counts.values,
                names=pattern_counts.index,
                title=f'SSH 로그 패턴별 분포 ({date_str})',
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            
            # 차트 저장
            chart_path = os.path.join(self.output_dir, f'message_pattern_chart_{date_str}.html')
            fig.write_html(chart_path)
            
            return chart_path
        except Exception as e:
            self.logger.warning(f"메시지 패턴 차트 생성 실패: {e}")
            return None
    
    def generate_excel_report(self, df, summary, date_str):
        """Excel 리포트 생성"""
        try:
            excel_path = os.path.join(self.output_dir, f'daily_report_{date_str}.xlsx')
            
            with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
                # 요약 정보 시트
                summary_df = pd.DataFrame([summary])
                summary_df.to_excel(writer, sheet_name='요약', index=False)
                
                # 상세 데이터 시트
                if not df.empty:
                    df.to_excel(writer, sheet_name='상세데이터', index=False)
                
                # 로그 레벨별 통계 시트
                if 'level' in df.columns:
                    level_stats = df['level'].value_counts().reset_index()
                    level_stats.columns = ['로그레벨', '건수']
                    level_stats.to_excel(writer, sheet_name='로그레벨통계', index=False)
                
                # 소스별 통계 시트
                if 'source' in df.columns:
                    source_stats = df['source'].value_counts().reset_index()
                    source_stats.columns = ['소스', '건수']
                    source_stats.to_excel(writer, sheet_name='소스별통계', index=False)
            
            self.logger.info(f"Excel 리포트 생성 완료: {excel_path}")
            return excel_path
        except Exception as e:
            self.logger.warning(f"Excel 리포트 생성 실패: {e}")
            return None
    
    def generate_pdf_report(self, summary, chart_paths, date_str):
        """PDF 리포트 생성"""
        pdf_path = os.path.join(self.output_dir, f'daily_report_{date_str}.pdf')
        
        pdf = FPDF()
        pdf.add_page()
        
        # 제목 (영문으로 변경)
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(0, 10, f'Daily Log Report - {date_str}', ln=True, align='C')
        pdf.ln(10)
        
        # 요약 정보
        pdf.set_font('Arial', 'B', 12)
        pdf.cell(0, 10, 'Summary Information', ln=True)
        pdf.set_font('Arial', '', 10)
        
        for key, value in summary.items():
            pdf.cell(0, 8, f'{key}: {value}', ln=True)
        
        pdf.ln(10)
        
        # 차트 정보
        if chart_paths:
            pdf.set_font('Arial', 'B', 12)
            pdf.cell(0, 10, 'Generated Charts', ln=True)
            pdf.set_font('Arial', '', 10)
            
            for chart_path in chart_paths:
                chart_name = os.path.basename(chart_path)
                pdf.cell(0, 8, f'- {chart_name}', ln=True)
        
        pdf.output(pdf_path)
        self.logger.info(f"PDF 리포트 생성 완료: {pdf_path}")
        return pdf_path
    
    def generate_html_report(self, summary, chart_paths, date_str):
        """HTML 리포트 생성"""
        html_path = os.path.join(self.output_dir, f'daily_report_{date_str}.html')
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>일일 로그 리포트 - {date_str}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .summary {{ margin-bottom: 30px; }}
                .summary table {{ border-collapse: collapse; width: 100%; }}
                .summary th, .summary td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .summary th {{ background-color: #f2f2f2; }}
                .charts {{ margin-bottom: 30px; }}
                .chart {{ margin-bottom: 20px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>일일 로그 리포트</h1>
                <h2>{date_str}</h2>
            </div>
            
            <div class="summary">
                <h3>요약 정보</h3>
                <table>
                    <tr><th>항목</th><th>값</th></tr>
        """
        
        for key, value in summary.items():
            html_content += f"<tr><td>{key}</td><td>{value}</td></tr>"
        
        html_content += """
                </table>
            </div>
            
            <div class="charts">
                <h3>차트</h3>
        """
        
        for chart_path in chart_paths:
            chart_name = os.path.basename(chart_path)
            html_content += f'<div class="chart"><iframe src="{chart_name}" width="100%" height="500px"></iframe></div>'
        
        html_content += """
            </div>
        </body>
        </html>
        """
        
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.info(f"HTML 리포트 생성 완료: {html_path}")
        return html_path
    
    def generate_daily_report(self, df, date_str):
        """일일 리포트 전체 생성"""
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
        
        # 차트 생성
        chart_paths = []
        
        log_level_chart = self.create_log_level_chart(df, date_str)
        if log_level_chart:
            chart_paths.append(log_level_chart)
        
        time_series_chart = self.create_time_series_chart(df, date_str)
        if time_series_chart:
            chart_paths.append(time_series_chart)
        
        source_chart = self.create_source_chart(df, date_str)
        if source_chart:
            chart_paths.append(source_chart)
        
        message_pattern_chart = self.create_message_pattern_chart(df, date_str)
        if message_pattern_chart:
            chart_paths.append(message_pattern_chart)
        
        # 리포트 파일들 생성
        excel_path = self.generate_excel_report(df, summary, date_str)
        pdf_path = self.generate_pdf_report(summary, chart_paths, date_str)
        html_path = self.generate_html_report(summary, chart_paths, date_str)
        
        return {
            'summary': summary,
            'excel_path': excel_path,
            'pdf_path': pdf_path,
            'html_path': html_path,
            'chart_paths': chart_paths
        } 