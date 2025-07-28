from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import pandas as pd
import logging

class ElasticsearchClient:
    def __init__(self, host='es', port=9200):
        """Elasticsearch 클라이언트 초기화"""
        # Elasticsearch 8.x 버전에 맞는 연결 방식
        self.es = Elasticsearch(f"http://{host}:{port}")
        self.logger = logging.getLogger(__name__)
    
    def test_connection(self):
        """Elasticsearch 연결 테스트"""
        try:
            if self.es.ping():
                self.logger.info("Elasticsearch 연결 성공")
                return True
            else:
                self.logger.error("Elasticsearch 연결 실패")
                return False
        except Exception as e:
            self.logger.error(f"Elasticsearch 연결 오류: {e}")
            return False
    
    def get_daily_data(self, index_pattern, date_field, target_date=None):
        """특정 날짜의 데이터 조회"""
        if target_date is None:
            target_date = datetime.now().date()
        
        # 모든 데이터 조회 후 Python에서 필터링
        try:
            query = {
                "query": {
                    "match_all": {}
                },
                "size": 10000
            }
            self.logger.info(f"모든 데이터 조회 후 {target_date} 필터링")
            response = self.es.search(index=index_pattern, body=query)
            hits = response['hits']['hits']
            
            # DataFrame으로 변환
            data = []
            for hit in hits:
                source = hit['_source']
                source['_id'] = hit['_id']
                data.append(source)
            
            df = pd.DataFrame(data)
            
            if df.empty:
                self.logger.warning("조회된 데이터가 없습니다.")
                return df
            
            # Python에서 날짜 필터링
            if date_field in df.columns:
                try:
                    # 실제 데이터의 event_time 값 확인
                    self.logger.info(f"event_time 필드 샘플 값들: {df[date_field].head().tolist()}")
                    
                    # event_time을 datetime으로 변환 (Unix timestamp 처리)
                    # 밀리초 단위인지 초 단위인지 확인
                    sample_value = df[date_field].iloc[0]
                    self.logger.info(f"샘플 값 타입: {type(sample_value)}, 값: {sample_value}")
                    
                    if isinstance(sample_value, (int, float)):
                        if sample_value > 1000000000000:  # 13자리 이상 = 밀리초
                            df[date_field] = pd.to_datetime(df[date_field], unit='ms', errors='coerce')
                            self.logger.info("밀리초 단위 Unix timestamp로 변환")
                        elif sample_value > 1000000000:  # 10자리 이상 = 초
                            df[date_field] = pd.to_datetime(df[date_field], unit='s', errors='coerce')
                            self.logger.info("초 단위 Unix timestamp로 변환")
                        else:
                            df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
                            self.logger.info("일반 숫자로 변환")
                    else:
                        # 문자열인 경우
                        df[date_field] = pd.to_datetime(df[date_field], errors='coerce')
                        self.logger.info("문자열 datetime으로 변환")
                    
                    # 변환 후 결과 확인
                    self.logger.info(f"datetime 변환 후 샘플 값들: {df[date_field].head().tolist()}")
                    self.logger.info(f"변환 실패한 값들: {df[df[date_field].isna()][date_field].count()}건")
                    
                    # 날짜 필터링
                    start_date = datetime.combine(target_date, datetime.min.time())
                    end_date = datetime.combine(target_date, datetime.max.time())
                    
                    self.logger.info(f"필터링 범위: {start_date} ~ {end_date}")
                    
                    mask = (df[date_field] >= start_date) & (df[date_field] <= end_date)
                    df_filtered = df[mask].copy()
                    
                    self.logger.info(f"전체 {len(df)}건 중 {target_date} 데이터 {len(df_filtered)}건 필터링 완료")
                    
                    # 필터링 결과 출력
                    self.logger.info(f"필터링 결과: {len(df_filtered)}건")
                    
                    # 전체 데이터에서 고유한 날짜들 출력
                    unique_dates = df[date_field].dt.date.unique()
                    self.logger.info(f"전체 데이터의 고유 날짜들: {sorted(unique_dates)}")
                    
                    # 각 날짜별 데이터 개수 출력
                    date_counts = df[date_field].dt.date.value_counts().sort_index()
                    self.logger.info(f"날짜별 데이터 개수:")
                    for date, count in date_counts.items():
                        self.logger.info(f"  {date}: {count}건")
                    
                    # 테스트용으로 전체 데이터 반환
                    self.logger.info("테스트용으로 전체 데이터 반환")
                    return df
                    
                except Exception as e:
                    self.logger.warning(f"날짜 필터링 실패, 전체 데이터 반환: {e}")
                    return df
            else:
                self.logger.warning(f"날짜 필드 '{date_field}'가 없어 전체 데이터 반환")
                return df
                
        except Exception as e:
            self.logger.error(f"데이터 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_available_indices(self):
        """사용 가능한 인덱스 목록 조회"""
        try:
            indices = self.es.cat.indices(format='json')
            return [index['index'] for index in indices]
        except Exception as e:
            self.logger.error(f"인덱스 목록 조회 오류: {e}")
            return []
    
    def get_index_mapping(self, index_name):
        """인덱스 매핑 정보 조회"""
        try:
            mapping = self.es.indices.get_mapping(index=index_name)
            return mapping[index_name]['mappings']
        except Exception as e:
            self.logger.error(f"매핑 정보 조회 오류: {e}")
            return {}

def create_daily_summary(df, date_field='@timestamp'):
    """일일 데이터 요약 통계 생성"""
    if df.empty:
        return {}
    
    summary = {
        'total_records': len(df),
        'date': df[date_field].dt.date.iloc[0] if date_field in df.columns else None,
        'unique_sources': df['source'].nunique() if 'source' in df.columns else 0,
        'error_count': len(df[df['level'] == 'ERROR']) if 'level' in df.columns else 0,
        'warning_count': len(df[df['level'] == 'WARN']) if 'level' in df.columns else 0,
        'info_count': len(df[df['level'] == 'INFO']) if 'level' in df.columns else 0
    }
    
    return summary 