# Subway-Area-BigData-Analysis
1. 문제 정의:
본 프로젝트는 수도권 지하철역의 시간대별 승하차 인원 데이터와 주변 상권의 밀집도 데이터를 결합하여, 유동인구 패턴이 상권 형성에 미치는 영향을 분석합니다
해결하고자 하는 문제: 유동인구가 가장 많은 역세권의 상권 특징은 무엇인가? 유동인구 대비 상권이 저평가된 지역은 어디인가?
사용 데이터:
    지하철 데이터: 서울 열린데이터 광장 '서울시 지하철 호선별 역별 시간대별 승하차 인원 정보' (CSV)
    상권 데이터: 소상공인시장진흥공단 '상가(상권)정보' (CSV)
데이터 규모: 누적 100MB 이상의 데이터를 확보하여 분석을 수행합니다.
2. 기술 스택:
강의에서 다룬 Hadoop 에코시스템 중 2개 이상의 핵심 컴포넌트를 조합하여 파이프라인을 구축합니다.
항목:수집(Ingest),저장(Storage),처리(Processing),분석(Analysis),시각화(Viz)
사용 기술:Python (Requests),HDFS	, Apache Hive,Apache Spark,Seaborn / Matplotlib 
역할:공공데이터 API 호출 및 수집 스크립트 자동화,수집된 100MB 이상의 정형 데이터 적재,대규모 데이터셋 스키마 정의 및 데이터 조인(Join),유동인구와 상권 밀집도 간의 상관관계 통계 분석,분석 결과(혼잡도 리포트) 그래프 도출
3. 구현 계획 (Pipeline):
모든 소스 코드는 HDP Sandbox 환경에서 실행 가능하도록 구현합니다.
데이터 수집 자동화: Python 스크립트를 통해 주기적으로 데이터를 호출하고 HDFS에 저장하는 파이프라인을 구축합니다.
데이터 전처리: Hive를 활용하여 결측치를 처리하고 역 이름/지역 코드를 기준으로 이종 데이터를 정합합니다.
핵심 분석 질문:

Q1. 시간대별 승하차 인원이 가장 많은 Top 10 역의 주요 업종 분포는?

Q2. 평일 출퇴근 시간대 유동인구와 주말 유동인구에 따른 상권 매출 변화 추이는?

Q3. 역별 승하차 인원 대비 상가 수가 적어 상권 확장이 기대되는 지역은 어디인가?
최종 결과 도출: Spark SQL을 이용해 도출된 통계 수치를 시각화하여 보고서에 반영합니다.
AI Tool Usage: Gemini: 프로젝트 주제 구체화 및 명세서 기반 기술 스택 구성, README.md 구조 설계 도움.

# 실행 가이드
본 프로젝트는 HDP Sandbox 환경에서 PySpark SQL을 활용하여 구현되었습니다. 아래 순서에 따라 코드를 실행하여 분석 결과를 재현할 수 있습니다.
본 프로젝트의 데이터 수집 스크립트(`collect_subway.py`)를 실행하려면 서울시 공공데이터 API 키가 필요합니다.
1. 깃허브에서 코드를 다운로드한 후, `.env.example` 파일의 이름을 `.env`로 변경합니다.
2. `.env` 파일 안에 본인이 발급받은 API 키를 입력하고 저장한 뒤 실행해 주세요.
*(분석 결과만 확인하실 경우 이 과정을 생략하고 3번 분석 스크립트부터 실행하시면 됩니다.)*
# 1. 데이터 준비 (HDFS 업로드)
1) HDFS에 데이터를 저장할 디렉토리 생성
hdfs dfs -mkdir -p /user/maria_dev/project/raw_data/subway

hdfs dfs -mkdir -p /user/maria_dev/project/raw_data/store

3) 로컬 환경의 원본 데이터를 HDFS로 복사
hdfs dfs -put data/subway_merged.csv /user/maria_dev/project/raw_data/subway/

hdfs dfs -put data/store_merged.csv /user/maria_dev/project/raw_data/store/

# 2. 환경 변수 설정 (Python 3.6)
export LANG=en_US.UTF-8

export LC_ALL=en_US.UTF-8

export PYSPARK_PYTHON='/bin/python3.6'

# 3. 분석 스크립트 실행 (Spark Submit)
 src 폴더에 있는 analysis.py 코드를 Spark를 통해 일괄 실행
 
spark-submit --driver-memory 512m --executor-memory 512m src/analysis.py

# 4. 분석 결과 확인
위 스크립트 실행이 완료되면, Q1, Q2, Q3 분석 결과가 HDFS의 아래 경로에 CSV 형태로 자동 저장됩니다.
 HDFS에 저장된 결과물 폴더 확인
 
hdfs dfs -ls /user/maria_dev/project/results/
