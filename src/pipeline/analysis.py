from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Subway_Store_Analysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("========== [1/4] 데이터 로드 및 전처리 시작 ==========")
df_subway = spark.read.csv("hdfs:///user/maria_dev/project/raw_data/subway/subway_merged.csv", header="true", inferSchema="true")
df_store = spark.read.csv("hdfs:///user/maria_dev/project/raw_data/store/store_merged.csv", header="true", inferSchema="true")

df_subway.createOrReplaceTempView("subway_traffic")
df_store.createOrReplaceTempView("store_info")

print("========== [2/4] Q1 분석 중... ==========")
df_q1 = spark.sql("""
SELECT station_name, peak_traffic, store_type, store_count, rank as top_3_ranking
FROM (
SELECT top10.STTN as station_name, top10.peak_traffic, st.`상권업종중분류명` as store_type, 
COUNT(st.`상가업소번호`) as store_count, 
ROW_NUMBER() OVER(PARTITION BY top10.STTN ORDER BY COUNT(st.`상가업소번호`) DESC) as rank
FROM (
SELECT STTN, SUM(HR_7_GET_ON_NOPE + HR_7_GET_OFF_NOPE + HR_8_GET_ON_NOPE + HR_8_GET_OFF_NOPE + HR_18_GET_ON_NOPE + HR_18_GET_OFF_NOPE + HR_19_GET_ON_NOPE + HR_19_GET_OFF_NOPE) as peak_traffic 
FROM subway_traffic GROUP BY STTN ORDER BY peak_traffic DESC LIMIT 10
) top10
LEFT JOIN store_info st ON (st.`도로명주소` LIKE CONCAT('%', top10.STTN, '%') OR st.`지번주소` LIKE CONCAT('%', top10.STTN, '%') OR st.`건물명` LIKE CONCAT('%', top10.STTN, '%'))
GROUP BY top10.STTN, top10.peak_traffic, st.`상권업종중분류명`
) ranked_data
WHERE rank <= 3
ORDER BY peak_traffic DESC, rank ASC
""")
df_q1.coalesce(1).write.csv("hdfs:///user/maria_dev/project/results/q1_top10_stores", header=True, mode="overwrite")

print("========== [3/4] Q2 분석 중... ==========")
df_q2 = spark.sql("""
WITH morning_stations AS (SELECT DISTINCT STTN FROM subway_traffic WHERE HR_8_GET_OFF_NOPE > 100000),
night_stations AS (SELECT DISTINCT STTN FROM subway_traffic WHERE (HR_22_GET_ON_NOPE + HR_23_GET_ON_NOPE) > 50000),
morning_stores AS (
SELECT st.`상권업종중분류명` as store_type, COUNT(st.`상가업소번호`) as m_count 
FROM morning_stations s LEFT JOIN store_info st ON (st.`도로명주소` LIKE CONCAT('%', s.STTN, '%') OR st.`지번주소` LIKE CONCAT('%', s.STTN, '%') OR st.`건물명` LIKE CONCAT('%', s.STTN, '%')) GROUP BY st.`상권업종중분류명`
),
night_stores AS (
SELECT st.`상권업종중분류명` as store_type, COUNT(st.`상가업소번호`) as n_count 
FROM night_stations s LEFT JOIN store_info st ON (st.`도로명주소` LIKE CONCAT('%', s.STTN, '%') OR st.`지번주소` LIKE CONCAT('%', s.STTN, '%') OR st.`건물명` LIKE CONCAT('%', s.STTN, '%')) GROUP BY st.`상권업종중분류명`
)
SELECT COALESCE(m.store_type, n.store_type) as store_type, COALESCE(m.m_count, 0) as morning_count, COALESCE(n.n_count, 0) as night_count 
FROM morning_stores m FULL OUTER JOIN night_stores n ON m.store_type = n.store_type ORDER BY morning_count DESC, night_count DESC
""")
df_q2.coalesce(1).write.csv("hdfs:///user/maria_dev/project/results/q2_morning_vs_night", header=True, mode="overwrite")

print("========== [4/4] Q3 분석 중... ==========")
df_q3 = spark.sql("""
SELECT top_stations.STTN as station_name, top_stations.peak_traffic, COUNT(st.`상가업소번호`) as total_stores, 
CAST(top_stations.peak_traffic AS DOUBLE) / COUNT(st.`상가업소번호`) as people_per_store
FROM (SELECT STTN, SUM(HR_8_GET_ON_NOPE + HR_8_GET_OFF_NOPE + HR_18_GET_ON_NOPE + HR_18_GET_OFF_NOPE) as peak_traffic FROM subway_traffic GROUP BY STTN) top_stations
LEFT JOIN store_info st ON (st.`도로명주소` LIKE CONCAT('%', top_stations.STTN, '%') OR st.`지번주소` LIKE CONCAT('%', top_stations.STTN, '%') OR st.`건물명` LIKE CONCAT('%', top_stations.STTN, '%'))
GROUP BY top_stations.STTN, top_stations.peak_traffic
HAVING COUNT(st.`상가업소번호`) > 50 
ORDER BY people_per_store DESC
LIMIT 5
""")
df_q3.coalesce(1).write.csv("hdfs:///user/maria_dev/project/results/q3_blue_ocean", header=True, mode="overwrite")

print("========== 모든 분석 완료 및 HDFS 저장 완료! ==========")
spark.stop()