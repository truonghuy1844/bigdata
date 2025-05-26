from pyspark.sql import SparkSession
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import builtins
from pyspark.sql.functions import to_timestamp, regexp_replace, year

# Khởi tạo SparkSession (nếu chưa có)
spark = SparkSession.builder.appName("Car Prices").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

# Đọc file CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("car_prices.csv")

# Làm sạch saledate: bỏ phần "GMT-0800 (PST)"
df_cleaned = df.withColumn(
    "saledate_clean",
    regexp_replace("saledate", "GMT.*", "")
)

# Chuyển thành kiểu timestamp
df_parsed = df_cleaned.withColumn(
    "saledate_ts",
    to_timestamp("saledate_clean", "EEE MMM dd yyyy HH:mm:ss")
)

# Trích xuất năm
df_final = df_parsed.withColumn(
    "sale_year",
    year("saledate_ts")
)

# Tạo lại Temp View để dùng SQL
df_final.createOrReplaceTempView("car_prices")

# In schema để kiểm tra
print("=== SCHEMA ===")
df_final.printSchema()

# Câu 1: 5 Hãng xe có mức bán trung bình (sellingprices) cao nhất trong mỗi năm
print("\n=== CÂU 1 === TOP 5 hãng xe có mức bán giá thực tế trung bình cao nhất trong mỗi năm")
query1 = """
WITH ranked_makes AS (
    SELECT IFNULL(make, 'NO_NAME') as brand,
           year,
           ROUND(AVG(IFNULL(mmr, 0)), 2) AS avg_market_price,
           ROUND(AVG(IFNULL(sellingprice, 0)), 2) AS avg_actual_price,
           ROW_NUMBER() OVER (PARTITION BY year ORDER BY AVG(IFNULL(sellingprice, 0)) DESC) AS rn
    FROM car_prices
    GROUP BY brand, year
)

SELECT brand, year, avg_market_price, avg_actual_price
FROM ranked_makes
WHERE rn <= 5
ORDER BY year DESC, avg_actual_price DESC
LIMIT 20
"""
spark.sql(query1).show()

# Câu 2: So sánh giá bán trung bình theo loại thân xe và tình trạng xe
print("\n=== CÂU 2 ===")
query2 = """
SELECT body as body, 
       condition AS condition_group,
       ROUND(AVG(sellingprice), 2) AS avg_price
FROM car_prices
WHERE sellingprice IS NOT NULL AND condition is not null AND body is not null
GROUP BY body, condition_group
ORDER BY  condition_group, avg_price DESC
LIMIT 10
"""

spark.sql(query2).show()

# Câu 3: Top 3 bang có số lượng xe bán ra nhiều nhất
print("\n=== CÂU 3 ===")
query3 = """
WITH top_sale AS(
       SELECT year, state, COUNT(*) AS total_sales,
      ROW_NUMBER() OVER (PARTITION BY year ORDER BY COUNT(*) DESC) AS rn
       FROM car_prices
       where state is not null and year is not null
       GROUP BY year, state
)

SELECT *  FROM top_sale WHERE rn <= 3
ORDER BY year DESC, total_sales DESC
LIMIT 20
"""
spark.sql(query3).show()

# Câu 4: Xác định xe bị bán dưới giá trị thị trường trên 5000 USD
print("\n=== CÂU 4 ===")
query4 = """
SELECT make as brand, model, year, mmr as market_price, sellingprice as actual_price,
       (mmr - sellingprice) AS diff_price
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
  AND (mmr - sellingprice) > 5000
ORDER BY diff_price DESC
LIMIT 10
"""
spark.sql(query4).show()

# Câu 5: Phân tích hiệu quả bán xe của từng đại lý theo loại xe
print("\n=== CÂU 5 ===")
percentiles_result = spark.sql("""
SELECT 
  PERCENTILE(sellingprice / mmr, array(0.25, 0.75)) AS ratios
FROM car_prices
WHERE sellingprice IS NOT NULL AND mmr IS NOT NULL
""").collect()

ratios = percentiles_result[0]["ratios"]
q1_ratio, q3_ratio = ratios[0], ratios[1]

query5 = f"""
SELECT seller,
       body,
       COUNT(*) AS total_sales,
       ROUND(AVG(sellingprice - mmr), 2) AS profit_avg,
       ROUND(SUM(sellingprice - mmr), 2) AS total_profit
FROM car_prices
WHERE mmr IS NOT NULL 
      AND sellingprice IS NOT NULL 
      AND body IS NOT NULL
      AND sellingprice / mmr >= {q1_ratio}
      AND sellingprice / mmr <= {q3_ratio}
GROUP BY seller, body
HAVING COUNT(*) > 50
ORDER BY profit_avg DESC
LIMIT 20
"""
spark.sql(query5).show()


# Câu 6: Nhóm xe có dấu hiệu bị định giá sai lệch theo phân vị (outlier detection)
print("\n=== CÂU 6 ===")
query6 = f"""
SELECT make, model, year, mmr, sellingprice,
       (sellingprice - mmr) AS deviation
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
    AND ( sellingprice / mmr < {q1_ratio}
    OR sellingprice / mmr > {q3_ratio})
ORDER BY ABS(sellingprice - mmr) DESC
LIMIT 20
"""
spark.sql(query6).show()

# Câu 7: Chênh lệch giá bán của mẫu xe Sorento vào năm 2015 so với đời sản xuất trước đó (năm sản xuất)
print("\n=== CÂU 7 ===")
query7 =( """
WITH sorento_yearly_sales AS (
    SELECT
        model,
        year,
        COUNT(*) AS total_sales,
        ROUND(AVG(sellingprice), 2) AS avg_selling_price
    FROM car_prices
    WHERE sellingprice IS NOT NULL
      AND year IS NOT NULL
      AND sale_year = 2015
      AND model = 'Sorento'
    GROUP BY model, year
    HAVING COUNT(*) > 50
)

SELECT 
    year,
    avg_selling_price,
    LAG(year) OVER (PARTITION BY model ORDER BY year DESC) as compare_year,
    LAG(avg_selling_price) OVER (PARTITION BY model ORDER BY year DESC) AS compare_year_price,
    ROUND(
        (avg_selling_price - LAG(avg_selling_price) OVER (PARTITION BY model ORDER BY year DESC)) 
        / LAG(avg_selling_price) OVER (PARTITION BY model ORDER BY year DESC) * 100, 2
    ) AS percent_change
FROM sorento_yearly_sales
ORDER BY year DESC

"""
)
spark.sql(query7).show()

# Câu 8: Hãng xe giữ giá tốt nhất theo thời gian
print("\n=== CÂU 8 ===")
query8 = """
WITH avg AS (
SELECT year, sale_year, model,
       ROUND(AVG(mmr), 2) AS avg_price,
       COUNT(*) AS total_sales,
       ROW_NUMBER() OVER (PARTITION BY year, model ORDER BY sale_year DESC ) AS rn
FROM car_prices
WHERE (mmr IS NOT NULL) AND (sellingprice IS NOT NULL) AND (model IS NOT NULL)
             AND (year IS NOT NULL) AND (sale_year IS NOT NULL)
GROUP BY year, sale_year, model
HAVING total_sales > 50
ORDER BY year DESC, sale_year DESC, avg_price DESC
),

change_rate as (
    SELECT
    year, sale_year, model, total_sales,
    avg_price,
    LAG(sale_year) OVER (PARTITION BY year, model ORDER BY rn) AS compare_year,
        LAG(avg_price) OVER (PARTITION BY year, model ORDER BY rn) AS compare_year_price,
        ROUND(
            (avg_price - LAG(avg_price) OVER (PARTITION BY year, model ORDER BY rn))
            / NULLIF(LAG(avg_price) OVER (PARTITION BY year, model ORDER BY rn), 0) * 100, 
            2
        ) AS percent_change
FROM avg
ORDER BY year
)

    SELECT 
        year, model, SUM(total_sales) as total_sale,
        ROUND(AVG(percent_change),2) AS ratio
    FROM change_rate
    WHERE percent_change IS NOT NULL
    GROUP BY year, model
    ORDER BY ratio DESC


"""
spark.sql(query8).show()

### Câu 9: Hãng được giao dịch nhiều nhất trong mỗi năm cho mỗi loại body (SUV hoặc Sedan) và loại hộp số
print("\n=== CÂU 9 ===")
query9 = """
WITH top_sale AS(
       SELECT year, IFNULL(body,'NO_TYPE') as body, IFNULL(transmission, 'NO_TYPE') as transmission,
        IFNULL(make,'NO_NAME') as brand,
       COUNT(*) AS total_sales,
       ROW_NUMBER() OVER (PARTITION BY year, body ORDER BY COUNT(*) DESC) AS rn
       FROM car_prices WHERE (body like 'SUV%' OR body like 'Sedan%' OR body like 'Van%') AND transmission = 'automatic' AND year > 2010
       GROUP BY year, body, transmission, brand
       
)

SELECT year, body, transmission, brand, total_sales  FROM top_sale WHERE rn <= 3
ORDER BY  year DESC, body, rn ASC
LIMIT 20
"""
spark.sql(query9).show()

### Câu 10: Tìm ra các seller (người bán) trong các phân khúc giá xe khác nhau (Cheap, Normal, Expensive, Very Expensive) dựa trên giá trị thị trường MMR
print("\n=== CÂU 10 ===")
query10= """
WITH car_value_group AS (
  SELECT *,
         CASE
           WHEN mmr < 5000 THEN 'Low'
           WHEN mmr BETWEEN 5000 AND 15000 THEN 'Mid'
           ELSE 'High'
         END AS value_segment
  FROM car_prices
  WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
        AND mmr > 0 AND sellingprice > 0
)
SELECT 
    seller,
    value_segment,
    COUNT(*) AS total_sales,
    ROUND(AVG(sellingprice), 2) AS avg_selling_price,
    ROUND(AVG(mmr), 2) AS avg_market_value,
    ROUND(AVG(sellingprice) / AVG(mmr), 2) AS price_ratio,
    CASE
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) < 0.75 THEN 'Very cheap'
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) >= 0.75 AND ROUND(AVG(sellingprice) / AVG(mmr), 2) < 0.95 THEN 'Cheap'
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) >= 0.95 AND ROUND(AVG(sellingprice) / AVG(mmr), 2) < 1.05 THEN 'Normal'
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) >= 1.05 AND ROUND(AVG(sellingprice) / AVG(mmr), 2) <= 1.25 THEN 'Expensive'
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) > 1.25 THEN 'Very expensive'
        ELSE 'Không xác định' 
    END AS seller_review
FROM car_value_group
GROUP BY seller, value_segment
HAVING COUNT(*) > 50

LIMIT 20
"""
spark.sql(query10).show()
spark.stop()

print("=== MACHINE LEARNING ===")


# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("GeographicMarketSegmentation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation,G1 Concurrent GC") \
    .getOrCreate()

# Load dữ liệu từ file CSV
def load_car_data():
    """Load dữ liệu từ file car_prices.csv"""
    
    # Đọc CSV với header tự động
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiline", "true") \
        .option("escape", '"') \
        .csv("car_prices.csv")
    
    # Kiểm tra và làm sạch dữ liệu
    print(f"Tổng số records ban đầu: {df.count()}")
    
    # Loại bỏ các records có giá trị null trong các cột quan trọng
    df_clean = df.filter(
        col("state").isNotNull() & 
        col("year").isNotNull() & 
        col("odometer").isNotNull()
    )
    
    # Chuyển đổi kiểu dữ liệu nếu cần
    df_clean = df_clean.withColumn("year", col("year").cast(IntegerType())) \
                       .withColumn("odometer", col("odometer").cast(IntegerType())) \
                       .withColumn("condition", col("condition").cast(DoubleType()))
    
    # Thêm cột price nếu không có (có thể từ cột khác hoặc tính toán)
    if "price" not in df_clean.columns:
        # Nếu không có cột price, tạo một cột ước tính dựa trên năm và odometer
        df_clean = df_clean.withColumn("price", 
            when(col("year") >= 2020, 30000 - col("odometer") * 0.1)
            .when(col("year") >= 2015, 20000 - col("odometer") * 0.08)
            .otherwise(15000 - col("odometer") * 0.05)
        )
    
    print(f"Số records sau khi làm sạch: {df_clean.count()}")
    
    return df_clean

# Load dữ liệu từ file
print("=== LOADING DỮ LIỆU TỪ FILE ===")
df = load_car_data()

print("=== TỔNG QUAN DỮ LIỆU ===")
print("Schema của dữ liệu:")
df.printSchema()
print(f"\nTổng số records: {df.count()}")
print(f"Số bang: {df.select('state').distinct().count()}")
print(f"Số hãng xe: {df.select('make').distinct().count()}")

# Hiển thị một vài records mẫu

# Thống kê cơ bản về dữ liệu
print("\nThống kê theo bang:")
df.groupBy("state").count().orderBy(desc("count")).show(10)

print("\nThống kê theo hãng xe:")
df.groupBy("make").count().orderBy(desc("count")).show(10)

# Tạo features để phân tích theo địa lý
def create_geographic_features(df):
    """Tạo các features cho phân vùng địa lý"""
    
    # 1. Tính toán các thống kê cơ bản theo bang
    state_stats = df.groupBy("state").agg(
        count("*").alias("total_vehicles"),
        avg("price").alias("avg_price"),
        avg("year").alias("avg_year"),
        avg("odometer").alias("avg_mileage"),
        avg("condition").alias("avg_condition"),
        stddev("price").alias("price_std"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        # Thêm các metrics khác
        countDistinct("make").alias("brand_diversity"),
        countDistinct("body").alias("body_type_diversity")
    ).fillna(0).filter("total_vehicles >= 5")
    
    # 2. Tính tỷ lệ các loại xe theo bang
    total_by_state = df.groupBy("state").count().withColumnRenamed("count", "total")
    
    body_type_counts = df.groupBy("state", "body").count()
    body_type_percentages = body_type_counts.join(total_by_state, "state") \
        .withColumn("percentage", col("count") / col("total") * 100) \
        .select("state", "body", "percentage") \
        .groupBy("state").pivot("body").sum("percentage").fillna(0)
    
    # 3. Tính tỷ lệ xe theo độ tuổi
    current_year = 2024  # Hoặc lấy năm hiện tại
    df_with_age = df.withColumn("car_age", current_year - col("year"))
    
    age_stats = df_with_age.groupBy("state").agg(
        avg("car_age").alias("avg_car_age"),
        sum(when(col("car_age") <= 3, 1).otherwise(0)).alias("new_cars_count"),
        sum(when((col("car_age") > 3) & (col("car_age") <= 7), 1).otherwise(0)).alias("mid_age_cars_count"),
        sum(when(col("car_age") > 7, 1).otherwise(0)).alias("old_cars_count")
    )
    
    # Tính tỷ lệ phần trăm
    age_stats = age_stats.join(total_by_state, "state") \
        .withColumn("new_cars_pct", col("new_cars_count") / col("total") * 100) \
        .withColumn("mid_age_cars_pct", col("mid_age_cars_count") / col("total") * 100) \
        .withColumn("old_cars_pct", col("old_cars_count") / col("total") * 100) \
        .drop("total", "new_cars_count", "mid_age_cars_count", "old_cars_count")
    
    # 4. Tính tỷ lệ xe theo mức giá
    price_stats = df.groupBy("state").agg(
        sum(when(col("price") < 15000, 1).otherwise(0)).alias("budget_cars_count"),
        sum(when((col("price") >= 15000) & (col("price") < 30000), 1).otherwise(0)).alias("mid_price_cars_count"),
        sum(when(col("price") >= 30000, 1).otherwise(0)).alias("luxury_cars_count")
    )
    
    price_stats = price_stats.join(total_by_state, "state") \
        .withColumn("budget_cars_pct", col("budget_cars_count") / col("total") * 100) \
        .withColumn("mid_price_cars_pct", col("mid_price_cars_count") / col("total") * 100) \
        .withColumn("luxury_cars_pct", col("luxury_cars_count") / col("total") * 100) \
        .drop("total", "budget_cars_count", "mid_price_cars_count", "luxury_cars_count")
    
    # Join tất cả features
    geographic_features = state_stats \
        .join(age_stats, "state", "left") \
        .join(price_stats, "state", "left")
    
    # Thêm body type percentages nếu có đủ dữ liệu
    try:
        geographic_features = geographic_features.join(body_type_percentages, "state", "left")
    except:
        print("Không thể thêm body type percentages - có thể do dữ liệu không đủ đa dạng")
    
    return geographic_features.fillna(0)

# Tạo features theo bang
geo_features = create_geographic_features(df)

# Chuẩn bị dữ liệu cho K-means
def prepare_features_for_clustering(df):
    """Chuẩn bị features cho clustering"""
    
    # Chọn các features chính để clustering (loại bỏ các cột không cần thiết)
    numeric_cols = []
    for col_name, col_type in df.dtypes:
        if col_type in ['int', 'bigint', 'float', 'double'] and col_name != 'state':
            numeric_cols.append(col_name)
    
    # Chọn features quan trọng nhất (tránh quá nhiều features)
    important_features = [
        "total_vehicles",
        "avg_price", 
        "avg_year",
        "avg_mileage",
        "avg_condition",
        "price_std",
        "brand_diversity",
        "avg_car_age",
        "new_cars_pct",
        "luxury_cars_pct"
    ]
    
    # Chỉ lấy các features tồn tại trong data
    feature_cols = [col for col in important_features if col in df.columns]
    
    print(f"Features được sử dụng cho clustering: {feature_cols}")
    
    # Loại bỏ null values và các bang có ít dữ liệu
    clean_df = df.filter(col("total_vehicles") >= 2)  # Chỉ lấy bang có ít nhất 2 xe
    clean_df = clean_df.select(["state"] + feature_cols).na.drop()
    
    print(f"Số bang sau khi lọc: {clean_df.count()}")
    
    # Vector assembler tạo thành vector để tính Kmeans
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    
    # Standard scaler
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, scaler])
    
    return pipeline, feature_cols

# Chuẩn bị pipeline
pipeline, feature_cols = prepare_features_for_clustering(geo_features)

# Fit pipeline
pipeline_model = pipeline.fit(geo_features)
scaled_data = pipeline_model.transform(geo_features)

# Tìm số cluster tối ưu bằng Elbow method


def find_optimal_clusters(data, max_k=None):
    """Tìm số cluster tối ưu"""
    
    # Kiểm tra dữ liệu đầu vào
    if data is None or data.count() == 0:
        raise ValueError("Dữ liệu đầu vào rỗng hoặc không hợp lệ.")
    
    total_states = data.count()
    print("Tổng số dòng dữ liệu:", total_states)
    print("Kiểu dữ liệu:", type(total_states))
    
    if max_k is None:
        if total_states < 2:
            raise ValueError("Giá trị total_states không hợp lệ, cần >= 2 để phân cụm.")
        max_k = builtins.min(8, total_states - 1)  # Không quá 8 và không vượt quá số mẫu
    
    print(f"Tìm kiếm K tối ưu từ 2 đến {max_k} cho {total_states} dòng dữ liệu")
    
    costs = []
    silhouette_scores = []
    
    for k in range(2, max_k + 1):
        try:
            kmeans = KMeans(k=k, seed=42, maxIter=100)
            model = kmeans.fit(data)
            
            cost = model.summary.trainingCost
            costs.append(cost)
            
            predictions = model.transform(data)
            evaluator = ClusteringEvaluator()
            silhouette = evaluator.evaluate(predictions)
            silhouette_scores.append(silhouette)
            
            print(f"K={k}: Cost={cost:.2f}, Silhouette={silhouette:.3f}")
            
        except Exception as e:
            print(f"Lỗi với K={k}: {str(e)}")
            break
    
    return costs, silhouette_scores

print("\n=== TÌM SỐ CLUSTER TỐI ƯU ===")
costs, silhouette_scores = find_optimal_clusters(scaled_data)

# Tự động chọn K tối ưu dựa trên silhouette score
import builtins  # Thêm dòng này ở đầu file nếu chưa có

if silhouette_scores:
    optimal_k = silhouette_scores.index(builtins.max(silhouette_scores)) + 2
    print(f"\nK tối ưu được chọn: {optimal_k} (Silhouette score cao nhất: {builtins.max(silhouette_scores):.3f})")
else:
    optimal_k = 3  # fallback
    print(f"\nSử dụng K mặc định: {optimal_k}")

print(f"\n=== CLUSTERING VỚI K={optimal_k} ===")

# Fit K-means model
kmeans = KMeans(k=optimal_k, seed=42, maxIter=100, tol=1e-4)
kmeans_model = kmeans.fit(scaled_data)

# Dự đoán clusters
predictions = kmeans_model.transform(scaled_data)

print("Kết quả clustering:")
result_df = predictions.select("state", "prediction", "total_vehicles", "avg_price", "avg_year")
result_df.orderBy("prediction", "state").show(50)

# Phân tích clusters
from pyspark.sql.functions import col, avg, min, max, count

# Định nghĩa feature_cols (danh sách các cột đã dùng trong VectorAssembler)
feature_cols = ['year', 'odometer', 'condition', 'sellingprice']  # bạn có thể thay đổi theo dữ liệu thực tế

# Hàm phân tích các cluster
def analyze_clusters(predictions_df, feature_cols):
    """Phân tích đặc điểm của các clusters"""
    
    print("\n=== PHÂN TÍCH CHI TIẾT CÁC CLUSTERS ===")
    
    # Lấy tất cả các cột số để phân tích
    numeric_cols = [c for c in predictions_df.columns 
                   if c not in ['state', 'prediction', 'features', 'features_raw']]
    
    for cluster_id in range(optimal_k):
        print(f"\n{'='*50}")
        print(f"CLUSTER {cluster_id}")
        print(f"{'='*50}")
        
        cluster_data = predictions_df.filter(col("prediction") == cluster_id)
        states_in_cluster = cluster_data.select("state").rdd.flatMap(lambda x: x).collect()
        print(f"Các bang ({len(states_in_cluster)}): {', '.join(sorted(states_in_cluster))}")
        
        # Thống kê cho các cột quan trọng
        important_stats_cols = [c for c in [
            "total_vehicles", "avg_price", "avg_year", 
            "avg_mileage", "avg_condition", "brand_diversity",
            "avg_car_age", "luxury_cars_pct"
        ] if c in numeric_cols]
        
        if important_stats_cols:
            stats_exprs = [count("*").alias("count")]
            for col_name in important_stats_cols:
                stats_exprs.extend([
                    avg(col_name).alias(f"avg_{col_name}"),
                    min(col_name).alias(f"min_{col_name}"),
                    max(col_name).alias(f"max_{col_name}")
                ])
            
            stats = cluster_data.agg(*stats_exprs).collect()[0]
            
            print(f"\nSố bang: {stats['count']}")
            
            for col_name in important_stats_cols:
                avg_val = stats[f"avg_{col_name}"] if f"avg_{col_name}" in stats else 0
                min_val = stats[f"min_{col_name}"] if f"min_{col_name}" in stats else 0
                max_val = stats[f"max_{col_name}"] if f"max_{col_name}" in stats else 0
                
                if avg_val is not None:
                    if col_name in ["avg_price", "price_std"]:
                        print(f"{col_name}: TB=${avg_val:.0f} (${min_val:.0f} - ${max_val:.0f})")
                    elif col_name in ["luxury_cars_pct", "new_cars_pct"]:
                        print(f"{col_name}: TB={avg_val:.1f}% ({min_val:.1f}% - {max_val:.1f}%)")
                    elif col_name == "avg_condition":
                        print(f"{col_name}: TB={avg_val:.2f} ({min_val:.2f} - {max_val:.2f})")
                    else:
                        print(f"{col_name}: TB={avg_val:.1f} ({min_val:.1f} - {max_val:.1f})")

        
        # Hiển thị top bang trong cluster
        print(f"\nTop bang theo số lượng xe:")
        cluster_data.select("state", "total_vehicles", "avg_price") \
                   .orderBy(desc("total_vehicles")) \
                   .show(5, truncate=False)

# Phân tích kết quả
feature_cols = ['year', 'odometer', 'condition', 'sellingprice']  
analyze_clusters(predictions, feature_cols)

# Xuất kết quả để visualization
print("\n=== XUẤT KẾT QUẢ ===")
final_results = predictions.select(
    "state", 
    "prediction",
    "total_vehicles",
    "avg_price",
    "avg_year", 
    "avg_mileage",
    "avg_condition"
).toPandas()

print("Kết quả cuối cùng:")
print(final_results.to_string(index=False))

# Tạo mapping tên cluster có ý nghĩa
cluster_names = {
    0: "Thị trường Cao cấp",
    1: "Thị trường Đại chúng", 
    2: "Thị trường Tiết kiệm"
}

final_results['cluster_name'] = final_results['prediction'].map(cluster_names)

print("\n" + "="*60)
print("=== PHÂN VÙNG THỊ TRƯỜNG CUỐI CÙNG ===")
print("="*60)

for cluster_id, group in final_results.groupby('prediction'):
    cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
    states_list = ', '.join(sorted(group['state'].tolist()))
    avg_price = group['avg_price'].mean()
    avg_year = group['avg_year'].mean()
    total_states = group['state'].nunique()

    print(f"{cluster_name} (ID: {cluster_id})")
    print(f"{'-'*60}")
    print(f"Số bang: {total_states}")
    print(f"Các bang: {states_list}")
    print(f"Giá bán trung bình: ${avg_price:,.0f}")
    print(f"Năm sản xuất trung bình: {avg_year:.1f}")
    print("-"*60)

# Lưu kết quả
print("\n=== LƯU KẾT QUẢ ===")
# Lưu vào Parquet
predictions.select("state", "prediction", "total_vehicles", "avg_price", "avg_year") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("geographic_market_segments")

print("Đã lưu kết quả vào thư mục 'geographic_market_segments'")

# Dừng Spark session
spark.stop()

print("\n=== HOÀN THÀNH ===")
print("Phân vùng địa lý thị trường đã được thực hiện thành công!")


print("\n=== CÂU 2_ML ===")

def main():
   # 1. Tạo SparkSession với thêm driver memory
   spark = SparkSession.builder \
       .appName("CarPriceEstimatorCLI") \
       .config("spark.driver.memory", "4g") \
       .getOrCreate()


   # 2. Đọc dữ liệu và drop NULL
   df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)
   df = df.dropna(subset=["sellingprice", "year", "odometer", "condition", "make"])


   # 3. Thêm cột tuổi xe
   df = df.withColumn("car_age", lit(2025) - col("year"))


   # 4. Mã hoá hãng xe
   make_indexer = StringIndexer(
       inputCol="make", outputCol="makeIndex", handleInvalid="keep"
   ).fit(df)
   df = make_indexer.transform(df)


   # 5. Gộp features
   assembler = VectorAssembler(
       inputCols=["car_age", "odometer", "condition", "makeIndex"],
       outputCol="features"
   )
   df = assembler.transform(df)


   # 6. Chia train/test
   train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)
   # pick 50% của train để bớt nặng
   train_df = train_df.sample(False, 0.5, seed=42)


   # 7. Khởi tạo models
   lr = LinearRegression(
       featuresCol="features", labelCol="sellingprice",
       maxIter=20, regParam=0.1
   )
   rf = RandomForestRegressor(
       featuresCol="features", labelCol="sellingprice",
       numTrees=20, maxDepth=5, maxBins=128, seed=42
   )


   # 8. Huấn luyện
   print("Training Linear Regression...")
   lr_model = lr.fit(train_df)
   print("Training Random Forest...")
   rf_model = rf.fit(train_df)


   # 9. Đánh giá
   evaluator = RegressionEvaluator(
       labelCol="sellingprice", predictionCol="prediction"
   )
   metrics = {}
   for name, model in [("LinearRegression", lr_model), ("RandomForest", rf_model)]:
       preds = model.transform(test_df)
       rmse = evaluator.setMetricName("rmse").evaluate(preds)
       mae  = evaluator.setMetricName("mae" ).evaluate(preds)
       r2   = evaluator.setMetricName("r2"  ).evaluate(preds)
       metrics[name] = (rmse, mae, r2)
       print(f"{name:17s} → RMSE={rmse:.2f}, MAE={mae:.2f}, R2={r2:.2f}")

   # 10. Chọn best_model
   models = {
   "LinearRegression": lr_model,
   "RandomForest": rf_model
   }
   best_name = builtins.min(metrics, key=lambda k: metrics[k][0])  # tên model có RMSE nhỏ nhất
   best_model = models[best_name]
   print(f"\n⭐️  Best model: {best_name}\n")

   # 11. CLI predict
   print("=== Car Price Estimator CLI ===")
   while True:
       year_in = input("Năm sản xuất (exit để thoát): ").strip()
       if year_in.lower()=="exit": break
       odo_in  = input("Odometer (km): ").strip()
       cond_in = input("Condition (1–5): ").strip()
       make_in = input("Hãng xe: ").strip()
       try:
           year, odo, cond = int(year_in), float(odo_in), float(cond_in)
       except:
           print("⚠️  Sai định dạng, thử lại.\n"); continue

       car_age = 2025 - year
       tmp = spark.createDataFrame([(make_in,)], ["make"])
       make_idx = make_indexer.transform(tmp).first().makeIndex
       new_df = spark.createDataFrame(
           [(float(car_age), odo, cond, make_idx)],
           ["car_age","odometer","condition","makeIndex"]
       )
       feat = assembler.transform(new_df)
       pred = best_model.transform(feat).first().prediction
       print(f"→ Estimated price: {pred:,.0f} USD\n")


   spark.stop()


if __name__ == "__main__":
    main()


