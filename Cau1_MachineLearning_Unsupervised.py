from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

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
    ).fillna(0)
    
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
import builtins
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

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
print("Các bang đã được phân nhóm dựa trên:")  
print("- Tổng số xe")
print("- Giá trung bình")
print("- Năm sản xuất trung bình")
print("- Số km đã đi trung bình")
print("- Tình trạng xe trung bình")

