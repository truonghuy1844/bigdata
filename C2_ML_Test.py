import os
os.environ["PYSPARK_PYTHON"] = r"C:\\Users\\admin\\AppData\\Local\\Programs\\Python\\Python39\\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, log1p, year
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
import sys

def main():
    spark = SparkSession.builder \
        .appName("CarPriceImprovedML") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Đọc dữ liệu và xử lý các giá trị thiếu
    df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)
    df = df.dropna(subset=["sellingprice", "year", "odometer", "condition", "make", "model", "body", "state", "mmr"])

    # Thêm tuổi xe và biến đổi log(sellingprice) để giảm ảnh hưởng outlier
    df = df.withColumn("car_age", lit(2025) - col("year"))
    df = df.withColumn("log_price", log1p(col("sellingprice")))

    # Mã hóa các biến phân loại quan trọng
    indexers = [
        StringIndexer(inputCol=c, outputCol=c+"Index", handleInvalid="keep")
        for c in ["make", "model", "body", "state", "transmission"]
    ]
    for indexer in indexers:
        df = indexer.fit(df).transform(df)

    # VectorAssembler với nhiều đặc trưng hơn
    assembler = VectorAssembler(
        inputCols=[
            "car_age", "odometer", "condition", "mmr",
            "makeIndex", "modelIndex", "bodyIndex", "stateIndex", "transmissionIndex"
        ],
        outputCol="features"
    )
    df = assembler.transform(df)

    # Chuẩn hóa đặc trưng đầu vào
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # Chia train/test
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)

    # Mô hình nâng cao hơn: GBTRegressor
    lr = LinearRegression(featuresCol="scaledFeatures", labelCol="log_price", maxIter=20, regParam=0.1)
    rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol="log_price", numTrees=50, maxDepth=7, seed=42)
    gbt = GBTRegressor(featuresCol="scaledFeatures", labelCol="log_price", maxIter=20, maxDepth=5, stepSize=0.1, seed=42)

    print("Đang huấn luyện các mô hình...")
    lr_model = lr.fit(train_df)
    rf_model = rf.fit(train_df)
    gbt_model = gbt.fit(train_df)

    # Đánh giá mô hình
    evaluator = RegressionEvaluator(labelCol="log_price", predictionCol="prediction")
    models = {
        "LinearRegression": lr_model,
        "RandomForest": rf_model,
        "GradientBoostedTree": gbt_model
    }
    metrics = {}
    for name, model in models.items():
        preds = model.transform(test_df)
        rmse = evaluator.setMetricName("rmse").evaluate(preds)
        mae  = evaluator.setMetricName("mae" ).evaluate(preds)
        r2   = evaluator.setMetricName("r2"  ).evaluate(preds)
        metrics[name] = (rmse, mae, r2)

    print("\n=== Evaluation Summary (log_price) ===")
    print("{:<22s} {:>8s} {:>8s} {:>8s}".format("Model", "RMSE", "MAE", "R²"))
    for name in metrics:
        rmse, mae, r2 = metrics[name]
        print(f"{name:<22s} {rmse:>8.2f} {mae:>8.2f} {r2:>8.2f}")

    # Chọn mô hình tốt nhất dựa trên RMSE
    best_name = min(metrics, key=lambda k: metrics[k][0])
    best_model = models[best_name]
    print(f"\n✅ Mô hình tốt nhất: {best_name}")

    # Biểu đồ so sánh mô hình
    import pandas as pd
    model_names = list(metrics.keys())
    rmse_values = [metrics[m][0] for m in model_names]
    mae_values  = [metrics[m][1] for m in model_names]
    r2_values   = [metrics[m][2] for m in model_names]

    plt.figure(figsize=(6,4))
    plt.bar(model_names, rmse_values)
    plt.title("So sánh RMSE (log giá)")
    plt.ylabel("RMSE")
    plt.grid(axis='y')
    plt.show()

    plt.figure(figsize=(6,4))
    plt.bar(model_names, mae_values, color='orange')
    plt.title("So sánh MAE (log giá)")
    plt.ylabel("MAE")
    plt.grid(axis='y')
    plt.show()

    plt.figure(figsize=(6,4))
    plt.bar(model_names, r2_values, color='green')
    plt.title("So sánh R²")
    plt.ylabel("R²")
    plt.ylim(0, 1)
    plt.grid(axis='y')
    plt.show()

if __name__ == "__main__":
    main()
