import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\admin\AppData\Local\Programs\Python\Python39\python.exe"

# file: car_price_tool.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import seaborn as sns # TẢI THÊM: pip install seaborn
# Đảm bảo rằng seaborn đã được cài đặt
import sys

def main():
    # ==== STEP 1: Khởi tạo SparkSession và cấu hình bộ nhớ ====
    spark = SparkSession.builder \
        .appName("CarPriceEstimatorCLI") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ==== STEP 2: Đọc và khám phá dữ liệu ban đầu ====
    df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)
    print("Schema của dữ liệu:")
    df.printSchema()
    df.select("sellingprice", "year", "odometer", "condition", "make").summary().show()

    # Trực quan hóa phân phối giá bán xe
    sample_df = df.select("sellingprice").sample(False, 0.1, seed=42).toPandas()
    plt.figure(figsize=(7, 4))
    sns.histplot(sample_df["sellingprice"], kde=True, bins=30)
    plt.title("Phân phối giá bán xe (sellingprice)")
    plt.xlabel("Selling Price")
    plt.ylabel("Số lượng")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Loại bỏ các dòng có giá trị NULL trong các cột quan trọng
    df = df.dropna(subset=["sellingprice", "year", "odometer", "condition", "make"])

    # ==== STEP 3: Feature Engineering ====
    df = df.withColumn("car_age", lit(2025) - col("year"))
    make_indexer = StringIndexer(inputCol="make", outputCol="makeIndex", handleInvalid="keep").fit(df)
    df = make_indexer.transform(df)
    print("EX Đã mã hóa cột make và thêm đặc trưng tuổi xe")

    assembler = VectorAssembler(
        inputCols=["car_age", "odometer", "condition", "makeIndex"],
        outputCol="features"
    )
    df = assembler.transform(df)

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)
    print(" Đã chuẩn hóa đặc trưng đầu vào")

    # Hiển thị ma trận tương quan giữa các biến số
    num_cols = ["car_age", "odometer", "condition", "makeIndex", "sellingprice"]
    sample_corr = df.select(*num_cols).sample(False, 0.1, seed=42).toPandas()
    plt.figure(figsize=(7, 6))
    sns.heatmap(sample_corr.corr(), annot=True, cmap='coolwarm', fmt=".2f")
    plt.title("Ma trận tương quan giữa các biến")
    plt.tight_layout()
    plt.show()

    # ==== STEP 4: Chia dữ liệu ====
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)
    train_df = train_df.sample(False, 0.5, seed=42)
    print(" Đã chia dữ liệu thành train/test")

    # ==== STEP 5: Huấn luyện mô hình ====
    lr = LinearRegression(featuresCol="scaledFeatures", labelCol="sellingprice", maxIter=20, regParam=0.1)
    rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol="sellingprice", numTrees=20, maxDepth=5, seed=42)

    print("Training Linear Regression...")
    lr_model = lr.fit(train_df)
    print("Training Random Forest...")
    rf_model = rf.fit(train_df)
    print(" Đã huấn luyện xong các mô hình")

    # ==== STEP 6: Đánh giá mô hình ====
    evaluator = RegressionEvaluator(labelCol="sellingprice", predictionCol="prediction")
    metrics = {}
    for name, model in [("LinearRegression", lr_model), ("RandomForest", rf_model)]:
        preds = model.transform(test_df)
        rmse = evaluator.setMetricName("rmse").evaluate(preds)
        mae  = evaluator.setMetricName("mae").evaluate(preds)
        r2   = evaluator.setMetricName("r2").evaluate(preds)
        metrics[name] = (rmse, mae, r2)

    print("\n=== Evaluation Summary ===")
    print("{:<20s} {:>8s} {:>8s} {:>8s}".format("Model", "RMSE", "MAE", "R²"))
    for name in metrics:
        rmse, mae, r2 = metrics[name]
        print(f"{name:<20s} {rmse:>8.2f} {mae:>8.2f} {r2:>8.2f}")

    models = {"LinearRegression": lr_model, "RandomForest": rf_model}
    best_name = min(metrics, key=lambda k: metrics[k][0])
    best_model = models[best_name]
    print(f"\n Best model selected: {best_name}")

    # ==== STEP 7: Trực quan hóa kết quả ====
    model_names = list(metrics.keys())
    rmse_values = [metrics[m][0] for m in model_names]
    mae_values  = [metrics[m][1] for m in model_names]
    r2_values   = [metrics[m][2] for m in model_names]

    plt.figure(figsize=(6,4))
    plt.bar(model_names, rmse_values)
    plt.title("So sánh RMSE")
    plt.ylabel("RMSE")
    plt.grid(axis='y')
    plt.show()

    plt.figure(figsize=(6,4))
    plt.bar(model_names, mae_values, color='orange')
    plt.title("So sánh MAE")
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

    # Biểu đồ thực tế vs dự đoán của mô hình tốt nhất
    best_preds = best_model.transform(test_df)
    pdf = best_preds.select("sellingprice", "prediction").toPandas()
    plt.figure(figsize=(6,6))
    sns.scatterplot(x="sellingprice", y="prediction", data=pdf, alpha=0.5)
    plt.plot([pdf.sellingprice.min(), pdf.sellingprice.max()],
             [pdf.sellingprice.min(), pdf.sellingprice.max()],
             color='red', linestyle='--')
    plt.xlabel("Giá thực tế")
    plt.ylabel("Giá dự đoán")
    plt.title(f"Giá thực tế vs. dự đoán - {best_name}")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # ==== STEP 8: CLI dự đoán ====
    print("\n=== Car Price Estimator CLI ===")
    while True:
        year_in = input("Năm sản xuất (exit để thoát): ").strip()
        if year_in.lower() == "exit": break

        odo_in = input("Odometer (km): ").strip()
        if odo_in.lower() == "exit": break

        cond_in = input("Condition (1–5): ").strip()
        if cond_in.lower() == "exit": break

        make_in = input("Hãng xe: ").strip()
        if make_in.lower() == "exit": break

        try:
            year, odo, cond = int(year_in), float(odo_in), float(cond_in)
        except:
            print("  Sai định dạng, thử lại.\n")
            continue

        car_age = 2025 - year
        tmp = spark.createDataFrame([(make_in,)], ["make"])
        make_idx = make_indexer.transform(tmp).first().makeIndex
        new_df = spark.createDataFrame(
            [(float(car_age), odo, cond, make_idx)],
            ["car_age", "odometer", "condition", "makeIndex"]
        )
        feat = assembler.transform(new_df)
        scaled_feat = scaler_model.transform(feat)
        pred = best_model.transform(scaled_feat).first().prediction
        print(f"→ Giá dự báo: {pred:,.0f} USD\n")

if __name__ == "__main__":
    main()
