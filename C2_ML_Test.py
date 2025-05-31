import os
os.environ["PYSPARK_PYTHON"] = r"C:\\Users\\admin\\AppData\\Local\\Programs\\Python\\Python39\\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, log1p, expm1
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def main():
    spark = SparkSession.builder \
        .appName("CarPriceImprovedML") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # === STEP 1: Đọc và xử lý dữ liệu ===
    df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)
    df = df.dropna(subset=["sellingprice", "year", "odometer", "condition", "make", "model", "body", "state", "mmr"])
    df = df.withColumn("car_age", lit(2025) - col("year"))
    df = df.withColumn("log_price", log1p(col("sellingprice")))

    # === STEP 2: Mã hóa biến phân loại ===
    indexers = [
        StringIndexer(inputCol=c, outputCol=c+"Index", handleInvalid="keep")
        for c in ["make", "model", "body", "state", "transmission"]
    ]
    for indexer in indexers:
        df = indexer.fit(df).transform(df)

    # === STEP 3: Tạo vector đặc trưng ===
    assembler = VectorAssembler(
        inputCols=[
            "car_age", "odometer", "condition", "mmr",
            "makeIndex", "modelIndex", "bodyIndex", "stateIndex", "transmissionIndex"
        ],
        outputCol="features"
    )
    df = assembler.transform(df)

    # === STEP 4: Chuẩn hóa ===
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    # === STEP 5: Chia dữ liệu train/test ===
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)

    # === STEP 6: Huấn luyện mô hình ===
    lr = LinearRegression(featuresCol="scaledFeatures", labelCol="log_price", maxIter=20, regParam=0.1)
    rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol="log_price", numTrees=50, maxDepth=7, seed=42)
    gbt = GBTRegressor(featuresCol="scaledFeatures", labelCol="log_price", maxIter=20, maxDepth=5, stepSize=0.1, seed=42)

    print("Đang huấn luyện các mô hình...")
    lr_model = lr.fit(train_df)
    rf_model = rf.fit(train_df)
    gbt_model = gbt.fit(train_df)

    # === STEP 7: Đánh giá mô hình ===
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
        mae  = evaluator.setMetricName("mae").evaluate(preds)
        r2   = evaluator.setMetricName("r2").evaluate(preds)
        metrics[name] = (rmse, mae, r2)

    print("\n=== Evaluation Summary (log_price) ===")
    print("{:<22s} {:>8s} {:>8s} {:>8s}".format("Model", "RMSE", "MAE", "R²"))
    for name in metrics:
        rmse, mae, r2 = metrics[name]
        print(f"{name:<22s} {rmse:>8.2f} {mae:>8.2f} {r2:>8.2f}")

    best_name = min(metrics, key=lambda k: metrics[k][0])
    best_model = models[best_name]
    print(f"\n✅ Mô hình tốt nhất: {best_name}")

    # === STEP 8: Trực quan hóa ===
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

    # === STEP 9: Dự báo giá với dữ liệu đầu vào mới ===
    print("\n=== Dự báo giá xe dựa trên thông tin nhập từ người dùng ===")
    while True:
        year_in = input("Năm sản xuất (exit để thoát): ").strip()
        if year_in.lower() == "exit": break

        odo_in = input("Số km đã đi (odometer): ").strip()
        cond_in = input("Tình trạng xe (1-5): ").strip()
        mmr_in = input("Giá trị thị trường (mmr): ").strip()
        make_in = input("Hãng xe: ").strip()
        model_in = input("Dòng xe: ").strip()
        body_in = input("Loại thân xe: ").strip()
        state_in = input("Bang đăng ký (mã bang): ").strip()
        trans_in = input("Loại hộp số (automatic/manual): ").strip()

        try:
            car_age = 2025 - int(year_in)
            odo = float(odo_in)
            cond = float(cond_in)
            mmr = float(mmr_in)
        except:
            print("\n❌ Lỗi định dạng, vui lòng nhập lại.\n")
            continue

        temp_df = spark.createDataFrame(
            [(make_in, model_in, body_in, state_in, trans_in)],
            ["make", "model", "body", "state", "transmission"]
        )

        for indexer in indexers:
            temp_df = indexer.fit(df).transform(temp_df)

        user_df = spark.createDataFrame(
            [(car_age, odo, cond, mmr) + tuple(temp_df.select("makeIndex", "modelIndex", "bodyIndex", "stateIndex", "transmissionIndex").first())],
            ["car_age", "odometer", "condition", "mmr", "makeIndex", "modelIndex", "bodyIndex", "stateIndex", "transmissionIndex"]
        )

        features_df = assembler.transform(user_df)
        scaled_df = scaler_model.transform(features_df)
        prediction_log = best_model.transform(scaled_df).first().prediction
        prediction = float(expm1(prediction_log))

        # === TÍNH TOÁN & GỢI Ý ===
        delta = prediction - mmr
        suggested_price = prediction * 1.03

        print(f"\n→ Giá dự báo: khoảng {prediction:,.0f} USD")
        print(f"→ Giá thị trường (MMR): {mmr:,.0f} USD")
        if delta > 0:
            print(f"⚠️ Xe này có thể đang bị định giá CAO hơn thị trường khoảng {delta:,.0f} USD")
        else:
            print(f"✅ Xe này có thể mua HỜI hơn thị trường khoảng {-delta:,.0f} USD")

        print(f"💡 Gợi ý giá NIÊM YẾT: khoảng {suggested_price:,.0f} USD (tăng 3% để thương lượng)\n")

if __name__ == "__main__":
    main()
