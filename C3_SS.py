# file: car_price_tool.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

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
    best_name, best_model = min(metrics.items(), key=lambda kv: kv[1][0])
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

if __name__=="__main__":
    main()
