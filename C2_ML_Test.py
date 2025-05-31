import os
os.environ["PYSPARK_PYTHON"] = r"C:/Users/admin/AppData/Local/Programs/Python/Python39/python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, log1p
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


def main():
    spark = SparkSession.builder \
        .appName("CarPriceEstimator") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)
    df = df.dropna(subset=["sellingprice", "year", "odometer", "condition", "make", "model", "body", "state", "transmission", "mmr"])
    df = df.withColumn("car_age", lit(2025) - col("year"))
    df = df.withColumn("log_price", log1p(col("sellingprice")))

    # === Biểu đồ: Phân phối giá bán xe ===
    sample_df = df.select("sellingprice").sample(False, 0.05, seed=42).toPandas()
    plt.figure(figsize=(7, 4))
    sns.histplot(sample_df["sellingprice"], kde=True, bins=30)
    plt.title("Biểu đồ phân phối giá bán xe")
    plt.xlabel("Giá bán")
    plt.ylabel("Số lượng")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # === Biểu đồ: Ma trận tương quan ===
    num_cols = ["car_age", "odometer", "condition", "mmr", "sellingprice"]
    sample_corr = df.select(*num_cols).sample(False, 0.05, seed=42).toPandas()
    plt.figure(figsize=(7, 6))
    sns.heatmap(sample_corr.corr(), annot=True, cmap='coolwarm', fmt=".2f")
    plt.title("Ma trận tương quan giữa các biến số")
    plt.tight_layout()
    plt.show()

    # Mã hóa biến phân loại
    cat_cols = ["make", "model", "body", "state", "transmission"]
    indexers = [StringIndexer(inputCol=c, outputCol=c+"Index", handleInvalid="keep").fit(df) for c in cat_cols]
    for indexer in indexers:
        df = indexer.transform(df)

    assembler = VectorAssembler(
        inputCols=["car_age", "odometer", "condition", "mmr", "makeIndex", "modelIndex", "bodyIndex", "stateIndex", "transmissionIndex"],
        outputCol="features")
    df = assembler.transform(df)

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)

    train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)

    lr = LinearRegression(featuresCol="scaledFeatures", labelCol="log_price", maxIter=20, regParam=0.1)
    rf = RandomForestRegressor(featuresCol="scaledFeatures", labelCol="log_price", numTrees=50, maxDepth=7, seed=42)
    gbt = GBTRegressor(featuresCol="scaledFeatures", labelCol="log_price", maxIter=50, maxDepth=7, stepSize=0.1, seed=42)

    models = {
        "LinearRegression": lr.fit(train_df),
        "RandomForest": rf.fit(train_df),
        "GradientBoostedTree": gbt.fit(train_df)
    }

    evaluator = RegressionEvaluator(labelCol="log_price", predictionCol="prediction")
    metrics = {}
    for name, model in models.items():
        preds = model.transform(test_df)
        rmse = evaluator.setMetricName("rmse").evaluate(preds)
        mae = evaluator.setMetricName("mae").evaluate(preds)
        r2 = evaluator.setMetricName("r2").evaluate(preds)
        metrics[name] = (rmse, mae, r2)

    print("\n=== Evaluation Summary (log_price) ===")
    print("{:<25s} {:>8s} {:>8s} {:>8s}".format("Model", "RMSE", "MAE", "R²"))
    for name in metrics:
        rmse, mae, r2 = metrics[name]
        print(f"{name:<25s} {rmse:>8.2f} {mae:>8.2f} {r2:>8.2f}")

    best_name = max(metrics, key=lambda k: metrics[k][2])
    best_model = models[best_name]
    print(f"\n Mô hình tốt nhất: {best_name}")

    if best_name in ["RandomForest", "GradientBoostedTree"]:
        print("\n Feature Importance:")
        importances = best_model.featureImportances.toArray()
        features = ["car_age", "odometer", "condition", "mmr", "makeIndex", "modelIndex", "bodyIndex", "stateIndex", "transmissionIndex"]
        fi_df = pd.DataFrame({"Feature": features, "Importance": importances}).sort_values("Importance", ascending=False)
        plt.figure(figsize=(6, 4))
        sns.barplot(x="Importance", y="Feature", data=fi_df)
        plt.title(f"Tầm quan trọng của các đặc trưng - {best_name}")
        plt.tight_layout()
        plt.show()

    # Biểu đồ đánh giá mô hình
    model_names = list(metrics.keys())
    rmse_values = [metrics[m][0] for m in model_names]
    mae_values = [metrics[m][1] for m in model_names]
    r2_values = [metrics[m][2] for m in model_names]

    plt.figure(figsize=(6, 4))
    plt.bar(model_names, rmse_values)
    plt.title("So sánh RMSE (log giá)")
    plt.ylabel("RMSE")
    plt.grid(axis='y')
    plt.show()

    plt.figure(figsize=(6, 4))
    plt.bar(model_names, mae_values, color='orange')
    plt.title("So sánh MAE (log giá)")
    plt.ylabel("MAE")
    plt.grid(axis='y')
    plt.show()

    plt.figure(figsize=(6, 4))
    plt.bar(model_names, r2_values, color='green')
    plt.title("So sánh R²")
    plt.ylabel("R²")
    plt.ylim(0, 1)
    plt.grid(axis='y')
    plt.show()

    print("\n=== Car Price Estimator CLI ===")
    while True:
        year_in = input("Năm sản xuất (exit để thoát): ").strip()
        if year_in.lower() == "exit": break
        odo_in = input("Odometer (km): ").strip()
        mmr_in = input("Giá trị thị trường MMR: ").strip()
        make_in = input("Hãng xe: ").strip()

        try:
            year = int(year_in)
            odo = float(odo_in)
            mmr = float(mmr_in)
        except:
            print("   Sai định dạng, vui lòng thử lại.\n")
            continue

        car_age = 2025 - year
        model_in = "corolla"
        body_in = "sedan"
        state_in = "ca"
        trans_in = "automatic"

        input_df = spark.createDataFrame([
            (make_in, model_in, body_in, state_in, trans_in)
        ], ["make", "model", "body", "state", "transmission"])

        for indexer in indexers:
            input_df = indexer.transform(input_df)

        input_data = spark.createDataFrame([
            (float(car_age), odo, 4.0, mmr) + tuple(input_df.select("makeIndex", "modelIndex", "bodyIndex", "stateIndex", "transmissionIndex").first())
        ], ["car_age", "odometer", "condition", "mmr", "makeIndex", "modelIndex", "bodyIndex", "stateIndex", "transmissionIndex"])

        feat = assembler.transform(input_data)
        scaled_feat = scaler_model.transform(feat)
        log_pred = best_model.transform(scaled_feat).first().prediction
        pred = np.expm1(log_pred)

        print(f"\n→ Giá dự báo: {pred:,.0f} USD")
        print(f"→ Giá thị trường (MMR): {mmr:,.0f} USD")

        delta = pred - mmr
        if delta > 0:
            print(f" Dự báo cho thấy bạn đang định giá cao hơn thị trường khoảng {delta:,.0f} USD → người mua có thể không sẵn sàng trả mức giá này")
        else:
            print(f" Dự báo cho thấy mức giá này thấp hơn thị trường khoảng {-delta:,.0f} USD → giá niêm yết này có thể thu hút người mua")

        suggested_price = pred * 1.03
        print(f" Gợi ý GIÁ NIÊM YẾT (để thương lượng): {suggested_price:,.0f} USD\n")


if __name__ == "__main__":
    main()
