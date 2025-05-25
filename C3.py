# Import thư viện cần thiết
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import pandas as pd

def main():
    # 1. Tạo Spark Session (phiên làm việc với Spark)
    spark = SparkSession.builder.appName("CarPricePrediction").getOrCreate()
    
    # 2. Đọc dữ liệu từ file CSV
    df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)
    print("== Dữ liệu đầu vào ==")
    df.show(5)

    # 3. Tiền xử lý dữ liệu: loại bỏ các dòng có giá trị NULL ở những cột quan trọng
    df = df.dropna(subset=["sellingprice", "year", "odometer", "condition", "make"])
    print("== Dữ liệu sau khi drop NULL ==")
    df.show(5)

    # 4. Tạo biến mới: 'car_age' = 2025 - year (giả định năm hiện tại là 2025)
    # Mục đích: tuổi xe ảnh hưởng rất lớn đến giá bán, xe càng cũ thì giá thường giảm
    df = df.withColumn("car_age", lit(2025) - col("year"))
    print("== Dữ liệu có thêm cột tuổi xe ==")
    df.select("year", "car_age").show(5)

    # 5. Chuyển biến hãng xe ('make') từ chữ sang số để mô hình có thể hiểu
    # StringIndexer tự động tạo ra cột 'makeIndex' chứa giá trị số ứng với từng hãng
    makeIndexer = StringIndexer(inputCol="make", outputCol="makeIndex").fit(df)
    df = makeIndexer.transform(df)
    print("== Dữ liệu có thêm cột makeIndex ==")
    df.select("make", "makeIndex").show(5)

    # 6. Chuẩn bị tập đặc trưng (features) cho mô hình
    # VectorAssembler gom các cột 'car_age', 'odometer', 'condition', 'makeIndex' thành một cột vector 'features'
    assembler = VectorAssembler(
        inputCols=["car_age", "odometer", "condition", "makeIndex"],
        outputCol="features"
    )
    df = assembler.transform(df)
    print("== Dữ liệu có cột features ==")
    df.select("car_age", "odometer", "condition", "makeIndex", "features").show(5, truncate=False)

    # 7. Chia dữ liệu thành tập huấn luyện (70%) và tập kiểm tra (30%)
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)
    print(f"Tập huấn luyện: {train_df.count()} dòng")
    print(f"Tập kiểm tra: {test_df.count()} dòng")

    # 8. Tạo mô hình Linear Regression để dự đoán giá bán 'sellingprice' từ đặc trưng 'features'
    lr = LinearRegression(featuresCol="features", labelCol="sellingprice")
    model = lr.fit(train_df)
    print("== Mô hình đã được huấn luyện ==")

    # 9. Dự đoán giá bán trên tập kiểm tra
    predictions = model.transform(test_df)
    print("== Kết quả dự đoán trên tập kiểm tra ==")
    predictions.select("features", "sellingprice", "prediction").show(10)

    # 10. Đánh giá mô hình bằng các chỉ số RMSE, MAE, R2
    evaluator_rmse = RegressionEvaluator(labelCol="sellingprice", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="sellingprice", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="sellingprice", predictionCol="prediction", metricName="r2")

    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print(f"Đánh giá mô hình:")
    print(f"- RMSE (Root Mean Squared Error): {rmse:.2f}")
    print(f"- MAE (Mean Absolute Error): {mae:.2f}")
    print(f"- R2 (Coefficient of Determination): {r2:.2f}")

    # 11. Xuất dữ liệu sang Pandas để trực quan hóa
    pandas_df = df.select("car_age", "make", "sellingprice").toPandas()

    # 12. Vẽ biểu đồ giá trung bình theo tuổi xe
    avg_price_by_age = pandas_df.groupby('car_age')['sellingprice'].mean()
    plt.figure(figsize=(10,5))
    plt.bar(avg_price_by_age.index, avg_price_by_age.values)
    plt.title("Giá bán trung bình theo tuổi xe")
    plt.xlabel("Tuổi xe (năm)")
    plt.ylabel("Giá bán trung bình")
    plt.grid(axis='y')
    plt.show()

    # 13. Vẽ biểu đồ giá trung bình theo hãng xe
    avg_price_by_make = pandas_df.groupby('make')['sellingprice'].mean().sort_values(ascending=False).head(20)
    plt.figure(figsize=(12,6))
    avg_price_by_make.plot(kind='bar')
    plt.title("Top 20 hãng xe theo giá bán trung bình")
    plt.xlabel("Hãng xe")
    plt.ylabel("Giá bán trung bình")
    plt.xticks(rotation=45)
    plt.grid(axis='y')
    plt.show()

    # Kết thúc phiên làm việc Spark
    spark.stop()

if __name__ == "__main__":
    main()
