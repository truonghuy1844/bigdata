from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, round
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

def main():
    spark = SparkSession.builder.appName("CarPricePrediction").getOrCreate()

    # Đọc dữ liệu
    df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)

    # Chọn các cột cần thiết, bỏ dòng null
    df = df.select("year", "make", "odometer", "condition", "sellingprice").dropna()

    # Chuyển cột 'make' sang số để làm input
    makeIndexer = StringIndexer(inputCol="make", outputCol="makeIndex", handleInvalid="keep")

    # Tạo vector đặc trưng
    assembler = VectorAssembler(inputCols=["year", "odometer", "condition", "makeIndex"], outputCol="features", handleInvalid="skip")

    # Mô hình hồi quy tuyến tính dự đoán sellingprice
    lr = LinearRegression(featuresCol="features", labelCol="sellingprice")

    # Pipeline nối các bước
    pipeline = Pipeline(stages=[makeIndexer, assembler, lr])

    # Chia train-test
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # Huấn luyện mô hình
    model = pipeline.fit(train_df)

    # Dự đoán trên test
    predictions = model.transform(test_df)

    # Tạo cột nhóm odometer theo khoảng
    predictions = predictions.withColumn(
        "odometer_range",
        when(col("odometer") < 5000, "0-5k")
        .when((col("odometer") >= 5000) & (col("odometer") < 10000), "5k-10k")
        .when((col("odometer") >= 10000) & (col("odometer") < 50000), "10k-50k")
        .otherwise("50k+")
    )

    # Tính giá trung bình dự đoán theo nhóm odometer và hãng xe
    avg_price_by_range_make = predictions.groupBy("odometer_range", "make") \
                                        .avg("prediction") \
                                        .withColumnRenamed("avg(prediction)", "avg_predicted_price") \
                                        .orderBy("odometer_range", "make")

    # Làm tròn giá cho dễ nhìn
    avg_price_by_range_make = avg_price_by_range_make.withColumn("avg_predicted_price", round(col("avg_predicted_price"), 2))

    print("=== Giá trung bình dự đoán theo nhóm odometer và hãng xe ===")
    avg_price_by_range_make.show(100, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
