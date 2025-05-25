from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Tạo Spark session
spark = SparkSession.builder.appName("CarPricePrediction").getOrCreate()

# 2. Đọc dữ liệu
df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)

# 3. Loại bỏ bản ghi thiếu dữ liệu quan trọng
df = df.dropna(subset=["year", "odometer", "condition", "make", "model", "color", "sellingprice"])

# 4. Chuyển các cột categorical sang số (StringIndexer)
indexers = [
    StringIndexer(inputCol="make", outputCol="makeIndex"),
    StringIndexer(inputCol="model", outputCol="modelIndex"),
    StringIndexer(inputCol="color", outputCol="colorIndex")
]
for indexer in indexers:
    df = indexer.fit(df).transform(df)

# 5. Tạo vector features
assembler = VectorAssembler(
    inputCols=["year", "odometer", "condition", "makeIndex", "modelIndex", "colorIndex"],
    outputCol="features"
)
df = assembler.transform(df)

# 6. Chia dữ liệu train/test
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# 7. Khởi tạo Random Forest Regressor
rf = RandomForestRegressor(featuresCol="features", labelCol="sellingprice")

# 8. Tạo grid tham số tuning
paramGrid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .addGrid(rf.numTrees, [50, 100]) \
    .build()

# 9. Đánh giá với RMSE
evaluator = RegressionEvaluator(labelCol="sellingprice", predictionCol="prediction", metricName="rmse")

# 10. CrossValidator để tìm tham số tốt nhất
cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)

# 11. Huấn luyện mô hình
cvModel = cv.fit(train_data)

# 12. Dự đoán trên test
predictions = cvModel.transform(test_data)

# 13. Hiển thị kết quả dự đoán
predictions.select("year", "make", "model", "sellingprice", "prediction").show(10, truncate=False)

# 14. Tính và in RMSE
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error trên tập test: {rmse:.2f}")

# 15. Dừng Spark session
spark.stop()