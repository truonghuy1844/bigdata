from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder.appName("CarPricePrediction").getOrCreate()

<<<<<<< HEAD
# Đọc dữ liệu
=======
# Đọc dữ liệu vào Spark DataFrame
>>>>>>> 80f17db2fc1a128b0c707bd609e131c0326256fe
df = spark.read.csv("car_prices.csv", header=True, inferSchema=True)

# Áp dụng StringIndexer cho categorical columns
indexer_make = StringIndexer(inputCol="make", outputCol="makeIndex").fit(df)
indexer_model = StringIndexer(inputCol="model", outputCol="modelIndex").fit(df)
indexer_color = StringIndexer(inputCol="color", outputCol="colorIndex").fit(df)

df = indexer_make.transform(df)
df = indexer_model.transform(df)
df = indexer_color.transform(df)

# Chuẩn bị vector features
assembler = VectorAssembler(
    inputCols=["year", "odometer", "condition", "makeIndex", "modelIndex", "colorIndex"],
    outputCol="features"
)
df = assembler.transform(df)

# Khởi tạo mô hình hồi quy Random Forest
rf = RandomForestRegressor(featuresCol="features", labelCol="sellingprice")

# Tạo lưới tham số tuning
paramGrid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .addGrid(rf.numTrees, [50, 100]) \
    .build()

# Khởi tạo evaluator hồi quy
evaluator = RegressionEvaluator(labelCol="sellingprice", predictionCol="prediction", metricName="rmse")

# Khởi tạo cross-validator
cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

# Huấn luyện mô hình
cvModel = cv.fit(df)

# Dự đoán
predictions = cvModel.transform(df)

# Hiển thị kết quả
predictions.select("features", "sellingprice", "prediction").show(5)
