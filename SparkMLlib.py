from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder.appName("CarPricePrediction").getOrCreate()

# Đọc dữ liệu vào Spark DataFrame
df = spark.read.csv("/mnt/data/car_prices.csv", header=True, inferSchema=True)

# Tiền xử lý dữ liệu
# Chuyển các cột categorical như 'make', 'model', 'color' thành các giá trị số (StringIndexer)
indexer_make = StringIndexer(inputCol="make", outputCol="makeIndex")
indexer_model = StringIndexer(inputCol="model", outputCol="modelIndex")
indexer_color = StringIndexer(inputCol="color", outputCol="colorIndex")

# Chuẩn bị các đặc trưng đầu vào (VectorAssembler)
assembler = VectorAssembler(inputCols=["year", "odometer", "condition", "makeIndex", "modelIndex", "colorIndex"], outputCol="features")

# Xây dựng mô hình Random Forest
rf = RandomForestClassifier(featuresCol="features", labelCol="sellingprice", numTrees=50)

# Tuning tham số bằng GridSearch + CrossValidation
paramGrid = ParamGridBuilder() \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .addGrid(rf.numTrees, [50, 100]) \
    .build()

# CrossValidator để tìm tham số tốt nhất
evaluator = RegressionEvaluator(labelCol="sellingprice", predictionCol="prediction", metricName="rmse")
cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

# Huấn luyện mô hình
cvModel = cv.fit(df)

# Dự đoán với mô hình tốt nhất
predictions = cvModel.transform(df)

# Hiển thị kết quả
predictions.select("features", "sellingprice", "prediction").show(5)
