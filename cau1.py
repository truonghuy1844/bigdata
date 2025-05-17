from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder \
    .appName("Read Car Prices CSV") \
    .getOrCreate()

# Đọc dữ liệu
df = spark.read.option("header", True).option("inferSchema", True).csv("car_prices.csv")

# Tạo Temp View
df.createOrReplaceTempView("car_prices")

# Hiển thị schema
print("=== SCHEMA ===")
df.printSchema()

query1 = """
SELECT make,
       ROUND(AVG(mmr - sellingprice), 2) AS avg_discount
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
GROUP BY make
ORDER BY avg_discount DESC
LIMIT 10
"""
spark.sql(query1).show()

# Giữ ứng dụng mở nếu chạy bằng spark-submit để quan sát
input("Nhấn Enter để kết thúc...")
