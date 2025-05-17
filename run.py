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



### Câu 9: Hãng được giao dịch nhiều nhất trong mỗi năm cho mỗi loại body (SUV hoặc Sedan) và loại hộp số
print("\n=== CÂU 9 ===")
query9 = """
WITH top_sale AS(
       SELECT IFNULL(body,'NO_TYPE') as body, IFNULL(transmission, 'NO_TYPE') as transmission,
        IFNULL(make,'NO_NAME') as brand,
       COUNT(*) AS total_sales,
       ROW_NUMBER() OVER (PARTITION BY body ORDER BY COUNT(*) DESC) AS rn
       FROM car_prices WHERE body like 'SUV%' OR body like 'Sedan%'
       GROUP BY body, transmission, brand
       
)

SELECT body, transmission, brand, total_sales  FROM top_sale WHERE rn <= 5
ORDER BY  body, rn ASC
LIMIT 20
"""
spark.sql(query9).show()