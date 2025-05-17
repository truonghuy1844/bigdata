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

# Câu 1: Hãng xe có mức giảm giá trung bình lớn nhất
print("\n=== CÂU 1 ===")
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

# Câu 2: So sánh giá bán trung bình theo loại thân xe và tình trạng xe
print("\n=== CÂU 2 ===")
query2 = """
SELECT IFNULL(body,'[No_Name]') as body, 
       IFNULL(condition, 0) AS condition_group,
       ROUND(AVG(sellingprice), 2) AS avg_price
FROM car_prices
WHERE sellingprice IS NOT NULL
GROUP BY body, condition_group
ORDER BY  condition_group
"""

spark.sql(query2).show()

# Câu 3: Top 5 bang có số lượng xe bán ra nhiều nhất
print("\n=== CÂU 3 ===")
query3 = """
SELECT state, COUNT(*) AS total_sales
FROM car_prices
GROUP BY state
ORDER BY total_sales DESC
LIMIT 5
"""
spark.sql(query3).show()

# Câu 4: Xác định xe bị bán dưới giá trị thị trường trên 5000 USD
print("\n=== CÂU 4 ===")
query4 = """
SELECT make, model, year, mmr, sellingprice,
       (mmr - sellingprice) AS discount
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
  AND (mmr - sellingprice) > 5000
ORDER BY discount DESC
LIMIT 10
"""
spark.sql(query4).show()

# Câu 5: Phân tích nhà bán xe định giá thấp hơn thị trường nhiều nhất
print("\n=== CÂU 5 ===")
query5 = """
SELECT seller,
       COUNT(*) AS total_sales,
       ROUND(AVG(mmr - sellingprice), 2) AS avg_discount,
       ROUND(SUM(mmr - sellingprice), 2) AS total_discount
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
GROUP BY seller
HAVING COUNT(*) > 100
ORDER BY avg_discount DESC
LIMIT 10
"""
spark.sql(query5).show()

# Câu 6: Nhóm xe có dấu hiệu bị định giá sai lệch theo phân vị (outlier detection)
print("\n=== CÂU 6 ===")
query6 = """
SELECT make, model, year, mmr, sellingprice,
       (sellingprice - mmr) AS deviation
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
  AND (sellingprice < 0.25 * mmr OR sellingprice > 1.75 * mmr)
ORDER BY ABS(sellingprice - mmr) DESC
LIMIT 20
"""
spark.sql(query6).show()

# Câu 7: Ảnh hưởng của tình trạng xe đến giá sau khi điều chỉnh số km
print("\n=== CÂU 7 ===")
query7 = """
SELECT condition_group, 
       ROUND(AVG(adjusted_price), 2) AS avg_adj_price
FROM (
    SELECT ROUND(condition) AS condition_group,
           sellingprice / (odometer + 1) AS adjusted_price
    FROM car_prices
    WHERE sellingprice IS NOT NULL AND odometer IS NOT NULL
)
GROUP BY condition_group
ORDER BY condition_group DESC
"""
spark.sql(query7).show()

# Câu 8: Hãng xe giữ giá tốt nhất theo thời gian
print("\n=== CÂU 8 ===")
query8 = """
SELECT year, make,
       ROUND(AVG(sellingprice / mmr), 2) AS price_ratio,
       COUNT(*) AS total_sales
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
GROUP BY year, make
HAVING COUNT(*) > 100
ORDER BY year, price_ratio DESC
"""
spark.sql(query8).show()

# Giữ ứng dụng mở nếu chạy bằng spark-submit
input("\nNhấn Enter để kết thúc...")
