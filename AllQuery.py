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

# Câu 1: 5 Hãng xe có mức bán trung bình cao nhất trong mỗi năm
print("\n=== CÂU 1 === TOP 5 hãng xe có mức bán giá thực tế trung bình cao nhất trong mỗi năm")
query1 = """
WITH ranked_makes AS (
    SELECT IFNULL(make, 'NO_NAME') as brand,
           year,
           ROUND(AVG(IFNULL(mmr, 0)), 2) AS avg_market_price,
           ROUND(AVG(IFNULL(sellingprice, 0)), 2) AS avg_actual_price,
           ROW_NUMBER() OVER (PARTITION BY year ORDER BY AVG(IFNULL(sellingprice, 0)) DESC) AS rn
    FROM car_prices
    GROUP BY brand, year
)

SELECT brand, year, avg_market_price, avg_actual_price
FROM ranked_makes
WHERE rn <= 5
ORDER BY year DESC, avg_actual_price DESC
LIMIT 20
"""
spark.sql(query1).show()

# Câu 2: So sánh giá bán trung bình theo loại thân xe và tình trạng xe
print("\n=== CÂU 2 ===")
print("\n=== CÂU 2 ===")
query2 = """
SELECT IFNULL(body,'[No_Name]') as body, 
       IFNULL(condition, 0) AS condition_group,
       ROUND(AVG(sellingprice), 2) AS avg_price
FROM car_prices
WHERE sellingprice IS NOT NULL
GROUP BY body, condition_group
ORDER BY  condition_group, avg_price DESC
LIMIT 10
"""

spark.sql(query2).show()

# Câu 3: Top 3 bang có số lượng xe bán ra nhiều nhất
print("\n=== CÂU 3 ===")
query3 = """
WITH top_sale AS(
       SELECT year, state, COUNT(*) AS total_sales,
      ROW_NUMBER() OVER (PARTITION BY year ORDER BY COUNT(*) DESC) AS rn
       FROM car_prices
       GROUP BY year, state
)

SELECT *  FROM top_sale WHERE rn <= 3
ORDER BY year DESC, total_sales DESC
LIMIT 20
"""
spark.sql(query3).show()

# Câu 4: Xác định xe bị bán dưới giá trị thị trường trên 5000 USD
print("\n=== CÂU 4 ===")
query4 = """
SELECT make as brand, model, year, mmr as market_price, sellingprice as actual_price,
       (mmr - sellingprice) AS diff_price
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
  AND diff_price > 5000
ORDER BY diff_price DESC
LIMIT 10
"""
spark.sql(query4).show()

# Câu 5: Phân tích hiệu quả bán xe của từng đại lý theo loại xe
print("\n=== CÂU 5 ===")
query5 = """
SELECT seller,
       body,
       COUNT(*) AS total_sales,
       ROUND(AVG(sellingprice - mmr), 2) AS avg_margin,
       ROUND(SUM(sellingprice - mmr), 2) AS total_margin
FROM car_prices
WHERE mmr IS NOT NULL 
      AND sellingprice IS NOT NULL 
      AND body IS NOT NULL
      AND 1.25*mmr <= sellingprice 
      AND sellingprice <= 1.75*mmr
GROUP BY seller, body
HAVING COUNT(*) > 50
ORDER BY avg_margin DESC
LIMIT 15
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

