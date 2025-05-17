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
  AND (mmr - sellingprice) > 5000
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
      AND sellingprice <= 1.25 * mmr
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
query = """
SELECT 
    make,
    model,
    year,
    COUNT(*) AS total_sales,
    ROUND(AVG(sellingprice), 2) AS avg_selling_price,
    ROUND(STDDEV(sellingprice), 2) AS stddev_price
FROM car_prices
WHERE sellingprice IS NOT NULL AND year IS NOT NULL
GROUP BY make, model, year
HAVING COUNT(*) > 10
ORDER BY make, model, year
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


### Câu 9: 
print("\n=== CÂU 8 ===")
query9 = """
SELECT year, make,
       ROUND(AVG(sellingprice / mmr), 2) AS price_ratio,
       COUNT(*) AS total_sales
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
GROUP BY year, make
HAVING COUNT(*) > 100
ORDER BY year, price_ratio DESC
"""
spark.sql(query9).show()

### Câu 10:
print("\n=== CÂU 10 ===")
query10= """
WITH car_value_group AS (
  SELECT *,
         CASE
           WHEN mmr < 5000 THEN 'Low'
           WHEN mmr BETWEEN 5000 AND 15000 THEN 'Mid'
           ELSE 'High'
         END AS value_segment
  FROM car_prices
  WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
        AND mmr > 0 AND sellingprice > 0
)
SELECT 
    seller,
    value_segment,
    COUNT(*) AS total_sales,
    ROUND(AVG(sellingprice), 2) AS avg_selling_price,
    ROUND(AVG(mmr), 2) AS avg_market_value,
    ROUND(AVG(sellingprice) / AVG(mmr), 2) AS price_ratio
FROM car_value_group
GROUP BY seller, value_segment
HAVING COUNT(*) > 50
ORDER BY price_ratio ASC
LIMIT 20
"""
spark.sql(query10).show()

