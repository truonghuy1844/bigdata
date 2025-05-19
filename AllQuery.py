from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, regexp_replace, year

# Khởi tạo SparkSession (nếu chưa có)
spark = SparkSession.builder.appName("Car Prices").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

# Đọc file CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("car_prices.csv")

# Làm sạch saledate: bỏ phần "GMT-0800 (PST)"
df_cleaned = df.withColumn(
    "saledate_clean",
    regexp_replace("saledate", "GMT.*", "")
)

# Chuyển thành kiểu timestamp
df_parsed = df_cleaned.withColumn(
    "saledate_ts",
    to_timestamp("saledate_clean", "EEE MMM dd yyyy HH:mm:ss")
)

# Trích xuất năm
df_final = df_parsed.withColumn(
    "sale_year",
    year("saledate_ts")
)

# Tạo lại Temp View để dùng SQL
df_final.createOrReplaceTempView("car_prices")

# In schema để kiểm tra
print("=== SCHEMA ===")
df_final.printSchema()

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
percentiles_result = spark.sql("""
SELECT 
  PERCENTILE(sellingprice / mmr, array(0.25, 0.75)) AS ratios
FROM car_prices
WHERE sellingprice IS NOT NULL AND mmr IS NOT NULL
""").collect()

ratios = percentiles_result[0]["ratios"]
q1_ratio, q3_ratio = ratios[0], ratios[1]

query5 = f"""
SELECT seller,
       body,
       COUNT(*) AS total_sales,
       ROUND(AVG(sellingprice - mmr), 2) AS profit_avg,
       ROUND(SUM(sellingprice - mmr), 2) AS total_profit
FROM car_prices
WHERE mmr IS NOT NULL 
      AND sellingprice IS NOT NULL 
      AND body IS NOT NULL
      AND sellingprice / mmr >= {q1_ratio}
      AND sellingprice / mmr <= {q3_ratio}
GROUP BY seller, body
HAVING COUNT(*) > 50
ORDER BY profit_avg DESC
LIMIT 20
"""
spark.sql(query5).show()


# Câu 6: Nhóm xe có dấu hiệu bị định giá sai lệch theo phân vị (outlier detection)
print("\n=== CÂU 6 ===")
query6 = f"""
SELECT make, model, year, mmr, sellingprice,
       (sellingprice - mmr) AS deviation
FROM car_prices
WHERE mmr IS NOT NULL AND sellingprice IS NOT NULL
    AND ( sellingprice / mmr < {q1_ratio}
    OR sellingprice / mmr > {q3_ratio})
ORDER BY ABS(sellingprice - mmr) DESC
LIMIT 20
"""
spark.sql(query6).show()

# Câu 7: Chênh lệch giá bán của mẫu xe Sorento vào năm 2015 so với đời sản xuất trước đó (năm sản xuất)
print("\n=== CÂU 7 ===")
query7 =( """
WITH sorento_yearly_sales AS (
    SELECT
        model,
        year,
        COUNT(*) AS total_sales,
        ROUND(AVG(sellingprice), 2) AS avg_selling_price
    FROM car_prices
    WHERE sellingprice IS NOT NULL
      AND year IS NOT NULL
      AND sale_year = 2015
      AND model = 'Sorento'
    GROUP BY model, year
    HAVING COUNT(*) > 50
)

SELECT 
    year,
    avg_selling_price,
    LAG(year) OVER (PARTITION BY model ORDER BY year DESC) as compare_year,
    LAG(avg_selling_price) OVER (PARTITION BY model ORDER BY year DESC) AS compare_year_price,
    ROUND(
        (avg_selling_price - LAG(avg_selling_price) OVER (PARTITION BY model ORDER BY year DESC)) 
        / LAG(avg_selling_price) OVER (PARTITION BY model ORDER BY year DESC) * 100, 2
    ) AS percent_change
FROM sorento_yearly_sales
ORDER BY year DESC

"""
)
spark.sql(query7).show()

# Câu 8: Hãng xe giữ giá tốt nhất theo thời gian
print("\n=== CÂU 8 ===")
query8 = """
WITH avg AS (
SELECT year, sale_year, model,
       ROUND(AVG(mmr), 2) AS avg_price,
       COUNT(*) AS total_sales,
       ROW_NUMBER() OVER (PARTITION BY year, model ORDER BY sale_year DESC ) AS rn
FROM car_prices
WHERE (mmr IS NOT NULL) AND (sellingprice IS NOT NULL) AND (model IS NOT NULL)
             AND (year IS NOT NULL) AND (sale_year IS NOT NULL)
GROUP BY year, sale_year, model
HAVING total_sales > 50
ORDER BY year DESC, sale_year DESC, avg_price DESC
),

change_rate as (
    SELECT
    year, sale_year, model, total_sales,
    avg_price,
    LAG(sale_year) OVER (PARTITION BY year, model ORDER BY rn) AS compare_year,
        LAG(avg_price) OVER (PARTITION BY year, model ORDER BY rn) AS compare_year_price,
        ROUND(
            (avg_price - LAG(avg_price) OVER (PARTITION BY year, model ORDER BY rn))
            / NULLIF(LAG(avg_price) OVER (PARTITION BY year, model ORDER BY rn), 0) * 100, 
            2
        ) AS percent_change
FROM avg
ORDER BY year
)

    SELECT 
        year, model, SUM(total_sales) as total_sale,
        ROUND(AVG(percent_change),2) AS ratio
    FROM change_rate
    WHERE percent_change IS NOT NULL
    GROUP BY year, model
    ORDER BY ratio DESC


"""
spark.sql(query8).show()

### Câu 9: Hãng được giao dịch nhiều nhất trong mỗi năm cho mỗi loại body (SUV hoặc Sedan) và loại hộp số
print("\n=== CÂU 9 ===")
query9 = """
WITH top_sale AS(
       SELECT year, IFNULL(body,'NO_TYPE') as body, IFNULL(transmission, 'NO_TYPE') as transmission,
        IFNULL(make,'NO_NAME') as brand,
       COUNT(*) AS total_sales,
       ROW_NUMBER() OVER (PARTITION BY year, body ORDER BY COUNT(*) DESC) AS rn
       FROM car_prices WHERE (body like 'SUV%' OR body like 'Sedan%' OR body like 'Van%') AND transmission = 'automatic' AND year > 2010
       GROUP BY year, body, transmission, brand
       
)

SELECT year, body, transmission, brand, total_sales  FROM top_sale WHERE rn <= 3
ORDER BY  year DESC, body, rn ASC
LIMIT 20
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
    ROUND(AVG(sellingprice) / AVG(mmr), 2) AS price_ratio,
    CASE
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) < 0.75 THEN 'Very cheap'
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) >= 0.75 AND ROUND(AVG(sellingprice) / AVG(mmr), 2) < 0.95 THEN 'Cheap'
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) >= 0.95 AND ROUND(AVG(sellingprice) / AVG(mmr), 2) < 1.05 THEN 'Normal'
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) >= 1.05 AND ROUND(AVG(sellingprice) / AVG(mmr), 2) <= 1.25 THEN 'Expensive'
        WHEN ROUND(AVG(sellingprice) / AVG(mmr), 2) > 1.25 THEN 'Very expensive'
        ELSE 'Không xác định' 
    END AS seller_review
FROM car_value_group
GROUP BY seller, value_segment
HAVING COUNT(*) > 50

LIMIT 20
"""
spark.sql(query10).show()

