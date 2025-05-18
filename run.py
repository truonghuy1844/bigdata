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