
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, month, year,
    count, avg, round as spark_round
)

spark = (
    SparkSession.builder
    .appName("EcommerceMonthlyDeliveryAnalytics")
    .getOrCreate()
)

# Read orders from HDFS
orders = spark.read.parquet(
    "hdfs://localhost:9000/ecommerce/processed/orders.parquet"
)

# Parse order timestamps
orders = (
    orders
    .withColumn(
        "order_purchase_timestamp",
        to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_delivered_customer_date",
        to_timestamp("order_delivered_customer_date")
    )
)

# Keep delivered orders with valid delivery dates
valid_orders = orders.filter(
    col("order_status").isin("delivered")
    & col("order_purchase_timestamp").isNotNull()
    & col("order_delivered_customer_date").isNotNull()
    & (
        col("order_delivered_customer_date")
        >= col("order_purchase_timestamp")
    )
)

# Calculate delivery duration in days
valid_orders = valid_orders.withColumn(
    "delivery_days",
    (
        col("order_delivered_customer_date").cast("long")
        - col("order_purchase_timestamp").cast("long")
    ) / 86400.0
)

# Aggregate delivery metrics by purchase month
monthly = (
    valid_orders
    .groupBy(
        year("order_purchase_timestamp").alias("year"),
        month("order_purchase_timestamp").alias("month")
    )
    .agg(
        count("*").alias("delivered_orders"),
        spark_round(avg("delivery_days"), 2).alias("avg_delivery_days")
    )
    .orderBy("year", "month")
)

monthly.show(30, truncate=False)

# Save the results back to HDFS
output_path = (
    "hdfs://localhost:9000/ecommerce/analytics/monthly_delivery_v2"
)

monthly.write.mode("overwrite").parquet(output_path)

print("Monthly delivery analytics saved to:", output_path)
print("Valid delivered orders:", valid_orders.count())

spark.stop()
