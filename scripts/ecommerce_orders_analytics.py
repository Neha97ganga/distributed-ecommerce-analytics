from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    expr,
    month,
    year
)

spark = (
    SparkSession.builder
    .appName("OlistOrdersAnalytics")
    .master("local[2]")
    .getOrCreate()
)

# Read the real Olist orders dataset from HDFS
orders = spark.read.parquet(
    "hdfs://localhost:9000/ecommerce/processed/orders.parquet"
)

print("Total orders:", orders.count())

print("Order schema:")
orders.printSchema()

# Convert timestamps to Spark timestamp types
orders = (
    orders
    .withColumn(
        "order_purchase_timestamp",
        col("order_purchase_timestamp").cast("timestamp")
    )
    .withColumn(
        "order_delivered_customer_date",
        col("order_delivered_customer_date").cast("timestamp")
    )
)

# Analyze delivered orders with valid delivery timestamps
delivered = orders.filter(
    (col("order_status") == "delivered")
    & col("order_purchase_timestamp").isNotNull()
    & col("order_delivered_customer_date").isNotNull()
    & (
        col("order_delivered_customer_date")
        >= col("order_purchase_timestamp")
    )
)

delivered = delivered.withColumn(
    "delivery_days",
    expr(
        "unix_timestamp(order_delivered_customer_date) "
        "- unix_timestamp(order_purchase_timestamp)"
    ) / 86400.0
)

print("Valid delivered orders:", delivered.count())

print("Overall delivery statistics:")
delivered.select(
    count("*").alias("valid_orders"),
    avg("delivery_days").alias("average_delivery_days")
).show()

print("Monthly delivery analytics:")
monthly = (
    delivered
    .withColumn("purchase_year", year("order_purchase_timestamp"))
    .withColumn("purchase_month", month("order_purchase_timestamp"))
    .groupBy("purchase_year", "purchase_month")
    .agg(
        count("*").alias("delivered_orders"),
        avg("delivery_days").alias("average_delivery_days")
    )
    .orderBy("purchase_year", "purchase_month")
)

monthly.show(50, truncate=False)

# Save results back to HDFS
monthly.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/ecommerce/analytics/monthly_delivery"
)

print("Monthly analytics saved to HDFS.")

spark.stop()
