
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round

spark = (
    SparkSession.builder
    .appName("EcommerceCategoryRevenue")
    .getOrCreate()
)

base = "hdfs://localhost:9000/ecommerce/processed/"

# Read the three datasets from HDFS
items = spark.read.parquet(base + "order_items.parquet")
orders = spark.read.parquet(base + "orders.parquet")
products = spark.read.parquet(base + "products.parquet")

# Keep delivered orders
delivered_orders = orders.filter(
    col("order_status") == "delivered"
).select("order_id")

# Join items to delivered orders, then to products
delivered_items = (
    items
    .join(delivered_orders, on="order_id", how="inner")
    .join(
        products.select("product_id", "product_category_name"),
        on="product_id",
        how="left"
    )
)

# Revenue here means item price + freight, not profit
category_revenue = (
    delivered_items
    .withColumn(
        "category",
        col("product_category_name")
        if "product_category_name" in delivered_items.columns
        else col("product_id")
    )
    .groupBy("category")
    .agg(
        spark_round(
            spark_sum(col("price") + col("freight_value")), 2
        ).alias("revenue"),
        spark_sum(col("order_item_id") * 0 + 1).alias("item_count")
    )
    .orderBy(col("revenue").desc())
)

category_revenue.show(20, truncate=False)

output = "hdfs://localhost:9000/ecommerce/analytics/category_revenue"
category_revenue.write.mode("overwrite").parquet(output)

print("Saved category analytics to:", output)

spark.stop()
