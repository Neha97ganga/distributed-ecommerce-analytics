from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import statistics

spark = (
    SparkSession.builder
    .appName("SparkRevenueBenchmark")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/ecommerce/processed/"

orders = spark.read.parquet(base + "orders.parquet")
items = spark.read.parquet(base + "order_items.parquet")
products = spark.read.parquet(base + "products.parquet")

# Run the same logical aggregation as PostgreSQL
def run_query():
    delivered = orders.filter(
        F.col("order_status") == "delivered"
    )

    joined = (
        delivered
        .join(items, "order_id")
        .join(products, "product_id")
    )

    result = (
        joined
        .groupBy("product_category_name")
        .agg(
            F.sum(
                F.col("price") + F.col("freight_value")
            ).alias("revenue"),
            F.count("*").alias("item_count")
        )
    )

    return result.collect()


times = []
expected_categories = None

for run in range(7):
    start = time.perf_counter()
    rows = run_query()
    elapsed_ms = (time.perf_counter() - start) * 1000

    print(f"Run {run + 1}: {elapsed_ms:.3f} ms")

    if expected_categories is None:
        expected_categories = len(rows)
    elif len(rows) != expected_categories:
        raise RuntimeError("Category count changed between runs")

    if run > 0:
        times.append(elapsed_ms)

print("\nSpark benchmark (excluding first run)")
print(f"Categories: {expected_categories}")
print(f"Runs measured: {len(times)}")
print(f"Average: {statistics.mean(times):.3f} ms")
print(f"Median:  {statistics.median(times):.3f} ms")
print(f"Min:     {min(times):.3f} ms")
print(f"Max:     {max(times):.3f} ms")

spark.stop()
