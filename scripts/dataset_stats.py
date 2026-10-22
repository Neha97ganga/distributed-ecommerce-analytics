from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceDatasetStats")
    .getOrCreate()
)

base_path = "hdfs://localhost:9000/ecommerce/processed"

tables = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "category_translation"
]

print("\n===== E-COMMERCE DATASET STATISTICS =====")

for table in tables:
    df = spark.read.parquet(f"{base_path}/{table}.parquet")

    print(f"{table}: {df.count()} rows, {len(df.columns)} columns")

spark.stop()
