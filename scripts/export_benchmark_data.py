from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ExportEcommerceBenchmarkData")
    .getOrCreate()
)

datasets = [
    "orders",
    "order_items",
    "products",
    "category_translation",
]

for name in datasets:
    source = f"hdfs://localhost:9000/ecommerce/processed/{name}.parquet"
    destination = f"file:///home/ganga/ecommerce_benchmark/csv/{name}"

    print(f"Exporting {name} from HDFS to CSV...")

    df = spark.read.parquet(source)

    # Use one output partition to create a single CSV data file.
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(destination)
    )

    print(f"Exported {name}: {df.count()} rows")

spark.stop()
print("All four datasets exported successfully.")
