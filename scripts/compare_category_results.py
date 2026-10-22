from pyspark.sql import SparkSession
import csv
import math

spark = SparkSession.builder \
    .appName("CompareCategoryResults") \
    .getOrCreate()

# Read Spark analytics output from HDFS
spark_df = spark.read.parquet(
    "hdfs://localhost:9000/ecommerce/analytics/category_revenue"
)

# Normalize missing category names
def normalize_category(category):
    if category is None or category == "":
        return None
    return category

# Store Spark results by category
spark_rows = {
    normalize_category(row["category"]): row
    for row in spark_df.collect()
}

# Read PostgreSQL results from CSV
POSTGRES_CSV = "/home/ganga/ecommerce_benchmark/postgres_category_revenue.csv"

with open(POSTGRES_CSV, newline="") as f:
    pg_rows = {
        normalize_category(row["category"]): row
        for row in csv.DictReader(f)
    }

print("PostgreSQL categories:", len(pg_rows))
print("Spark categories:", len(spark_rows))

# Compare category names
missing_in_pg = set(spark_rows.keys()) - set(pg_rows.keys())
missing_in_spark = set(pg_rows.keys()) - set(spark_rows.keys())

print("Missing from PostgreSQL:", missing_in_pg)
print("Missing from Spark:", missing_in_spark)

# Compare revenue and item counts
mismatches = []

for category in spark_rows:
    if category not in pg_rows:
        continue

    spark_revenue = float(spark_rows[category]["revenue"])
    pg_revenue = float(pg_rows[category]["revenue"])

    spark_count = int(spark_rows[category]["item_count"])
    pg_count = int(pg_rows[category]["item_count"])

    if not math.isclose(
        spark_revenue, pg_revenue,
        rel_tol=1e-9, abs_tol=0.01
    ) or spark_count != pg_count:
        mismatches.append({
            "category": category,
            "spark_revenue": spark_revenue,
            "postgres_revenue": pg_revenue,
            "spark_count": spark_count,
            "postgres_count": pg_count
        })

print("Mismatched categories:", len(mismatches))

for mismatch in mismatches:
    print(mismatch)

if not missing_in_pg and not missing_in_spark and not mismatches:
    print("\nVALIDATION PASSED: Spark and PostgreSQL results match.")
else:
    print("\nCHECK REQUIRED: Results differ.")

spark.stop()
