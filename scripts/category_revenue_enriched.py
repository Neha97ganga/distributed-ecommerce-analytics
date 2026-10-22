
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

spark = (
    SparkSession.builder
    .appName("CategoryRevenueEnriched")
    .getOrCreate()
)

revenue_path = (
    "hdfs://localhost:9000/ecommerce/analytics/category_revenue"
)

translation_path = (
    "hdfs://localhost:9000/ecommerce/processed/category_translation.parquet"
)

output_path = (
    "hdfs://localhost:9000/ecommerce/analytics/category_revenue_enriched"
)

# Read the existing category revenue results
revenue_df = spark.read.parquet(revenue_path)

# Read the Portuguese-to-English translation table
translation_df = spark.read.parquet(translation_path)

# Join using the original category name
enriched_df = (
    revenue_df.alias("r")
    .join(
        translation_df.alias("t"),
        col("r.category") == col("t.product_category_name"),
        "left"
    )
    .select(
        col("r.category"),
        coalesce(
            col("t.product_category_name_english"),
            col("r.category")
        ).alias("category_english"),
        col("r.revenue"),
        col("r.item_count")
    )
)

print("Total categories:", enriched_df.count())

print("Top 10 categories by revenue:")
enriched_df.orderBy(
    col("revenue").desc()
).show(10, truncate=False)

# Save to a NEW location; preserve the original dataset
enriched_df.write.mode("overwrite").parquet(output_path)

print("Saved enriched analytics to:", output_path)

spark.stop()
