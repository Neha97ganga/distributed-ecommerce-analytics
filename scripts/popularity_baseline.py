from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window

spark = (
    SparkSession.builder
    .appName("EcommercePopularityBaseline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/ecommerce/processed/"
output = "hdfs://localhost:9000/ecommerce/recommendations/"

# Rebuild exactly the same interaction data and ID mappings
orders = spark.read.parquet(base + "orders.parquet")
items = spark.read.parquet(base + "order_items.parquet")

interactions = (
    orders.filter(F.col("order_status") == "delivered")
    .select("order_id", "customer_id")
    .join(items.select("order_id", "product_id"), "order_id")
    .filter(
        F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
    )
    .groupBy("customer_id", "product_id")
    .agg(F.count("*").cast("float").alias("rating"))
)

user_map = interactions.select("customer_id").distinct().withColumn(
    "user_id",
    (F.row_number().over(Window.orderBy("customer_id")) - 1).cast("int")
)

product_map = interactions.select("product_id").distinct().withColumn(
    "product_num",
    (F.row_number().over(Window.orderBy("product_id")) - 1).cast("int")
)

data = (
    interactions
    .join(user_map, "customer_id")
    .join(product_map, "product_id")
    .select("user_id", "product_num", "rating")
)

train, test = data.randomSplit([0.8, 0.2], seed=42)

# Use the same held-out users and products eligible for ALS evaluation
train_users = train.select("user_id").distinct()
train_products = train.select("product_num").distinct()

eligible_test = (
    test
    .join(train_users, "user_id", "left_semi")
    .join(train_products, "product_num", "left_semi")
    .select("user_id", "product_num")
    .distinct()
)

# Popularity is calculated ONLY from training interactions
popularity = (
    train.groupBy("product_num")
    .agg(F.sum("rating").alias("popularity"))
    .orderBy(F.desc("popularity"), F.asc("product_num"))
)

# Exclude products each user already purchased in training
seen = train.select("user_id", "product_num").distinct()

users = eligible_test.select("user_id").distinct()

candidates = (
    users.crossJoin(popularity)
    .join(seen, ["user_id", "product_num"], "left_anti")
)

rank_window = Window.partitionBy("user_id").orderBy(
    F.desc("popularity"), F.asc("product_num")
)

top5 = (
    candidates
    .withColumn("position", F.row_number().over(rank_window))
    .filter(F.col("position") <= 5)
    .select("user_id", "product_num", "position")
)

# Calculate metrics on the same eligible held-out interactions
hits = (
    top5.select("user_id", "product_num")
    .join(eligible_test, ["user_id", "product_num"], "inner")
    .groupBy("user_id")
    .agg(F.count("*").alias("hits"))
)

recommended_counts = (
    top5.groupBy("user_id")
    .agg(F.count("*").alias("num_recommended"))
)

relevant_counts = (
    eligible_test.groupBy("user_id")
    .agg(F.count("*").alias("num_relevant"))
)

per_user = (
    relevant_counts
    .join(recommended_counts, "user_id", "left")
    .join(hits, "user_id", "left")
    .fillna({"num_recommended": 0, "hits": 0})
    .filter(F.col("num_recommended") > 0)
    .withColumn(
        "precision_at_5",
        F.col("hits") / F.col("num_recommended")
    )
    .withColumn(
        "recall_at_5",
        F.col("hits") / F.col("num_relevant")
    )
)

metrics = per_user.agg(
    F.count("*").alias("evaluated_users"),
    F.avg("precision_at_5").alias("Precision@5"),
    F.avg("recall_at_5").alias("Recall@5")
).first()

print("\n===== POPULARITY BASELINE =====")
print("Evaluated users:", metrics["evaluated_users"])
print("Precision@5:", metrics["Precision@5"])
print("Recall@5:", metrics["Recall@5"])

# Save baseline results for inspection
(
    top5.join(user_map, "user_id")
    .join(product_map, "product_num")
    .select("customer_id", "product_id", "position")
    .write.mode("overwrite").parquet(output + "popularity_top5")
)

print("Saved baseline recommendations to:", output + "popularity_top5")

spark.stop()
