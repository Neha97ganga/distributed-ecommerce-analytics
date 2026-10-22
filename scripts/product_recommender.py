from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window
from pyspark.ml.recommendation import ALS

spark = (
    SparkSession.builder
    .appName("EcommerceProductRecommender")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/ecommerce/processed/"
output = "hdfs://localhost:9000/ecommerce/recommendations/"

# 1. Load the processed datasets
orders = spark.read.parquet(base + "orders.parquet")
items = spark.read.parquet(base + "order_items.parquet")

# 2. Build customer-product purchase interactions
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

# 3. Map original IDs to the integer IDs required by ALS
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

# 4. Split interactions into training and test sets
train, test = data.randomSplit([0.8, 0.2], seed=42)
train.cache()
test.cache()

print("Total interactions:", data.count())
print("Training interactions:", train.count())
print("Test interactions:", test.count())

# 5. Train the ALS collaborative-filtering model
als = ALS(
    userCol="user_id",
    itemCol="product_num",
    ratingCol="rating",
    rank=10,
    maxIter=10,
    regParam=0.1,
    coldStartStrategy="drop",
    implicitPrefs=True,
    seed=42
)

model = als.fit(train)

# 6. Prepare the test set for ranking evaluation.
# Evaluate only users and products represented in training.
train_users = train.select("user_id").distinct()
train_products = train.select("product_num").distinct()

eligible_test = (
    test
    .join(train_users, "user_id", "left_semi")
    .join(train_products, "product_num", "left_semi")
    .select("user_id", "product_num")
    .distinct()
    .cache()
)

# 7. Generate candidates and exclude products already purchased
# in the training set. Generate extra candidates so filtering
# still leaves enough products for a top-5 list.
candidate_recs = model.recommendForAllUsers(50)

exploded_recs = (
    candidate_recs
    .select(
        "user_id",
        F.explode("recommendations").alias("rec")
    )
    .select(
        "user_id",
        F.col("rec.product_num").alias("product_num"),
        F.col("rec.rating").alias("score")
    )
)

seen = train.select("user_id", "product_num").distinct()

unseen_recs = exploded_recs.join(
    seen, ["user_id", "product_num"], "left_anti"
)

rank_window = Window.partitionBy("user_id").orderBy(
    F.col("score").desc()
)

top5_numeric = (
    unseen_recs
    .withColumn("position", F.row_number().over(rank_window))
    .filter(F.col("position") <= 5)
    .cache()
)

# 8. Calculate Precision@5 and Recall@5
hits = (
    top5_numeric.select("user_id", "product_num")
    .join(eligible_test, ["user_id", "product_num"], "inner")
    .groupBy("user_id")
    .agg(F.count("*").alias("hits"))
)

recommended_counts = (
    top5_numeric.groupBy("user_id")
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

print("\n===== RECOMMENDER EVALUATION =====")
print("Evaluated users:", metrics["evaluated_users"])
print("Precision@5:", metrics["Precision@5"])
print("Recall@5:", metrics["Recall@5"])

# 9. Convert numeric product IDs back to original product IDs
readable_recs = (
    top5_numeric
    .join(user_map, "user_id")
    .join(product_map, "product_num")
    .select(
        "customer_id",
        "product_id",
        "position",
        F.col("score").alias("recommendation_score")
    )
    .orderBy("customer_id", "position")
)

# Save readable recommendations to HDFS
readable_recs.write.mode("overwrite").parquet(
    output + "top5_readable"
)

print("\nSaved readable recommendations to:", output + "top5_readable")
readable_recs.show(20, truncate=False)

train.unpersist()
test.unpersist()
eligible_test.unpersist()
top5_numeric.unpersist()
spark.stop()
