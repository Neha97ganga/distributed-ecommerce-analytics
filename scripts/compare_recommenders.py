from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window
from pyspark.ml.recommendation import ALS

spark = (
    SparkSession.builder
    .appName("CompareEcommerceRecommenders")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/ecommerce/processed/"
output = "hdfs://localhost:9000/ecommerce/recommendations/"

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
    interactions.join(user_map, "customer_id")
    .join(product_map, "product_id")
    .select("user_id", "product_num", "rating")
)

train, test = data.randomSplit([0.8, 0.2], seed=42)
train.cache()
test.cache()

print("Training interactions:", train.count())
print("Test interactions:", test.count())

train_users = train.select("user_id").distinct()
train_products = train.select("product_num").distinct()

eligible_test = (
    test.join(train_users, "user_id", "left_semi")
    .join(train_products, "product_num", "left_semi")
    .select("user_id", "product_num")
    .distinct()
    .cache()
)

# Evaluate only users with held-out interactions and at least
# five unseen training-catalog products available to recommend.
seen = train.select("user_id", "product_num").distinct()

eligible_users = eligible_test.select("user_id").distinct()

unseen_counts = (
    eligible_users.crossJoin(train_products)
    .join(seen, ["user_id", "product_num"], "left_anti")
    .groupBy("user_id")
    .agg(F.count("*").alias("available_items"))
)

eval_users = (
    unseen_counts.filter(F.col("available_items") >= 5)
    .select("user_id")
    .cache()
)

eligible_test = (
    eligible_test.join(eval_users, "user_id", "left_semi").cache()
)

print("Shared evaluation users:", eval_users.count())
print("Eligible held-out interactions:", eligible_test.count())

# Popularity baseline: training data only
popularity = (
    train.groupBy("product_num")
    .agg(F.sum("rating").alias("popularity"))
)

pop_candidates = (
    eval_users.crossJoin(popularity)
    .join(seen, ["user_id", "product_num"], "left_anti")
)

pop_window = Window.partitionBy("user_id").orderBy(
    F.desc("popularity"), F.asc("product_num")
)

pop_top5 = (
    pop_candidates
    .withColumn("position", F.row_number().over(pop_window))
    .filter(F.col("position") <= 5)
    .select("user_id", "product_num")

)
pop_top5.write.mode("overwrite").parquet(
    output + "comparison_popularity_top5"
)
# ALS model, trained on exactly the same training data
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

# Request more candidates, then exclude previously seen items
als_candidates = model.recommendForUserSubset(
    eval_users, 100
)

als_exploded = (
    als_candidates
    .select("user_id", F.explode("recommendations").alias("rec"))
    .select(
        "user_id",
        F.col("rec.product_num").alias("product_num"),
        F.col("rec.rating").alias("score")
    )
    .join(seen, ["user_id", "product_num"], "left_anti")
)

als_window = Window.partitionBy("user_id").orderBy(F.desc("score"))

als_top5 = (
    als_exploded
    .withColumn("position", F.row_number().over(als_window))
    .filter(F.col("position") <= 5)
    .select("user_id", "product_num")
)
als_top5.write.mode("overwrite").parquet(
    output + "comparison_als_top5"
)

def evaluate(name, recommendations):
    hits = (
        recommendations
        .join(eligible_test, ["user_id", "product_num"], "inner")
        .groupBy("user_id")
        .agg(F.count("*").alias("hits"))
    )

    counts = (
        recommendations.groupBy("user_id")
        .agg(F.count("*").alias("recommended"))
    )

    per_user = (
        eval_users
        .join(counts, "user_id", "left")
        .join(hits, "user_id", "left")
        .fillna({"recommended": 0, "hits": 0})
        .withColumn(
            "precision",
            F.when(
                F.col("recommended") > 0,
                F.col("hits") / F.col("recommended")
            ).otherwise(0.0)
        )
        .withColumn(
            "recall",
            F.col("hits") / F.lit(1.0)
        )
    )
    print(f"\n{name} - total hits:")
    print(per_user.agg(F.sum("hits")).first()[0])

    print(f"{name} - users grouped by hit count:")
    per_user.groupBy("hits").count().orderBy("hits").show()

    print(f"{name} - users grouped by recall:")
    per_user.groupBy("recall").count().orderBy("recall").show(20)
    # Calculate recall using each user's total held-out relevant items.
    relevant = eligible_test.groupBy("user_id").agg(
        F.count("*").alias("relevant")
    )
    print(f"\n{name}: relevant-item counts among hit users")
    per_user.filter(F.col("hits") > 0) \
        .groupBy("relevant").count().orderBy("relevant").show()

    print(f"{name}: recommendation counts per user")
    per_user.groupBy("recommended").count().orderBy("recommended").show()
    per_user = (
        per_user.drop("recall")
        .join(relevant, "user_id", "inner")
        .withColumn("recall", F.col("hits") / F.col("relevant"))
    )

    row = per_user.agg(
        F.avg("precision").alias("precision_at_5"),
        F.avg("recall").alias("recall_at_5")
    ).first()

    print(
        f"{name}: Precision@5={row['precision_at_5']:.6f}, "
        f"Recall@5={row['recall_at_5']:.6f}"
    )
    return (name, float(row["precision_at_5"]), float(row["recall_at_5"]))
pop_hit_users = (
    pop_top5.join(
        eligible_test.select("user_id", "product_num"),
        ["user_id", "product_num"],
        "inner"
    )
    .select("user_id").distinct()
)

als_hit_users = (
    als_top5.join(
        eligible_test.select("user_id", "product_num"),
        ["user_id", "product_num"],
        "inner"
    )
    .select("user_id").distinct()
)

print("\n===== HIT USER COMPARISON =====")
print("Popularity hit users:", pop_hit_users.count())
print("ALS hit users:", als_hit_users.count())
print(
    "Same users hit by both:",
    pop_hit_users.intersect(als_hit_users).count()
)
print(
    "Popularity-only hit users:",
    pop_hit_users.subtract(als_hit_users).count()
)
print(
    "ALS-only hit users:",
    als_hit_users.subtract(pop_hit_users).count()
)
results = [
    evaluate("Popularity", pop_top5),
    evaluate("ALS", als_top5)
]

summary = spark.createDataFrame(
    results, ["model", "precision_at_5", "recall_at_5"]
)

summary.show(truncate=False)
summary.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(output + "model_comparison")

print("Saved comparison to:", output + "model_comparison")

train.unpersist()
test.unpersist()
eval_users.unpersist()
eligible_test.unpersist()
spark.stop()
