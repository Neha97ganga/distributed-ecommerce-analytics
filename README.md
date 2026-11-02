# Distributed E-Commerce Analytics and Recommendation System

## 1. Project Overview

This project implements a data analytics and product recommendation pipeline
using HDFS, Apache Spark, PostgreSQL, and ALS collaborative filtering.

The project processes e-commerce order data, calculates category-level
revenue, validates analytical results against PostgreSQL, benchmarks query
execution, and generates personalized product recommendations.

## 2. Technology Stack

- Apache Hadoop HDFS: distributed storage for processed Parquet datasets
- Apache Spark 3.5.7: data processing and analytics
- PySpark MLlib: ALS collaborative-filtering model
- PostgreSQL 16: SQL analytics, validation, and query optimization
- Python and Matplotlib: scripts and visualizations
- WSL/Linux: development environment

## 3. Data Processing

Processed datasets are stored in:

`hdfs://localhost:9000/ecommerce/processed/`

Datasets include customers, orders, order items, payments, reviews,
products, sellers, and category translations.

## 4. Category Revenue Analytics

The Spark job joins delivered orders with order items and products,
then calculates category-level revenue using:

`SUM(price + freight_value)`

This metric represents item price plus freight, not profit.

The analytical output contains 74 categories.

Top categories by reported revenue:

| Category | Revenue |
|---|---:|
| beleza_saude | 1,412,089.53 |
| relogios_presentes | 1,264,333.12 |
| cama_mesa_banho | 1,225,209.26 |
| esporte_lazer | 1,118,256.91 |
| informatica_acessorios | 1,032,723.77 |

## 5. PostgreSQL Validation and Optimization

The category revenue results were compared between Spark and PostgreSQL.

- Categories in PostgreSQL: 74
- Categories in Spark: 74
- Mismatched categories: 0
- Validation result: PASS

Indexes were added to join-key columns and table statistics were updated.

Reported PostgreSQL benchmark averages:

| Configuration | Average execution time |
|---|---:|
| Before indexes | 165.185 ms |
| After indexes | 145.866 ms |

This represents an observed reduction of approximately 11.7% in average
execution time for this local workload. Results may vary with caching,
system load, and execution conditions.

The Spark and PostgreSQL timings use different timing boundaries, so they
should not be interpreted as a perfectly controlled performance comparison.

## 6. Product Recommendation System

The recommendation pipeline uses ALS collaborative filtering on
customer-product interactions from delivered orders.

- Training/test split: 80% / 20%
- Random split seed: 42
- ALS rank: 10
- Maximum iterations: 10
- Regularization parameter: 0.1
- Implicit feedback mode: enabled

Recommendations are saved to HDFS, including readable customer and
product identifiers.

## 7. Recommendation Evaluation

Both recommendation models were evaluated using the same training/test
split and the same eligible evaluation users.

Evaluation details:

- Training interactions: 79,827
- Test interactions: 19,958
- Shared evaluation users: 1,393
- Eligible held-out interactions: 1,451

### Model Performance

| Model | Precision@5 | Recall@5 | Total Hits |
|---|---:|---:|---:|
| Popularity Baseline | 0.007179 | 0.033381 | 50 |
| ALS Collaborative Filtering | 0.009189 | 0.045585 | 64 |

ALS achieved approximately 28.0% higher Precision@5 and 36.6% higher
Recall@5 than the popularity baseline in this evaluation.

Both models were evaluated on the same held-out interactions.
These results represent one random train/test split and do not establish
general superiority. A time-based evaluation would provide a more
realistic simulation of future purchases.

## 8. Output Locations

Processed data:
`/ecommerce/processed/`

Category revenue analytics:
`/ecommerce/analytics/category_revenue`

Readable ALS recommendations:
`/ecommerce/recommendations/top5_readable`

Popularity recommendations:
`/ecommerce/recommendations/popularity_top5`

Shared evaluation metrics:
`/ecommerce/recommendations/model_comparison`

Category revenue with English names:
`/ecommerce/analytics/category_revenue_enriched`

Monthly delivery analytics:
`/ecommerce/analytics/monthly_delivery` (from `ecommerce_orders_analytics.py`)
and `/ecommerce/analytics/monthly_delivery_v2` (from `monthly_delivery_spark.py`)

## 9. Visualizations

Charts are generated in:

`~/ecommerce_project_charts/`

Current charts include category revenue, PostgreSQL index benchmarking,
and recommendation evaluation metrics.

## 10. Running the Scripts

Example commands from the development environment:

```bash
spark-submit scripts/category_revenue_spark.py
spark-submit scripts/product_recommender.py
spark-submit scripts/popularity_baseline.py
spark-submit scripts/compare_recommenders.py
```

Some scripts depend on the WSL Spark installation and the configured
Python environment. Set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON`
to the environment containing the required dependencies before running
Spark ML scripts.

## 11. Limitations and Future Work

- The ALS lead over popularity is small (64 vs 50 hits on 1,451 eligible
  held-out interactions); repeat the evaluation over several random splits
  before drawing conclusions.
- Inspect ranking overlap between ALS and popularity recommendations.
- Explore ALS hyperparameter tuning after validating the evaluation.
- Consider time-based train/test splitting for future-purchase evaluation.
- Add more visualizations and automate the end-to-end pipeline.
- Improve reproducibility by documenting memory requirements and dependencies.

## Project Scripts

The executable Python scripts are located in the `scripts/` directory.

| Script | Purpose |
|---|---|
| `category_revenue_spark.py` | Distributed category revenue analytics using Spark |
| `category_revenue_enriched.py` | Extended category revenue analysis |
| `monthly_delivery_spark.py` | Monthly delivery analytics |
| `ecommerce_orders_analytics.py` | Order analytics with monthly delivery statistics (overlaps with `monthly_delivery_spark.py`) |
| `dataset_stats.py` | Dataset statistics |
| `product_recommender.py` | ALS collaborative-filtering recommendations |
| `popularity_baseline.py` | Popularity-based recommendation baseline |
| `compare_recommenders.py` | Recommendation evaluation using Precision@5 and Recall@5 |
| `benchmark_postgres.py` | PostgreSQL query benchmarking |
| `benchmark_spark.py` | Spark query benchmarking |
| `compare_category_results.py` | Validate category revenue results against PostgreSQL |
| `create_project_charts.py` | Generate project visualization charts |
| `export_benchmark_data.py` | Export benchmark data |

## Reproducibility Notes

- Hadoop/HDFS, Spark, PostgreSQL, Java, and Python must be configured before running the relevant scripts.
- The scripts may use local paths and HDFS paths configured for the development environment. Review those paths before running on another machine.
- Recommendation evaluation results apply to the current train-test split and should not be interpreted as proof that either recommendation method is universally superior.
- Benchmark measurements depend on the environment, caching, and execution conditions.

