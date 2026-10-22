import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import csv
import os

out = os.path.expanduser("~/ecommerce_project_charts")
os.makedirs(out, exist_ok=True)

# Chart 1: Top 5 categories by revenue
categories = [
    "beleza_saude",
    "relogios_presentes",
    "cama_mesa_banho",
    "esporte_lazer",
    "informatica_acessorios",
]
revenue = [
    1412089.53,
    1264333.12,
    1225209.26,
    1118256.91,
    1032723.77,
]

plt.figure(figsize=(10, 6))
plt.barh(categories[::-1], revenue[::-1])
plt.xlabel("Revenue (price + freight)")
plt.title("Top 5 E-commerce Categories by Revenue")
plt.tight_layout()
plt.savefig(f"{out}/top_categories_revenue.png", dpi=160)
plt.close()

# Chart 2: PostgreSQL benchmark before and after indexes
labels = ["Before indexes", "After indexes"]
times = [165.185, 145.866]

plt.figure(figsize=(8, 5))
plt.bar(labels, times)
plt.ylabel("Average query time (ms)")
plt.title("PostgreSQL Query Optimization")
plt.tight_layout()
plt.savefig(f"{out}/postgres_index_benchmark.png", dpi=160)
plt.close()

# Chart 3: Shared recommender evaluation
models = ["Popularity", "ALS"]
precision = [0.0070404172, 0.0070404172]
recall = [0.0352020860, 0.0352020860]

plt.figure(figsize=(8, 5))
plt.bar(models, [x * 100 for x in precision])
plt.ylabel("Precision@5 (%)")
plt.title("Recommender Comparison — Shared Evaluation")
plt.tight_layout()
plt.savefig(f"{out}/recommender_precision.png", dpi=160)
plt.close()

plt.figure(figsize=(8, 5))
plt.bar(models, [x * 100 for x in recall])
plt.ylabel("Recall@5 (%)")
plt.title("Recommender Comparison — Shared Evaluation")
plt.tight_layout()
plt.savefig(f"{out}/recommender_recall.png", dpi=160)
plt.close()

print("Charts saved in:", out)
for name in sorted(os.listdir(out)):
    print(name)
