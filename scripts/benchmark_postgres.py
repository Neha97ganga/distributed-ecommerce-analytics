import json
import subprocess
import statistics

query = """
SELECT
    p.product_category_name AS category,
    SUM(i.price + i.freight_value) AS revenue,
    COUNT(*) AS item_count
FROM orders o
JOIN order_items i ON o.order_id = i.order_id
JOIN products p ON i.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_name
"""

times = []

for run in range(7):
    sql = "EXPLAIN (ANALYZE, FORMAT JSON) " + query

    result = subprocess.run(
        [
            "sudo", "-u", "postgres",
            "psql", "-X", "-qAt",
            "-d", "ecommerce", "-c", sql
        ],
        capture_output=True,
        text=True,
        check=True
    )

    plan = json.loads(result.stdout)[0]
    execution_ms = plan["Execution Time"]

    print(f"Run {run + 1}: {execution_ms:.3f} ms")

    if run > 0:
        times.append(execution_ms)

print("\nPostgreSQL benchmark (excluding first run)")
print(f"Runs measured: {len(times)}")
print(f"Average: {statistics.mean(times):.3f} ms")
print(f"Median:  {statistics.median(times):.3f} ms")
print(f"Min:     {min(times):.3f} ms")
print(f"Max:     {max(times):.3f} ms")
