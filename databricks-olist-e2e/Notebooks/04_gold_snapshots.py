# Databricks notebook source
# Gold Snapshot: materialize DLT LIVE VIEWs to physical Delta tables for BI
CATALOG, SCHEMA = "olist", "dlt"

views_to_snap = [
    ("gold_kpi_orders",       "gold_kpi_orders_snap"),
    ("gold_kpi_reviews",      "gold_kpi_reviews_snap"),
    ("gold_rev_by_category",  "gold_rev_by_category_snap"),
    ("gold_conversion",       "gold_conversion_snap"),
]

for view_name, table_name in views_to_snap:
    src = f"{CATALOG}.{SCHEMA}.{view_name}"
    dst = f"{CATALOG}.{SCHEMA}.{table_name}"
    df = spark.table(src)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dst)
    print(f"Snapshotted {src} -> {dst} ({df.count()} rows)")

print("Gold snapshots refreshed.")
