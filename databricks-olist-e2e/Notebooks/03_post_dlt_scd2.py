# Databricks notebook source
# MAGIC %run "/Users/ahmadniyzar@gmail.com/Olist_Project/02_scd2_helpers"

# COMMAND ----------

CATALOG, SCHEMA = "olist", "dlt"
silver = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

# Stage from silver
stg_customer = (spark.table(silver("silver_customers"))
    .select("customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"))

stg_product  = (spark.table(silver("silver_products"))
    .select("product_id","category_en","weight_g","length_cm","height_cm","width_cm"))

stg_seller   = (spark.table(silver("silver_sellers"))
    .select("seller_id","seller_zip_code_prefix","seller_city","seller_state"))

# SCD2 merges
scd2_merge(f"{CATALOG}.{SCHEMA}.dim_customer", stg_customer,
           business_keys=["customer_unique_id"],
           tracked_cols=["customer_zip_code_prefix","customer_city","customer_state"])

scd2_merge(f"{CATALOG}.{SCHEMA}.dim_product", stg_product,
           business_keys=["product_id"],
           tracked_cols=["category_en","weight_g","length_cm","height_cm","width_cm"])

scd2_merge(f"{CATALOG}.{SCHEMA}.dim_seller", stg_seller,
           business_keys=["seller_id"],
           tracked_cols=["seller_zip_code_prefix","seller_city","seller_state"])

print("SCD2 dimensions updated.")