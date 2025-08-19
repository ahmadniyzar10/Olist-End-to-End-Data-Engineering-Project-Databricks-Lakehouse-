-- Databricks notebook source
-- =========================
-- BRONZE LAYER
-- =========================

CREATE OR REFRESH LIVE TABLE bronze_orders
  TBLPROPERTIES (
    "quality" = "expectations",
    "expectations.valid_purchase_ts" = "order_purchase_timestamp IS NOT NULL",
    "expectations.valid_status" = "order_status IN ('delivered','shipped','invoiced','canceled','unavailable','processing','created','approved')"
  )
AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'olist_orders_dataset.csv'
);

CREATE OR REFRESH LIVE TABLE bronze_order_items AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'olist_order_items_dataset.csv'
);

CREATE OR REFRESH LIVE TABLE bronze_payments AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'olist_order_payments_dataset.csv'
);

CREATE OR REFRESH LIVE TABLE bronze_reviews AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'olist_order_reviews_dataset.csv'
);

CREATE OR REFRESH LIVE TABLE bronze_customers AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'olist_customers_dataset.csv'
);

CREATE OR REFRESH LIVE TABLE bronze_products AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'olist_products_dataset.csv'
);

CREATE OR REFRESH LIVE TABLE bronze_sellers AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'olist_sellers_dataset.csv'
);

CREATE OR REFRESH LIVE TABLE bronze_categories AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'product_category_name_translation.csv'
);

CREATE OR REFRESH LIVE TABLE bronze_geolocation AS
SELECT *
FROM read_files(
  '/Volumes/olist/source/raw_data/',
  format => 'csv',
  header => true,
  inferSchema => true,
  fileNamePattern => 'olist_geolocation_dataset.csv'
);

-- =========================
-- SILVER LAYER
-- =========================

CREATE OR REFRESH LIVE TABLE silver_orders AS
SELECT
  order_id,
  customer_id,
  order_status,
  CAST(order_purchase_timestamp AS TIMESTAMP)              AS order_purchase_ts,
  CAST(order_approved_at AS TIMESTAMP)                     AS order_approved_ts,
  CAST(order_delivered_carrier_date AS TIMESTAMP)          AS delivered_carrier_ts,
  CAST(order_delivered_customer_date AS TIMESTAMP)         AS delivered_customer_ts,
  CAST(order_estimated_delivery_date AS DATE)              AS estimated_delivery_dt
FROM LIVE.bronze_orders;

CREATE OR REFRESH LIVE TABLE silver_order_items AS
SELECT
  order_id,
  order_item_id,
  product_id,
  seller_id,
  CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
  CAST(price AS DOUBLE) AS price,
  CAST(freight_value AS DOUBLE) AS freight_value
FROM LIVE.bronze_order_items;

CREATE OR REFRESH LIVE TABLE silver_payments AS
SELECT
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  CAST(payment_value AS DOUBLE) AS payment_value
FROM LIVE.bronze_payments;

CREATE OR REFRESH LIVE TABLE silver_reviews AS
SELECT
  review_id,
  order_id,
  review_score,
  CAST(review_creation_date AS DATE)         AS review_creation_dt,
  CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_ts
FROM LIVE.bronze_reviews;

CREATE OR REFRESH LIVE TABLE silver_customers AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  TRIM(LOWER(customer_city))  AS customer_city,
  UPPER(customer_state)       AS customer_state
FROM LIVE.bronze_customers;

CREATE OR REFRESH LIVE TABLE silver_products AS
SELECT
  p.product_id,
  COALESCE(t.product_category_name_english, p.product_category_name) AS category_en,
  CAST(product_weight_g AS INT)  AS weight_g,
  CAST(product_length_cm AS INT) AS length_cm,
  CAST(product_height_cm AS INT) AS height_cm,
  CAST(product_width_cm AS INT)  AS width_cm
FROM LIVE.bronze_products p
LEFT JOIN LIVE.bronze_categories t
  ON p.product_category_name = t.product_category_name;

CREATE OR REFRESH LIVE TABLE silver_sellers AS
SELECT
  seller_id,
  seller_zip_code_prefix,
  TRIM(LOWER(seller_city)) AS seller_city,
  UPPER(seller_state)      AS seller_state
FROM LIVE.bronze_sellers;

CREATE OR REFRESH LIVE TABLE silver_geolocation AS
SELECT
  geolocation_zip_code_prefix,
  ROUND(CAST(geolocation_lat AS DOUBLE), 6) AS lat,
  ROUND(CAST(geolocation_lng AS DOUBLE), 6) AS lng,
  TRIM(LOWER(geolocation_city)) AS city,
  UPPER(geolocation_state)      AS state
FROM LIVE.bronze_geolocation;
