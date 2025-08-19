-- Databricks notebook source
-- ========== FACTS ==========
CREATE LIVE TABLE fact_order_items AS
SELECT
  i.order_id, i.order_item_id, i.product_id, i.seller_id,
  i.price, i.freight_value,
  DATE(o.order_purchase_ts) AS order_date
FROM LIVE.silver_order_items i
JOIN LIVE.silver_orders o USING(order_id);

CREATE LIVE TABLE fact_payments AS
SELECT order_id, payment_type, payment_value
FROM LIVE.silver_payments;

CREATE LIVE TABLE fact_reviews AS
SELECT order_id, review_score, review_creation_dt
FROM LIVE.silver_reviews;

-- ========== GOLD (views for BI) ==========
CREATE LIVE VIEW gold_kpi_orders AS
SELECT
  COUNT(DISTINCT o.order_id)                                                  AS orders,
  SUM(i.price)                                                                AS gmv,
  SUM(i.price) / NULLIF(COUNT(DISTINCT o.order_id), 0)                        AS aov,
  AVG(DATEDIFF(o.delivered_customer_ts, o.order_approved_ts))                 AS avg_delivery_days,
  AVG(CASE WHEN CAST(o.delivered_customer_ts AS DATE) > o.estimated_delivery_dt THEN 1.0 ELSE 0.0 END) AS pct_late
FROM LIVE.silver_orders o
LEFT JOIN LIVE.silver_order_items i USING(order_id);

CREATE LIVE VIEW gold_kpi_reviews AS
SELECT AVG(review_score) AS avg_review_score
FROM LIVE.silver_reviews;

CREATE LIVE VIEW gold_rev_by_category AS
SELECT p.category_en, SUM(i.price) AS gmv
FROM LIVE.silver_order_items i
JOIN LIVE.silver_products p USING(product_id)
GROUP BY p.category_en
ORDER BY gmv DESC;

CREATE LIVE VIEW gold_conversion AS
WITH paid AS (SELECT COUNT(DISTINCT order_id) AS paid_orders FROM LIVE.silver_payments),
delivered AS (
  SELECT COUNT(DISTINCT order_id) AS delivered_orders
  FROM LIVE.silver_orders WHERE delivered_customer_ts IS NOT NULL
)
SELECT delivered_orders / NULLIF(paid_orders,0) AS paid_to_delivered_rate
FROM paid CROSS JOIN delivered;

-- Materialize KPI views as physical Delta tables for BI access
CREATE LIVE TABLE gold_kpi_orders_snap AS
SELECT * FROM LIVE.gold_kpi_orders;

CREATE LIVE TABLE gold_kpi_reviews_snap AS
SELECT * FROM LIVE.gold_kpi_reviews;

CREATE LIVE TABLE gold_rev_by_category_snap AS
SELECT * FROM LIVE.gold_rev_by_category;

CREATE LIVE TABLE gold_conversion_snap AS
SELECT * FROM LIVE.gold_conversion;
