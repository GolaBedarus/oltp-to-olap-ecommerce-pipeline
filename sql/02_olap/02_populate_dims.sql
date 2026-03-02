-- Dim Date
TRUNCATE
  analytics.fact_sales,
  analytics.dim_date,
  analytics.dim_product,
  analytics.dim_customer
RESTART IDENTITY;

INSERT INTO analytics.dim_date (date_key, full_date, year, month, day, day_of_week)
SELECT
  (EXTRACT(YEAR FROM d)::int * 10000 + EXTRACT(MONTH FROM d)::int * 100 + EXTRACT(DAY FROM d)::int) AS date_key,
  d::date AS full_date,
  EXTRACT(YEAR FROM d)::int AS year,
  EXTRACT(MONTH FROM d)::int AS month,
  EXTRACT(DAY FROM d)::int AS day,
  EXTRACT(DOW FROM d)::int AS day_of_week
FROM generate_series(
  (SELECT MIN((order_purchase_timestamp::timestamp))::date FROM oltp.orders),
  (SELECT MAX((order_purchase_timestamp::timestamp))::date FROM oltp.orders),
  interval '1 day'
) AS d
ON CONFLICT (full_date) DO NOTHING;

-- Dim Product


INSERT INTO analytics.dim_product (product_id, product_category_name, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
SELECT
  product_id,
  product_category_name,
  product_weight_g::int,
  product_length_cm::int,
  product_height_cm::int,
  product_width_cm::int
FROM oltp.products
ON CONFLICT (product_id) DO NOTHING;

-- Dim Customer


INSERT INTO analytics.dim_customer (customer_id, customer_unique_id, customer_city, customer_state)
SELECT
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state
FROM oltp.customers
ON CONFLICT (customer_id) DO NOTHING;