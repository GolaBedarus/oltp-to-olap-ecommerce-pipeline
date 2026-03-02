TRUNCATE analytics.fact_sales RESTART IDENTITY;

INSERT INTO analytics.fact_sales (
  order_id, order_item_id, date_key, customer_key, product_key,
  quantity, price, freight_value, revenue, order_status
)
SELECT
  oi.order_id,
  oi.order_item_id,
  (EXTRACT(YEAR FROM (o.order_purchase_timestamp::timestamp))::int * 10000
    + EXTRACT(MONTH FROM (o.order_purchase_timestamp::timestamp))::int * 100
    + EXTRACT(DAY FROM (o.order_purchase_timestamp::timestamp))::int) AS date_key,
  dc.customer_key,
  dp.product_key,
  1 AS quantity,
  oi.price::numeric,
  oi.freight_value::numeric,
  (oi.price::numeric * 1) AS revenue,
  o.order_status
FROM oltp.order_items oi
JOIN oltp.orders o
  ON o.order_id = oi.order_id
JOIN analytics.dim_customer dc
  ON dc.customer_id = o.customer_id
JOIN analytics.dim_product dp
  ON dp.product_id = oi.product_id
ON CONFLICT (order_id, order_item_id) DO NOTHING;