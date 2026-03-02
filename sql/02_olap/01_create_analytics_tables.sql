CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_date (
  date_key INTEGER PRIMARY KEY,
  full_date DATE UNIQUE NOT NULL,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL,
  day_of_week INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_product (
  product_key BIGSERIAL PRIMARY KEY,
  product_id TEXT UNIQUE NOT NULL,
  product_category_name TEXT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
  customer_key BIGSERIAL PRIMARY KEY,
  customer_id TEXT UNIQUE NOT NULL,
  customer_unique_id TEXT,
  customer_city TEXT,
  customer_state TEXT
);

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
  sales_key BIGSERIAL PRIMARY KEY,
  order_id TEXT NOT NULL,
  order_item_id INT NOT NULL,
  date_key INTEGER NOT NULL,
  customer_key BIGINT NOT NULL,
  product_key BIGINT NOT NULL,
  quantity INT NOT NULL,
  price NUMERIC,
  freight_value NUMERIC,
  revenue NUMERIC,
  order_status TEXT,

  CONSTRAINT uq_sales_line UNIQUE (order_id, order_item_id),
  CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES analytics.dim_date(date_key),
  CONSTRAINT fk_customer FOREIGN KEY (customer_key) REFERENCES analytics.dim_customer(customer_key),
  CONSTRAINT fk_product FOREIGN KEY (product_key) REFERENCES analytics.dim_product(product_key)
);