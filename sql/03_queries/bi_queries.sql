-- Total revenue
SELECT SUM(revenue) AS total_revenue
FROM analytics.fact_sales;

-- Revenue mensual
SELECT
  d.year,
  d.month,
  SUM(f.revenue) AS revenue
FROM analytics.fact_sales f
JOIN analytics.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Top 10 categorías por revenue
SELECT
  p.product_category_name,
  SUM(f.revenue) AS revenue
FROM analytics.fact_sales f
JOIN analytics.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10;