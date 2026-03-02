-- Test 1: claves no nulas en fact
SELECT *
FROM analytics.fact_sales
WHERE date_key IS NULL OR customer_key IS NULL OR product_key IS NULL;

-- Test 2: no duplicados por línea de venta
SELECT order_id, order_item_id, COUNT(*) AS c
FROM analytics.fact_sales
GROUP BY 1,2
HAVING COUNT(*) > 1;

-- Test 3: revenue no negativo
SELECT *
FROM analytics.fact_sales
WHERE revenue < 0;