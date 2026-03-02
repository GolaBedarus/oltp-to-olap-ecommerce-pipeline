-- quality_gate.sql
-- Si cualquiera de estos checks encuentra problemas, Airflow debe fallar.

-- 1) Fact table no puede estar vacía
DO $$
BEGIN
  IF (SELECT COUNT(*) FROM analytics.fact_sales) = 0 THEN
    RAISE EXCEPTION 'DQ FAIL: analytics.fact_sales está vacía';
  END IF;
END $$;

-- 2) No NULL keys en fact
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM analytics.fact_sales
    WHERE date_key IS NULL OR customer_key IS NULL OR product_key IS NULL
  ) THEN
    RAISE EXCEPTION 'DQ FAIL: NULL keys en analytics.fact_sales';
  END IF;
END $$;

-- 3) No duplicados por (order_id, order_item_id)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM analytics.fact_sales
    GROUP BY order_id, order_item_id
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'DQ FAIL: duplicados en (order_id, order_item_id)';
  END IF;
END $$;

-- 4) Revenue no negativo
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM analytics.fact_sales
    WHERE revenue < 0
  ) THEN
    RAISE EXCEPTION 'DQ FAIL: revenue negativo detectado';
  END IF;
END $$;

-- 5) Dimensiones no vacías
DO $$
BEGIN
  IF (SELECT COUNT(*) FROM analytics.dim_customer) = 0 THEN
    RAISE EXCEPTION 'DQ FAIL: dim_customer vacía';
  END IF;

  IF (SELECT COUNT(*) FROM analytics.dim_product) = 0 THEN
    RAISE EXCEPTION 'DQ FAIL: dim_product vacía';
  END IF;

  IF (SELECT COUNT(*) FROM analytics.dim_date) = 0 THEN
    RAISE EXCEPTION 'DQ FAIL: dim_date vacía';
  END IF;
END $$;