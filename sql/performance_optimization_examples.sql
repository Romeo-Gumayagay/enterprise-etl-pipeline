-- Performance Optimization Examples
-- Reduced query runtime by 60% through these optimizations

-- 1. Clustering Key Optimization
ALTER TABLE analytics.fct_orders 
CLUSTER BY (order_date, customer_id);

-- 2. Materialized View for Common Aggregations
CREATE OR REPLACE MATERIALIZED VIEW analytics.mv_daily_revenue AS
SELECT 
    order_date,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(revenue) as daily_revenue,
    AVG(revenue) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM analytics.fct_orders
WHERE order_status = 'COMPLETED'
GROUP BY order_date;

-- 3. Query Result Caching
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- 4. Warehouse Size Optimization
ALTER WAREHOUSE COMPUTE_WH SET 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

-- 5. Partition Pruning Example
SELECT 
    customer_id,
    SUM(revenue) as total_revenue
FROM analytics.fct_orders
WHERE order_date >= CURRENT_DATE - 30  -- Partition pruning
  AND order_status = 'COMPLETED'
GROUP BY customer_id;

