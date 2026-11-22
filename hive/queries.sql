-- Checks and aggregations on warehouse_orders.
-- Run with: hive -f queries.sql

SET hive.cli.print.header=true;

-- schema
DESCRIBE FORMATTED warehouse_orders;

-- row count (should be 3000)
SELECT COUNT(*) AS row_count FROM warehouse_orders;

-- sample rows
SELECT * FROM warehouse_orders LIMIT 10;

-- null check
SELECT
    SUM(CASE WHEN is_late        IS NULL THEN 1 ELSE 0 END) AS null_label,
    SUM(CASE WHEN order_date     IS NULL THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN distance_miles IS NULL THEN 1 ELSE 0 END) AS null_distance,
    SUM(CASE WHEN weight_lbs     IS NULL THEN 1 ELSE 0 END) AS null_weight
FROM warehouse_orders;

-- late rate by carrier
SELECT
    carrier,
    COUNT(*)                          AS orders,
    SUM(is_late)                      AS late_orders,
    ROUND(AVG(is_late) * 100, 1)      AS late_pct,
    ROUND(AVG(distance_miles), 0)     AS avg_miles
FROM warehouse_orders
GROUP BY carrier
ORDER BY late_pct DESC;

-- late rate by warehouse and peak season
SELECT
    warehouse_id,
    peak_season,
    COUNT(*)                          AS orders,
    ROUND(AVG(is_late) * 100, 1)      AS late_pct
FROM warehouse_orders
GROUP BY warehouse_id, peak_season
ORDER BY warehouse_id, peak_season;

-- order profile by service level
SELECT
    service_level,
    COUNT(*)                          AS orders,
    ROUND(AVG(order_lines), 1)        AS avg_lines,
    ROUND(AVG(weight_lbs), 1)         AS avg_weight_lbs,
    ROUND(AVG(promised_days), 1)      AS avg_promised_days,
    ROUND(AVG(is_late) * 100, 1)      AS late_pct
FROM warehouse_orders
GROUP BY service_level
ORDER BY orders DESC;

-- orders and late rate by month
SELECT
    MONTH(order_date)                 AS order_month,
    COUNT(*)                          AS orders,
    ROUND(AVG(is_late) * 100, 1)      AS late_pct
FROM warehouse_orders
GROUP BY MONTH(order_date)
ORDER BY order_month;
