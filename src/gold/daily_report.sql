DROP TABLE IF EXISTS gold.upsell.daily_report;
CREATE TABLE IF NOT EXISTS gold.upsell.daily_report AS (

SELECT 
    DATE(t1.transaction_date) AS date,
    t2.product_name as product_name,
    COUNT(DISTINCT t1.transaction_id) AS n_transactions,
    COUNT(DISTINCT t1.customer_id) AS n_customers,
    SUM(t1.transaction_points) AS n_points,
    SUM(CASE WHEN t1.transaction_points > 0 THEN t1.transaction_points ELSE 0 END) AS n_positive_points,
    SUM(CASE WHEN t1.transaction_points < 0 THEN t1.transaction_points ELSE 0 END) AS n_negative_points

FROM silver.upsell.transactions AS t1

LEFT JOIN silver.upsell.transactions_product AS t2
ON t1.transaction_id = t2.transaction_id

--WHERE DATE(t1.transaction_date) = '{dt_ref}'

GROUP BY date, product_name GROUPING SETS ((date, product_name), (date))
ORDER BY date

)