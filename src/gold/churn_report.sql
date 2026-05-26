WITH tb_new AS (

  SELECT DISTINCT
        date('{dt_ref}')AS dtRef,
        t1.customer_id

  FROM silver.upsell.transactions AS t1

  WHERE DATE(t1.transaction_date) <= '{dt_ref}'
  AND DATE(t1.transaction_date) > '{dt_ref}' - INTERVAL 28 DAY

),

tb_old AS (

  SELECT DISTINCT
        date('{dt_ref}' - INTERVAL 28 DAY)AS dtRef,
        t1.customer_id

  FROM silver.upsell.transactions AS t1

  WHERE DATE(t1.transaction_date) <= '{dt_ref}' - INTERVAL 28 DAY
  AND DATE(t1.transaction_date) > '{dt_ref}' - INTERVAL 56 DAY
  
)

select date('{dt_ref}') AS dtRef,
       count(t1.customer_id) AS qtdeBaseOld,
       count(t2.customer_id) AS qtdeBaseNewNotChurn,
       count(t1.customer_id) - count(t2.customer_id)  AS nrQtdeChurn,
       1 - count(t2.customer_id) / count(t1.customer_id) AS ChurnRate

FROM tb_old as t1

LEFT JOIN tb_new AS t2
ON t1.customer_id = t2.customer_id

GROUP BY ALL