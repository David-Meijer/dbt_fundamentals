--amounts can not be negative

SELECT 
     payment_id
    ,SUM(amount) AS sum_amount
FROM {{ref('stg_stripe_payments')}}
GROUP BY payment_id
HAVING sum_amount < 0