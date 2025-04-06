

SELECT
     orders.order_id
    ,orders.customer_id 
    ,COALESCE(SUM(payments.amount) / 100, 0) AS amount
FROM {{ref('jaffle_shop_orders')}} AS orders 
    LEFT JOIN {{ref('stripe_payments')}} AS payments ON orders.order_id = payments.order_id
GROUP BY 
     orders.order_id
    ,orders.customer_id