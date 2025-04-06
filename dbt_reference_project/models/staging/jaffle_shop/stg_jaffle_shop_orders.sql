

SELECT
     ID         AS order_id
    ,USER_ID    AS customer_id
    ,ORDER_DATE AS order_date
    ,STATUS     AS order_date 
FROM {{source('jaffle_shop', 'jaffle_shop_orders')}}