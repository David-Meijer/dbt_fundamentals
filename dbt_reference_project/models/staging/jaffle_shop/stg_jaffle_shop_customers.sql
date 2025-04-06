

SELECT
     ID         AS customer_id
    ,FIRST_NAME AS first_name
    ,LAST_NAME  AS last_name 
FROM {{source('jaffle_shop', 'jaffle_shop_customers')}}