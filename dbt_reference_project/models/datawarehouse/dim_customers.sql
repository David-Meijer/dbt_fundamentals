--model for dwh_dim_customers

WITH customers AS (
    SELECT 
         customer_id
        ,first_name
        ,last_name
    FROM {{ref('jaffle_shop_customers')}}
)
,
orders AS (
    SELECT 
         order_id
        ,customer_id
        ,order_date
        ,status
    FROM {{ref('jaffle_shop_orders')}}
)
,
customer_orders AS (
    SELECT
        customer_id
        ,MIN(order_date) AS first_order_date
        ,MAX(order_date) AS most_recent_order_date
        ,COUNT(order_id) AS number_of_orders
    FROM orders
    GROUP BY customer_id
)
,
--Calculating the lifetime value per customer using the orders again and keeping only records where payment is 'success' 
--This is done isolated as the payments table can have more than 1 record per order_id.
customer_lifetime_value AS (
    SELECT
         orders.customer_id
        ,SUM(COALESCE(payments.amount, 0) / 100) AS lifetime_value
    FROM orders
        LEFT JOIN {{ref('stripe_payments')}} AS payments ON orders.order_id = payments.order_id
    WHERE 1=1
        AND payments.status == 'success'
    GROUP BY orders.customer_id
)
,
dim_customer AS(
    SELECT 
         customers.customer_id
        ,customers.first_name
        ,customers.last_name 
        ,customer_orders.first_order_date                       AS first_order_date
        ,customer_orders.most_recent_order_date                 AS most_recent_order_date
        ,COALESCE(customer_orders.number_of_orders, 0)          AS number_of_orders
        ,COALESCE(customer_lifetime_value.lifetime_value, 0)    AS lifetime_value 
    FROM customers
        LEFT JOIN customer_orders ON customers.customer_id = customer_orders.customer_id
        LEFT JOIN customer_lifetime_value ON customer_lifetime_value.customer_id = customers.customer_id
)
SELECT
     customer_id
    ,first_name
    ,last_name
    ,first_order_date
    ,most_recent_order_date
    ,number_of_orders
    ,lifetime_value 
FROM dim_customer

