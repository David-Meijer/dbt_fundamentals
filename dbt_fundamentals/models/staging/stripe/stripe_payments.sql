

SELECT
     ID             AS payment_id
    ,ORDERID        AS order_id
    ,PAYMENTMETHOD  AS payment_method
    ,STATUS         AS status
    ,AMOUNT         AS amount
    ,CREATED        AS created
FROM {{source('stripe', 'stripe_payments')}}