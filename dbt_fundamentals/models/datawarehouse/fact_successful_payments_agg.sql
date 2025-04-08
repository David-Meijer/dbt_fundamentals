-------------------------------------------
-- payment methods used in jinja for loop--
-------------------------------------------
{%- set payment_methods = ['coupon', 'gift_card', 'credit_card', 'bank_transfer'] -%}

WITH payments AS(
    SELECT
         payment_id
        ,order_id
        ,payment_method
        ,status
        ,amount
        ,created
    FROM {{ ref('stripe_payments')}}
)
,
payments_pivoted AS (
    SELECT
        order_id

        {# Loop through all possible payment methods, generating a sum & case when statement for each #}
        {% for payment_method in payment_methods %}
            ,SUM(CASE WHEN payment_method = '{{payment_method}}' THEN COALESCE((NULLIF(amount, 0) / 100), 0) ELSE 0 END) AS {{payment_method}}_amount
        {% endfor %}
    FROM payments
    WHERE 1=1
        AND status = 'success'
    GROUP BY order_id
)
SELECT * FROM payments_pivoted