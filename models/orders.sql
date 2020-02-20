with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
)
select orders.customer_id,orders.order_date, orders.order_id,
    sum(payments.payment_amount) as order_total_payments, 
    min(payments.payment_date) as first_payment_date, 
    max(payments.payment_date) as last_payment_date,
    count(payments.payment_id) as payment_count
from orders
left join payments using(order_id)
group by orders.customer_id,orders.order_date, orders.order_id