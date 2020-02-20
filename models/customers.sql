with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('orders') }}
),
customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(order_total_payments) as customer_total_payment, 
    min(first_payment_date) as customer_first_paid_date, 
    max(last_payment_date) as customer_last_paid_date,
    sum(payment_count) as customer_payment_count
    from orders
    group by customer_id
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.customer_total_payment, 
   customer_orders.customer_first_paid_date, 
    customer_orders.customer_last_paid_date,
    customer_orders.customer_payment_count
    from customers
    left join customer_orders using (customer_id)
)
select * from final