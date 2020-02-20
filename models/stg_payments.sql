select
    id as payment_id,
    "orderID" as order_id,
    "paymentMethod" as payment_method,
    amount as payment_amount,
    created as payment_date
from raw.stripe.payment