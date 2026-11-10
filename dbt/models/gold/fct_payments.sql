-- Grain: one row per payment (an order can be paid in several parts).
select
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    o.customer_id,
    cast(date_format(o.order_purchase_timestamp, '%Y%m%d') as integer) as purchase_date_key
from {{ source('silver', 'order_payments') }} p
join {{ source('silver', 'orders') }} o on p.order_id = o.order_id