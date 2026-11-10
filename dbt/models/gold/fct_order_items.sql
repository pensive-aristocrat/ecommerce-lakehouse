-- Grain: one row per item in an order.
with orders as (
    select * from {{ source('silver', 'orders') }}
),
items as (
    select * from {{ source('silver', 'order_items') }}
),
reviews as (  -- one score per order, so the join cannot duplicate items
    select order_id, avg(review_score) as review_score
    from {{ source('silver', 'reviews') }}
    group by order_id
)
select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    cast(date_format(o.order_purchase_timestamp, '%Y%m%d') as integer) as purchase_date_key,
    o.order_status,
    o.delivered_late,
    i.price,
    i.freight_value,
    i.price + i.freight_value as item_total,
    r.review_score
from items i
join orders o on i.order_id = o.order_id
left join reviews r on i.order_id = r.order_id