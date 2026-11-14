-- Returns any (order_id, order_item_id) that appears more than once.
select order_id, order_item_id, count(*) as copies
from {{ ref('fct_order_items') }}
group by order_id, order_item_id
having count(*) > 1