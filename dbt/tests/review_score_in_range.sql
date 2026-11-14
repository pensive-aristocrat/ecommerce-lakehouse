-- Returns any review score outside 1 to 5.
select order_id, review_score
from {{ ref('fct_order_items') }}
where review_score < 1 or review_score > 5