select product_id,
       product_category_name_english as category,
       product_weight_g, product_length_cm, product_height_cm,
       product_width_cm, product_photos_qty
from {{ source('silver', 'products') }}