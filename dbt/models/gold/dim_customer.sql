-- Current version of each customer (from the SCD Type 2 table).
select customer_id, customer_unique_id, customer_zip_code_prefix,
       customer_city, customer_state
from {{ source('silver', 'customers_scd2') }}
where is_current