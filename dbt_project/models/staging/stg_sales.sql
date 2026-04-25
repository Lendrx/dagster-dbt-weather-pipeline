-- Staging: Verkaufsdaten typisieren und vereinheitlichen.
with source_data as (
    select
        order_id,
        customer_name,
        amount,
        order_date
    from {{ source('my_source', 'raw_sales') }}
)
select
    order_id,
    upper(customer_name) as customer_name,
    amount,
    cast(order_date as date) as order_date
from source_data