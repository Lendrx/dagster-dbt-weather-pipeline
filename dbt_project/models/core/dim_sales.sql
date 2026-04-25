-- Core: Verkaufsdaten mit Steuerkennzahl.
with sales as (
    select
        order_id,
        customer_name,
        amount,
        order_date
    from {{ ref('stg_sales') }}
)
select
    order_id,
    customer_name,
    amount,
    order_date,
    round((amount * 0.19)::numeric, 2) as tax_amount
from sales
