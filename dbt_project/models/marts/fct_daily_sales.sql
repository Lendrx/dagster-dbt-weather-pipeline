-- Mart: Taegliche Sales-Kennzahlen.
with sales as (
    select
        order_date,
        order_id,
        amount,
        tax_amount
    from {{ ref('dim_sales') }}
)
select
    order_date,
    count(order_id) as total_orders,
    sum(amount) as total_revenue,
    sum(tax_amount) as total_tax
from sales
group by 1