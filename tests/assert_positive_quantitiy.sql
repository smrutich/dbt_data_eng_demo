-- Test: No order line items should have zero or negative quantity
-- Layer: Gold
-- Model: fact_orders
-- Severity: error

select
    order_detail_id,
    quantity
from {{ ref('fact_orders') }}
where not (quantity between 0 and 100)