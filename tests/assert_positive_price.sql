-- Test: No pizza should have zero or negative price
-- Layer: Gold
-- Model: dim_pizza
-- Severity: error

select
    pizza_id,
    price
from {{ ref('dim_pizza') }}
where price <= 0