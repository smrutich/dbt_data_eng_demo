-- Test: Every pizza_id in fact_orders must exist in dim_pizza
-- Ensures no orphaned fact records
-- Layer: Gold
-- Severity: error

select
    f.order_detail_id,
    f.pizza_id
from {{ ref('fact_orders') }} f
left join {{ ref('dim_pizza') }} d
    on f.pizza_sk = d.pizza_sk
where d.pizza_sk is null