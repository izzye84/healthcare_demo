with orders as (
    select * from {{ ref('order__filtered') }}
)

select * from orders