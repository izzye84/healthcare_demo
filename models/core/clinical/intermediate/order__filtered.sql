with

orders as (
    select * from {{ ref('stg_ssm__lab_order') }}
),

observations as (
    select * from {{ ref('stg_ssm__lab_observation') }}
)

select *
from orders
where identifier_order in (select identifier_order from observations)