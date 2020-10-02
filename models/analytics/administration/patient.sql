with

ssm_patient as (
    select *
    from {{ ref('member_patient__joined') }}
)

select *
from ssm_patient