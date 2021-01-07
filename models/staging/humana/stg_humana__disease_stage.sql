with referral as (

    select identifier_external_source,
           esrd_2clms_last4mo,
           ckd_stage,
           client_id,
           ingest_date

    from {{ ref('base_humana__referral') }}

),

disease_stage as (

    select identifier_external_source,
           case when esrd_2clms_last4mo = 'Y'
                then 'ESRD'

                when ckd_stage is not null
                then 'CKD ' || ckd_stage
           end as disease_stage,
           client_id,
           ingest_date

    from referral

)

select *
from disease_stage
