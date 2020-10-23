with claim_header as (

    select *
    from {{ ref('base_ssm__claim_header') }}

),

final as (

    select identifier_claim_header,
           case when facility in ('02',
                                  '19',
                                  '21',
                                  '22',
                                  '23',
                                  '24',
                                  '26',
                                  '31',
                                  '34',
                                  '41',
                                  '42',
                                  '51',
                                  '52',
                                  '53',
                                  '56',
                                  '61')
                then 'institutional'
                else 'professional'
           end as type,
           sub_type,
           patient,
           billable_period_start,
           billable_period_end,
           insurer,
           provider,
           facility,
           total,
           client_id,
           ingest_date

    from claim_header

)

select *
from final
