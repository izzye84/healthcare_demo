{{
    config(
        dist = 'practitioner',
        sort = 'practitioner'
    )
}}

with

base_practitioner as (
    select * from {{ ref('base_reference_data__npi') }}
),

base_practitioner_role as (
    select * from {{ ref('base_reference_data__npi_taxonomy') }}
),

reduce_taxonomy  as (
    select 
        identifier_npi as practitioner

        ,case 
            when provider_taxonomy_switch_1 = 'Y' then provider_taxonomy_code_1
            when provider_taxonomy_switch_2 = 'Y' then provider_taxonomy_code_2
            when provider_taxonomy_switch_3 = 'Y' then provider_taxonomy_code_3
            when provider_taxonomy_switch_4 = 'Y' then provider_taxonomy_code_4
            when provider_taxonomy_switch_5 = 'Y' then provider_taxonomy_code_5
            when provider_taxonomy_switch_6 = 'Y' then provider_taxonomy_code_6
            when provider_taxonomy_switch_7 = 'Y' then provider_taxonomy_code_7
            when provider_taxonomy_switch_8 = 'Y' then provider_taxonomy_code_8
            when provider_taxonomy_switch_9 = 'Y' then provider_taxonomy_code_9
            when provider_taxonomy_switch_10 = 'Y' then provider_taxonomy_code_10
            when provider_taxonomy_switch_11 = 'Y' then provider_taxonomy_code_11
            when provider_taxonomy_switch_12 = 'Y' then provider_taxonomy_code_12
            when provider_taxonomy_switch_13 = 'Y' then provider_taxonomy_code_13
            when provider_taxonomy_switch_14 = 'Y' then provider_taxonomy_code_14
            when provider_taxonomy_switch_15 = 'Y' then provider_taxonomy_code_15
        end as specialty_code

    ,provider_practice_telephone_number as telecom_work_phone_number 
    ,provider_practice_fax_number as telecom_work_fax_number
from base_practitioner
),

joined as (
    select distinct
        {{ dbt_utils.surrogate_key([
            'reduce_taxonomy.practitioner',
            'reduce_taxonomy.specialty_code',
            'base_practitioner_role.specialty_code_display']) 
        }} as identifier_practitioner_role
        ,reduce_taxonomy.practitioner
        ,reduce_taxonomy.specialty_code
        ,base_practitioner_role.specialty_code_display
        ,base_practitioner_role.specialty_code_text
        ,reduce_taxonomy.telecom_work_phone_number
        ,reduce_taxonomy.telecom_work_fax_number
    from reduce_taxonomy inner join base_practitioner_role
    on reduce_taxonomy.specialty_code = base_practitioner_role.specialty_code
)

select * from joined