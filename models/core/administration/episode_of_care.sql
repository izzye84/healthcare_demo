{{
    config(
        dist = 'identifier_sh_uid'
    )
}}

select * from {{ ref('episode_of_care__assembled') }}