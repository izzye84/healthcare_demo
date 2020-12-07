with 

source_admissibility as (
    select * from {{ ref('base_platform__admissibility') }} where client_id in ('ssm','humana')
),

source_enrollment_status as (
    select * from {{ ref('base_platform__enrollment_status') }} where client_id in ('ssm','humana')
),

source_health_cloud as (
    select * from {{ ref('base_platform__health_cloud_account') }} where client_id in ('ssm','humana')
),

lag_admissible as (
	select 
		identifier_sh_uid
		,active
		,ingest_date 
		,lag(active,1) over(partition by identifier_sh_uid order by ingest_date) as previous_admissible
	from source_admissibility 
),

episode_period as (
	select
		identifier_sh_uid
		
        ,case 
			when (active = 'true' and previous_admissible = 'false')
				or (active = 'true' and previous_admissible is null)
			then ingest_date
		end as period_start
		
        ,case
			when active = 'false' and previous_admissible = 'true'
			then ingest_date
		end as period_end

	from lag_admissible 
),

filter_episodes as (
	select *
	from episode_period
	where period_start is not null 
),

union_status as (
	select
		identifier_sh_uid
		,status
		,status_reason
        ,client_id
		,ingest_date
	from source_enrollment_status

	union all

	select 
        identifier_sh_uid 
	    ,status
	    
        ,case status
	 	    when 'Disenrolled' then disenrolled_reason
	     else inadmissible_reason
	    end as status_reason

        ,client_id
	    ,ingest_date
	from source_health_cloud
),

add_row_num as (
	select 
        identifier_sh_uid
	    ,status
	    ,status_reason
	    ,row_number() over(partition by identifier_sh_uid order by ingest_date desc) as row_num
        ,client_id
	from union_status
),

current_status as (
	select *
	from add_row_num 
	where row_num = 1
),

joined as (
	select
		filter_episodes.identifier_sh_uid
		,filter_episodes.period_start
		,filter_episodes.period_end
		,current_status.status
		,current_status.status_reason
        ,current_status.client_id
	from filter_episodes left join current_status
	on filter_episodes.identifier_sh_uid = current_status.identifier_sh_uid
)

select * from joined