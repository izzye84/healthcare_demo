{{
	config(
		dist = 'feature_link'
	)
}}

with

join_claims as (
	select 
		claim.patient
		,claim.sub_type
		,claim_item.identifier_claim_header
		,claim_item.serviced_period_start
		,claim_item.serviced_period_end
		,claim_item.location
	from {{ ref('claim') }} inner join {{ ref('claim_item') }}
	on claim.identifier_claim_header = claim_item.identifier_claim_header	
),

filter_claims as (
	select
		patient
		,identifier_claim_header
		,serviced_period_start
		,serviced_period_end
	from join_claims
	where (sub_type like '11%' or sub_type = '12%')
	and location = '21'
),

monthly_claim_count as (
	select 
		patient
		,date(date_trunc('month',serviced_period_start)) as claim_month
		,count(distinct identifier_claim_header) as monthly_claim_count
	from filter_claims
	group by 1,2
),

patient_claim_date_min_max as (
	select
		patient
		,date(date_trunc('month',min(serviced_period_start))) as first_claim_month
		,date(date_trunc('month',max(serviced_period_start))) as last_claim_month
	from join_claims
	group by 1
),

month_per_patient as (
	select distinct 
		month_spine.date_month
		,monthly_claim_count.patient
	from {{ ref('month_spine') }} cross join monthly_claim_count
),

sum_monthly_claims as (
	select monthly_claim_count.*
	,(sum(monthly_claim_count) over(partition by month_per_patient.patient order by month_per_patient.date_month rows between 11 preceding and current row) - 1) as total_claims
	from month_per_patient left join monthly_claim_count
	on month_per_patient.patient = monthly_claim_count.patient and month_per_patient.date_month = monthly_claim_count.claim_month
),

remove_extra_rows as (
	select *
	from sum_monthly_claims
	where patient is not null
),

filter_dates as (
	select remove_extra_rows.*
	from remove_extra_rows inner join patient_claim_date_min_max
	on remove_extra_rows.patient = patient_claim_date_min_max.patient
	where remove_extra_rows.claim_month >= dateadd('month',12,patient_claim_date_min_max.first_claim_month)
)

select distinct
	filter_claims.identifier_claim_header as feature_link
	,'100004' as feature_code
	,'Admit Rate Prior 12 Months' as feature_display
	,filter_dates.total_claims as feature_value
	,filter_dates.claim_month as feature_date
from filter_dates inner join filter_claims
on filter_dates.patient = filter_claims.patient and filter_dates.claim_month = date_trunc('month',filter_claims.serviced_period_start)