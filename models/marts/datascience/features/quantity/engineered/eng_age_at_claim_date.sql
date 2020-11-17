{{
	config(
		dist = 'feature_link'
	)
}}

with 

claim_items as (
	select distinct identifier_claim_header
	    ,serviced_period_end
	from {{ ref('claim_item') }}
	where location = '21'
),

claims as (
	select identifier_claim_header
	    ,patient
	from {{ ref('claim') }}
	where sub_type like '11%' or sub_type like '12%'
),

join_claims as (
	select claim_items.identifier_claim_header
		,claim_items.serviced_period_end
		,claims.patient
	from claim_items inner join claims
		on claim_items.identifier_claim_header = claims.identifier_claim_header
),

add_birth_date as (
	select join_claims.identifier_claim_header
		,join_claims.serviced_period_end
		,patient.birth_date
	from join_claims inner join {{ ref('patient') }}
	on join_claims.patient = patient.identifier_external_source
),

calculate_age as (
	select identifier_claim_header
		,datediff('year',birth_date,serviced_period_end) as age_at_time_of_claim
		,serviced_period_end
		,birth_date
	from add_birth_date
)

select identifier_claim_header as feature_link
	,'100000' as feature_code
	,'Age at time of service date end' as feature_display
	,age_at_time_of_claim as feature_value
	,serviced_period_end as feature_date
from calculate_age