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

encounters as (
	select distinct identifier_external_encounter
		,hospitalization_discharge_disposition_code
	from {{ ref('encounter') }}
),

discharge_status as (
	select join_claims.identifier_claim_header
		,join_claims.serviced_period_end
		,encounters.hospitalization_discharge_disposition_code
	from join_claims inner join encounters
	on join_claims.identifier_claim_header = encounters.identifier_external_encounter
)

select identifier_claim_header as feature_link
	,'100002' as feature_code
	,'Discharge Status' as feature_display
	,hospitalization_discharge_disposition_code as feature_value
	,serviced_period_end as feature_date
from discharge_status