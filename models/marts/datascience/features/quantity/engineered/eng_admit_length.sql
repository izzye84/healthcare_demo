{{
	config(
		dist = 'feature_link'
	)
}}

with 

claim_items as (
	select distinct identifier_claim_header
	,serviced_period_start
	,serviced_period_end
	from {{ ref('claim_item') }}
	where location = '21'
),

claims as (
	select identifier_claim_header
	from {{ ref('claim') }}
	where sub_type like '11%' or sub_type like '12%'
),

join_claims as (
	select claim_items.identifier_claim_header
		,claim_items.serviced_period_start
		,claim_items.serviced_period_end
	from claim_items inner join claims
		on claim_items.identifier_claim_header = claims.identifier_claim_header
)

select identifier_claim_header as feature_link
	,'100003' as feature_code
	,'Admit Length Days' as feature_display
	,serviced_period_end - serviced_period_start as feature_value
	,serviced_period_end as feature_date
from join_claims