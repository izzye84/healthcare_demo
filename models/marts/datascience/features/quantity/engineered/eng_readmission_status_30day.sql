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
		,patient
	from {{ ref('claim') }}
	where sub_type like '11%' or sub_type like '12%'
),

join_claims as (
	select claim_items.identifier_claim_header
		,claim_items.serviced_period_start
		,claim_items.serviced_period_end
		,claims.patient
		,row_number() over (partition by claims.patient, 
			claim_items.serviced_period_end
			order by claim_items.identifier_claim_header desc) as rnum
	from claim_items inner join claims
		on claim_items.identifier_claim_header = claims.identifier_claim_header
),

readmit_status as (
	select identifier_claim_header
		,case
			when 
				date_diff('day',
					serviced_period_end,
					lead(serviced_period_start) over (partition by patient order by serviced_period_start)) <= 30 then 1
			else 0
		end as readmit_30day
		,serviced_period_end
	from join_claims where rnum = 1
)

select identifier_claim_header as feature_link
	,'100005' as feature_code
	,'Readmission Status 30 Days' as feature_display
	,readmit_30day as feature_value
	,serviced_period_end as feature_date
from readmit_status