WITH claim_line AS (

    SELECT *
    FROM {{ ref('stg_humana__medical_claims') }}

),

final AS (

    SELECT identifier_claim_header,
           identifier_claim_line,
           revenue,
           product_or_service,
           modifier_1,
           modifier_2,
           serviced_period_start,
           serviced_period_end,
           pot_cd AS location,
           quantity,
           CASE WHEN quantity <> 0
                THEN chrg_amt / quantity
                ELSE 0
           END AS unit_price,
           chrg_amt AS net,
           client_id,
           ingest_date

    FROM claim_line

)

SELECT *
FROM final
