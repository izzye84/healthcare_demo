WITH claim_header AS (

    SELECT *
    FROM {{ ref('stg_humana__medical_claims') }}

),

claim_header_total AS (

    SELECT identifier_claim_header,
           identifier_claim_line,
           'professional' AS type,
           patient,
           billable_period_start,
           billable_period_end,
           'humana' AS insurer,
           provider,
           pot_cd AS facility,
           SUM(chrg_amt) OVER (PARTITION BY identifier_claim_header) AS total,
           client_id,
           ingest_date

    FROM claim_header

),

final AS (

  SELECT identifier_claim_header,
         type,
         patient,
         billable_period_start,
         billable_period_end,
         insurer,
         provider,
         facility,
         total,
         client_id,
         ingest_date

  FROM claim_header_total
  WHERE identifier_claim_line = 1

)

SELECT *
FROM final
