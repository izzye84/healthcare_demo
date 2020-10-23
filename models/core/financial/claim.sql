WITH claim_header AS (

    SELECT *
    FROM {{ ref('claim_header__union') }}

)

SELECT *
FROM claim_header
