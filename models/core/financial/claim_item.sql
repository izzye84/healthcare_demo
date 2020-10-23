WITH claim_line AS (

    SELECT *
    FROM {{ ref('claim_line__union') }}

)

SELECT *
FROM claim_line
