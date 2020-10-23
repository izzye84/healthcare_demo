WITH claim_line AS (

    SELECT *
    FROM {{ ref('claim_line__calculations') }}

),

-- union clients into a single dataset; e.g., Conviva, Humana, SSM, etc.
final AS (

    SELECT *
    FROM claim_line

)

SELECT *
FROM final
