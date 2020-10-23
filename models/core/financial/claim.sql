WITH claim_header AS (

    SELECT *
    FROM {{ ref('claim_header__sum') }}

),

-- union clients into a single dataset; e.g., Conviva, Humana, SSM, etc.
final AS (

    SELECT *
    FROM claim_header

)

SELECT *
FROM final
