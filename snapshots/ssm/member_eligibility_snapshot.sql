{% snapshot member_eligibility_snapshot %}
    {{
        config(
            unique_key= "patient_id || '-' || lob || '-' || insurance_name || '-' || eff_date",
            strategy='timestamp',
            updated_at='ingest_date'
        )
    }}

    select * from {{ source('ssm_claims','member_eligibility') }}

{% endsnapshot %}