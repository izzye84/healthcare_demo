with

loinc_source as (
    select * from {{ source('reference_data','loinc') }}
),

renamed as (
    select {{ empty_string_to_null('loinc_num') }} as loinc_num
        ,{{ empty_string_to_null('component') }} as component
        ,{{ empty_string_to_null('property') }} as property
        ,{{ empty_string_to_null('time_aspct') }} as time_aspct
        ,{{ empty_string_to_null('body_system') }} as body_system
        ,{{ empty_string_to_null('scale_typ') }} as scale_typ
        ,{{ empty_string_to_null('method_typ') }} as method_typ
        ,{{ empty_string_to_null('class') }} as class
        ,{{ empty_string_to_null('versionlastchanged') }} as version_last_changed
        ,{{ empty_string_to_null('chng_type') }} as chng_type
        ,{{ empty_string_to_null('definitiondescription') }} as definition_description
        ,{{ empty_string_to_null('status') }} as status
        ,{{ empty_string_to_null('consumer_name') }} as consumer_name
        ,classtype as class_type
        ,{{ empty_string_to_null('formula') }} as formula
        ,{{ empty_string_to_null('species') }} as species
        ,{{ empty_string_to_null('exmpl_answers') }} as exmpl_answers
        ,{{ empty_string_to_null('survey_quest_text') }} as survey_quest_text
        ,{{ empty_string_to_null('survey_quest_src') }} as survey_quest_src
        ,{{ empty_string_to_null('unitsrequired') }} as units_required
        ,{{ empty_string_to_null('submitted_units') }} as submitted_units
        ,{{ empty_string_to_null('relatednames2') }} as related_names_2
        ,{{ empty_string_to_null('shortname') }} as short_name
        ,{{ empty_string_to_null('order_obs') }} as order_obs
        ,{{ empty_string_to_null('cdisc_common_tests') }} as cdisc_common_tests
        ,{{ empty_string_to_null('hl7_field_subfield_id') }} as hl7_field_subfield_id
        ,{{ empty_string_to_null('external_copyright_notice') }} as external_copyright_notice
        ,{{ empty_string_to_null('example_units') }} as example_units
        ,{{ empty_string_to_null('long_common_name') }} as long_common_name
        ,{{ empty_string_to_null('unitsandrange') }} as units_and_range
        ,{{ empty_string_to_null('example_ucum_units') }} as example_ucum_units
        ,{{ empty_string_to_null('example_si_ucum_units') }} as example_si_ucum_units
        ,{{ empty_string_to_null('status_reason') }} as status_reason
        ,{{ empty_string_to_null('status_text') }} as status_text
        ,{{ empty_string_to_null('change_reason_public') }} as change_reason_public
        ,common_test_rank as common_test_rank
        ,common_order_rank as common_order_rank
        ,common_si_test_rank as common_si_test_rank
        ,{{ empty_string_to_null('hl7_attachment_structure') }} as hl7_attachment_structure
        ,{{ empty_string_to_null('externalcopyrightlink') }} as external_copyright_link
        ,{{ empty_string_to_null('paneltype') }} as panel_type
        ,{{ empty_string_to_null('askatorderentry') }} as ask_at_order_entry
        ,{{ empty_string_to_null('associatedobservations') }} as associated_observations
        ,{{ empty_string_to_null('versionfirstreleased') }} as version_first_released
        ,{{ empty_string_to_null('validhl7attachmentrequest') }} as valid_hl7_attachment_request
        ,{{ empty_string_to_null('displayname') }} as display_name
    from loinc_source
)

select * from renamed