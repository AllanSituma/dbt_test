{{
    config(
        materialized='incremental',
        schema='data_science'
    )

}}



SELECT DISTINCT ON (transaction_code) loan_first."ClientID",
sms_raw.*
FROM sms_raw 
JOIN loan_first on loan_first."user_id" = sms_raw.user_id
{% if is_incremental() %}

WHERE created_at > (SELECT MAX(created_at) FROM {{this}})

{% endif %}

AND  split_part(recipient,' ',3)  LIKE '%07%'
OR split_part(recipient,' ',4)  LIKE '%07%'

