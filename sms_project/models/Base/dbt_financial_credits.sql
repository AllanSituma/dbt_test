{{
    config(
        materialized='incremental',
        schema='data_science'
    )
}}

SELECT DISTINCT ON (transaction_code)loan_first."ClientID" client_id ,
       sms_raw.*
FROM sms_raw
JOIN loan_first on loan_first.user_id = sms_raw.user_id
WHERE sender is not null
AND split_part(sender,' ',1) != 'Lipa'
AND split_part(sender,' ',3) NOT LIKE '%07%'
AND split_part(sender,' ',4) NOT LIKE '%07%'
AND split_part(sender,' ',5) NOT LIKE '%07%'
AND split_part(sender,' ',6) NOT LIKE '%07%'
AND split_part(sender,' ',6) NOT LIKE '%254%'
AND split_part(sender,' ',3) NOT LIKE '%254%'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where created_at > (select max(created_at) from {{ this }})

{% endif %}