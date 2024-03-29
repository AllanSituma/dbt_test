{{
    config(
        materialized='incremental',
        schema='data_science'
    )
}}

SELECT DISTINCT ON (transaction_code)client_id,
       amount,
       transaction_code,
	 transaction_date,
       created_at 
FROM (
SELECT loan_first."ClientID" client_id,
      sms_raw.*
FROM sms_raw
JOIN loan_first ON loan_first.user_id = sms_raw.user_id
)q1
WHERE q1.payment_type = 'airtime'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND created_at > (select max(created_at) from {{ this }})

{% endif %}