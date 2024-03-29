{{
      config(
            materialized='incremental',
            schema='data_science'
      )
}}


SELECT DISTINCT ON (transaction_code)   client_id,
       amount,
       transaction_code,
       created_at,
	transaction_date
FROM (
SELECT loan_first."ClientID" client_id,
      sms_raw.*
FROM sms_raw
JOIN loan_first on loan_first.user_id = sms_raw.user_id
)q1
JOIN loan_first ON loan_first."ClientID" = q1.client_id
WHERE payment_type = 'mshwari_loan'

{% if is_incremental() %}

AND created_at > (SELECT MAX(created_at)FROM {{this}})

{% endif %}