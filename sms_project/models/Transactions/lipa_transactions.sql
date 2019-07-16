{{
      config(
            materialized='incremental',
            schema='data_science'
      )
}}


WITH sms_raw_new AS (
SELECT loan_first."ClientID" client_id,
      sms_raw.*
FROM sms_raw
JOIN loan_first ON loan_first.user_id = sms_raw.user_id
)

SELECT client_id,
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
WHERE split_part(sender,' ',1) = 'Lipa'

{% if is_incremental() %}

AND  created_at > (SELECT MAX(created_at)FROM {{this}})

{% endif %}