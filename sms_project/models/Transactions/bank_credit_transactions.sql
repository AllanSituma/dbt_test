{{
    config(
        materialized='incremental',
        schema='data_science'
    )
}}
WITH transactions AS (
SELECT
client_id,
amount,
sender,
transaction_code,
transaction_date,
created_at
FROM {{ref('dbt_financial_credits')}}
WHERE lower(split_part(sender,' ',2)) = 'bank'
OR lower(split_part(sender,' ',3)) = 'bank'
OR lower(split_part(sender,' ',3)) = 'equity'
OR lower(split_part(sender,' ',4)) = 'bank'
OR lower(split_part(sender,' ',5)) = 'bank'
OR split_part(sender,' ',1) =  '329137'
OR split_part(sender,' ',1) =  '329330'
OR split_part(sender,' ',1) =  '517819'
OR lower(split_part(sender,' ',2)) = 'stanbic'
OR lower(split_part(sender,' ',1)) = 'citibank'
OR lower(split_part(sender,' ',1)) = 'dtb'
OR lower(split_part(sender,' ',1)) = 'gulf'
),
	
last_loan as (
SELECT * 
FROM loan_first 
WHERE loan_first."LastLoanOn" IS NOT NULL)

SELECT transactions.*
FROM last_loan
JOIN transactions on last_loan."ClientID" = transactions.client_id
AND last_loan."LastLoanOn"::Date >= transactions.transaction_date::date
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE created_at > (select max(created_at) from {{ this }})

{% endif %}

