{{
    config(
        materialized='incremental',
        schema='data_science'
    )
}}

WITH transactions as (
SELECT
client_id,
amount,
paybill,
account,
transaction_code,
transaction_date,
created_at
FROM {{ref ('dbt_financial_debits')}}
WHERE lower(split_part(paybill,' ',2)) = 'bank'
OR lower(split_part(paybill,' ',3)) = 'bank'
OR lower(split_part(paybill,' ',3)) = 'equity'
OR lower(split_part(paybill,' ',2)) = 'stanbic'
OR lower(split_part(paybill,' ',2)) = 'stanchart'
OR lower(split_part(paybill,' ',1)) = 'citi'
OR lower(split_part(paybill,' ',1)) = 'dtb'
OR lower(split_part(paybill,' ',1)) = 'gulf'
OR lower(split_part(paybill,' ',2)) = 'chase'
OR lower(split_part(recipient,' ',2)) = 'cfc'
OR UPPER(split_part(recipient,' ',1)) = 'CBA'
OR lower(split_part(recipient,' ',1)) = 'ecobank'
OR lower(split_part(sender,' ',1)) = 'gulf'),
	
last_loan as (
SELECT * 
FROM loan_first
WHERE loan_first."LastLoanOn" IS NOT NULL)

SELECT transactions.*
FROM last_loan
JOIN transactions on last_loan."ClientID" = transactions.client_id
AND transactions.transaction_date::date <= last_loan."LastLoanOn"::Date


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where created_at > (select max(created_at) from {{ this }})

{% endif %}