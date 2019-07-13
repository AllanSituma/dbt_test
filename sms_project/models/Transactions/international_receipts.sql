SELECT
client_id,
amount,
sender,
transaction_date
FROM {{ref('dbt_financial_credits')}}
WHERE lower(split_part(sender,' ',6)) = 'bank'
AND split_part(sender,' ',1) NOT IN ('329137','329330','517819')