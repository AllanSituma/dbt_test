SELECT
client_id,
UPPER(split_part(paybill,' ',3)) Bank,
amount
FROM {{ref('dbt_financial_debits')}}
WHERE lower(split_part(paybill,' ',2)) = 'via'
OR lower(split_part(recipient,' ',3)) = 'via'
OR lower(split_part(recipient,' ',4)) = 'via'
OR lower(split_part(recipient,' ',5)) = 'via'
OR lower(split_part(recipient,' ',6)) = 'via'