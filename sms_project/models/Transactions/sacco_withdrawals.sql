WITH all_transactions as (
SELECT
    amount,
	paybill,
	client_id,
	split_part(sender,' ',1) first_part,
	split_part(sender,' ',2) second_part,
	split_part(sender,' ',3) third_part,
	split_part(sender,' ',4) fourth_part,
	split_part(sender,' ',5) fifth_part,
	split_part(sender,' ',6) sixth_part,
	transaction_date
FROM {{ref('dbt_financial_credits')}}
)


SELECT client_id,
	      paybill,
	      amount,
	      first_part,
	      transaction_date
FROM all_transactions
WHERE second_part = 'SACCO'
	OR first_part = 'KBSACCO'
	OR third_part = 'SACCO'
	OR second_part = 'Sacco'
	OR second_part = 'Sacco.'
    OR third_part = 'Sacco'
    OR fourth_part = 'SACCO'
    OR fourth_part = 'Sacco'