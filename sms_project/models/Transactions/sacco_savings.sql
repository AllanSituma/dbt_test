WITH all_transactions as (
SELECT
    amount,
	paybill,
	client_id,
	split_part(paybill,' ',1) first_part,
	split_part(paybill,' ',2) second_part,
	split_part(paybill,' ',3) third_part,
	split_part(paybill,' ',4) fourth_part,
	split_part(paybill,' ',5) fifth_part,
	split_part(paybill,' ',6) sixth_part,
	transaction_code,
	transaction_date
FROM {{ref('dbt_financial_debits')}})


SELECT client_id,
	      paybill,
	      amount,
	      first_part,
		  transaction_code,
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