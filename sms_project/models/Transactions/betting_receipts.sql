WITH all_transactions AS (
SELECT
	amount,
	sender,
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
	      sender,
	      amount,
	      first_part,
	      transaction_date
	FROM all_transactions
	WHERE first_part IN ('1XBET',
		   'BETIN',
		   'BETWAY',
		   'DAFABET','SPORTYBET',
		   'O-PLAY','ODIBETS.','PAMBAZUKA','POWERBETS.',
		   'Pevans','JUSTBET_PAYMENTS','KWIKBET','Lipua',
		   'BETMASTER','DAFABET','ELITEBET','GAMECODE','GAMETECH','GAMING','JAMII',
			'BETNAMI','BETRAFIKI.','BETYETU','CITYBET','MOZZARTBET','BETIKA'	)
	