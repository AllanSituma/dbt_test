{{
	config(
		materialized='incremental',
		schema='data_science'
	)
}}



WITH transactions as (

SELECT    client_id,
	      sender,
	      amount,
	      CASE WHEN third_part = 'TIMIZA' then 'TIMIZA' 
	           else first_part
	      end first_part,
		  transaction_code,
	      transaction_date,
		  created_at
	FROM (
SELECT
    amount,
	sender,
	client_id,
	split_part(paybill,' ',1) first_part,
	split_part(paybill,' ',2) second_part,
	split_part(paybill,' ',3) third_part,
	split_part(paybill,' ',4) fourth_part,
	split_part(paybill,' ',5) fifth_part,
	split_part(paybill,' ',6) sixth_part,
	created_at,
	transaction_code,
	transaction_date
FROM {{ref('dbt_financial_debits')}}    ) q1
	WHERE first_part IN (
	  'AERO','AFRI','AFRIKALOAN','ALIQUOT','BAYES',
		'BIDII','BRANCH','EXTEND','FINANCE PLAN','GRANARY',
		'HARAKA','INVENTURE','JISTAWI','KUWAZO','Letshego',
		'LPESA','MKOPO','OKOLEA','OPAL','Paddy','PEOLIMONA','PESAPRO',
		'UTUNZI','PEZESHA','USAWA2','SHIKA','SPIDER','STAWIKA','Tala',
		'SUAVE','Zidisha','WEZA','UBAPESA'
	) OR
	
	second_part IN (
	'AERO','AFRI','AFRIKALOAN','ALIQUOT','BAYES',
		'BIDII','BRANCH','EXTEND','FINANCE PLAN','GRANARY',
		'HARAKA','INVENTURE','JISTAWI','KUWAZO','Letshego',
		'LPESA','MKOPO','OKOLEA','OPAL','Paddy','PEOLIMONA','PESAPRO',
		'UTUNZI','PEZESHA','USAWA2','SHIKA','SPIDER','STAWIKA','Tala',
		'SUAVE','Zidisha','WEZA','UBAPESA'
	
	
	)
	OR 
    third_part IN (
	'AERO','AFRI','AFRIKALOAN','ALIQUOT','TIMIZA','BAYES',
		'BIDII','BRANCH','EXTEND','FINANCE PLAN','GRANARY',
		'HARAKA','INVENTURE','JISTAWI','KUWAZO','Letshego',
		'LPESA','MKOPO','OKOLEA','OPAL','Paddy','PEOLIMONA','PESAPRO',
		'UTUNZI','PEZESHA','USAWA2','SHIKA','SPIDER','STAWIKA','Tala',
		'SUAVE','Zidisha','WEZA','UBAPESA'
	
	)
),



last_loan AS (
SELECT * 
FROM loan_first 
WHERE loan_first."LastLoanOn" IS NOT NULL)

SELECT transactions.*
FROM last_loan
JOIN transactions ON last_loan."ClientID" = transactions.client_id
AND last_loan."LastLoanOn"::Date >= transactions.transaction_date::date

{% if is_incremental() %}

WHERE created_at > (SELECT MAX(created_at) FROM {{this}})

{% endif %}