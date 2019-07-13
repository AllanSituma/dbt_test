WITH sms_raw_new AS (
SELECT loan_first."ClientID" client_id,
      sms_raw.*
FROM sms_raw
JOIN loan_first ON loan_first.user_id = sms_raw.user_id
)

SELECT client_id,
       amount,
	   transaction_date 
FROM (
SELECT loan_first."ClientID" client_id,
      sms_raw.*
FROM sms_raw
JOIN loan_first ON loan_first.user_id = sms_raw.user_id
)q1
WHERE split_part(sender,' ',1) = 'Lipa'