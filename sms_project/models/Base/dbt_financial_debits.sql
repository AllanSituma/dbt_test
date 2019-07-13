SELECT 
DISTINCT ON (transaction_code)loan_first."ClientID" client_id,
sms_raw.*
FROM sms_raw 
JOIN loan_first on loan_first."user_id" = sms_raw.user_id
WHERE payment_type = 'sent'
AND paybill IS NOT NULL