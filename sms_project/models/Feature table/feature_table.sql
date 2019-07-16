{{
    config(
        materialized='table',
        schema='data_science'
    )
}}

WITH airtime_purchase as (
    SELECT *
    FROM {{ref('airtime_ag_purchase')}}
),

bank_deposit as (
    SELECT *
    FROM {{ref('bank_deposit_ag')}}
),

bank_withdraw as (
    SELECT *
    FROM {{ref('bank_withdraw_ag')}}
),

betting_receipts as (
    SELECT *
    FROM {{ref('betting_receipts_ag')}}
),

betting_spends as (
    SELECT *
    FROM {{ref('betting_spends_ag')}}
),

lipa_mpesa as (
    SELECT * 
    FROM {{ref('lipa_transactions_ag')}}
),

loanapp_borrowing as (
    SELECT *
    FROM {{ref('loanapp_borrowing_ag')}}
),

loanapp_repayment as (
    SELECT *
    FROM {{ref('loanapp_repayment_ag')}}
),

loanapps_used as (
    SELECT *
    FROM {{ref('loanapps_used')}}
),

mshwari as (
    SELECT *
    FROM {{ref('mshwari_receipts_ag')}}
),

bank_payments as (
    SELECT *
    FROM {{ref('payments_via_bank_ag')}}
),

sacco_savings as (
    SELECT *
    FROM {{ref('sacco_savings_ag')}}
),

sacco_withdraw as (
    SELECT *
    FROM {{ref('sacco_withdrawal_ag')}}
)