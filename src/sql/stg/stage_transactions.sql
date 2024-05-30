--drop table if exists STV2024031257__STAGING.stg_transactions;
CREATE TABLE STV2024031257__STAGING.stg_transactions
(
	operation_id varchar(60) NOT NULL,
	account_number_from integer NULL,
	account_number_to integer NULL,
	currency_code integer NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount integer NULL,
	transaction_dt timestamp NULL,
    CONSTRAINT stg_transactions_pkey PRIMARY KEY (operation_id)
)
ORDER BY operation_id, transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt) ALL NODES
PARTITION BY transaction_dt::date;
