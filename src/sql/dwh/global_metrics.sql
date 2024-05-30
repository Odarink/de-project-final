--drop table if exists STV2024031257__DWH.dwh_global_metrics;
CREATE TABLE STV2024031257__DWH.dwh_global_metrics
(
	date_update timestamp NULL,
	cnt_transactions integer NULL,
	avg_transactions_per_account numeric(15,2) NULL,
	cnt_accounts_make_transactions integer NULL,
	currency_from integer NULL,
	amount_total numeric(15,2) NULL,
    CONSTRAINT stg_transactions_pkey PRIMARY KEY (date_update)
)
ORDER BY date_update
PARTITION BY date_update::date;
