--drop table if exists STV2024031257__STAGING.stg_сurrencies;
CREATE TABLE STV2024031257__STAGING.stg_сurrencies
(
	date_update timestamp NULL,
	currency_code integer NOT NULL,
	currency_code_with integer NULL,
	currency_with_div float NULL,
    CONSTRAINT stg_сurrencies_pkey PRIMARY KEY (currency_code, date_update, currency_code_with)
)
ORDER BY currency_code, date_update
SEGMENTED BY hash(currency_code) ALL NODES
PARTITION BY date_update::date;
