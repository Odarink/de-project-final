import vertica_python
import pandas as pd

class MetricsCalculator:
    def __init__(self, vertica_config):
        self.vertica_config = vertica_config

    def fetch_and_calculate_metrics(self, date: str) -> pd.DataFrame:
        with vertica_python.connect(**self.vertica_config) as conn:
            with conn.cursor() as cur:
                query = f"""
                WITH cte AS (
                    SELECT *,
                        COUNT(t.operation_id) OVER (PARTITION BY t.account_number_from, t.currency_code, DATE(t.transaction_dt)) AS cnt_per_account
                    FROM STV2024031257__STAGING.transactions AS t
                    WHERE t.transaction_dt::date = '{date}'
                        AND t.account_number_from > 0
                        AND t.account_number_to > 0
                        AND t.status = 'done'
                        AND t.transaction_type IN ('c2a_incoming',
                                                    'c2b_partner_incoming',
                                                    'sbp_incoming',
                                                    'sbp_outgoing',
                                                    'transfer_incoming',
                                                    'transfer_outgoing')
                )
                SELECT 
                    '{date}' AS date_update,
                    SUM(t.amount * c.currency_with_div) AS amount_total,
                    COUNT(*) AS cnt_transactions,
                    AVG(cnt_per_account) AS avg_transactions_per_account,
                    COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
                FROM cte AS t 
                LEFT JOIN STV2024031257__STAGING.currencies c ON t.currency_code = c.currency_code AND currency_code_with = 420
                WHERE c.date_update = '{date}'
                GROUP BY 1
                """

                cur.execute(query)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=columns)
                return df

    def load_metrics_to_vertica(self, df: pd.DataFrame, vertica_table_name: str):
        with vertica_python.connect(**self.vertica_config) as conn:
            with conn.cursor() as cur:
                # Удаление старых данных
                date_update = df['date_update'][0]
                delete_query = f"DELETE FROM {vertica_table_name} WHERE date_update = '{date_update}'"
                cur.execute(delete_query)

                # Загрузка новых данных
                columns = ', '.join(df.columns)
                values = ', '.join(['%s'] * len(df.columns))
                insert_query = f"INSERT INTO {vertica_table_name} ({columns}) VALUES ({values})"
                data_tuples = [tuple(x) for x in df.to_numpy()]
                cur.executemany(insert_query, data_tuples)


    def calculate_and_load_metrics(self, date: str, vertica_table_name: str):
        df = self.fetch_and_calculate_metrics(date)
        self.load_metrics_to_vertica(df, vertica_table_name)
