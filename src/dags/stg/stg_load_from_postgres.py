import pandas as pd
import psycopg2
import vertica_python
from datetime import datetime


class DataMigrator :
    def __init__(self, pg_config, vertica_config) :
        self.pg_config = pg_config
        self.vertica_config = vertica_config

    def fetch_data_from_postgres(self, date: str, table_name: str, date_column: str) -> pd.DataFrame :
        with psycopg2.connect(
            dbname=self.pg_config['dbname'],
            user=self.pg_config['user'],
            password=self.pg_config['password'],
            host=self.pg_config['host'],
            port=self.pg_config['port']
        ) as conn:
            query = f"""
                SELECT * 
                FROM {table_name} 
                WHERE {date_column}::date = '{date}'::date
            """
            df = pd.read_sql(query, conn)
            return df

    def load_data_to_vertica(self, df: pd.DataFrame, date_column: str, date_value: str, vertica_table_name: str) :


        with vertica_python.connect(**self.vertica_config) as conn:
            with conn.cursor() as cur:
                # Удаление старых данных
                delete_query = f"DELETE FROM {vertica_table_name} WHERE {date_column}::date = '{date_value}'::date"
                cur.execute(delete_query)
                # Загрузка новых данных
                columns = df.columns.tolist()
                insert_query = f"INSERT INTO {vertica_table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])})"
                for row in df.itertuples(index=False, name=None) :
                    cur.execute(insert_query, row)

    def migrate_data(self, date: str, pg_table_name: str, vertica_table_name: str, date_column: str) :
        data = self.fetch_data_from_postgres(date, pg_table_name, date_column)
        self.load_data_to_vertica(data, date_column, date, vertica_table_name)
