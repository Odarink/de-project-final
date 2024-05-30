import psycopg2
import vertica_python
from typing import List, Dict, Any
from datetime import datetime
from pydantic import BaseModel



class TransactionModel(BaseModel) :
    operation_id: str
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: int
    transaction_dt: datetime


class CurrenciesModel(BaseModel) :
    date_update: datetime
    currency_code: int
    currency_code_with: int
    currency_with_div: float





class DataMigrator:
    def __init__(self, pg_config: Dict[str, str], vertica_config: Dict[str, str], data_model) :
        self.pg_config = pg_config
        self.vertica_config = vertica_config
        self.data_model = data_model

    def generate_select_query(self, table_name: str, fields: dict, cast_dict: dict, date_column_name: str) -> str :
        select_fields = []
        for field_name, field_type in fields.items() :
            # Используем словарь cast_dict для преобразования типов данных Pydantic в типы данных PostgreSQL
            pg_type = cast_dict.get(field_type, 'text')  # Используем 'text' по умолчанию

            select_fields.append(f"CAST({field_name} AS {pg_type}) AS {field_name}")

        select_clause = ", ".join(select_fields)
        query = f"""
        SELECT 
        {select_clause} 
        FROM {table_name} as t
        WHERE DATE_TRUNC('day', {date_column_name}) = %s;
        """
        return query

    def fetch_data_from_postgres(self, date: datetime, table_name: str, date_column_name: str) -> List[BaseModel]:
        dict_cast = {
            'int' : 'integer',
            'float' : 'float',
            'datetime' : 'timestamp',
            'str' : 'text'
        }
        fields_dict = self.data_model.__annotations__
        fields_str_dict = {field_name : field_type.__name__ for field_name, field_type in fields_dict.items()}

        query = self.generate_select_query(table_name=table_name,
                                           fields=fields_str_dict,
                                           cast_dict=dict_cast,
                                           date_column_name=date_column_name)

        connection = psycopg2.connect(**self.pg_config)
        cursor = connection.cursor()
        cursor.execute(query, (date,))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = [self.data_model(**dict(zip(columns, row))) for row in rows]

        cursor.close()
        connection.close()

        return data

    def load_data_to_vertica(self, data: list, table_name: str) :
        connection = vertica_python.connect(**self.vertica_config)
        cursor = connection.cursor()

        if not data :
            print("No data to insert.")
            return

        columns = data[0].dict().keys()
        column_list = ', '.join(columns)
        values_placeholder = ', '.join([f":{col}" for col in columns])
        insert_query = f"INSERT INTO {table_name} ({column_list}) VALUES ({values_placeholder})"

        cursor.executemany(insert_query, [item.dict() for item in data])
        connection.commit()

        cursor.close()
        connection.close()

    def migrate_data(self, date: datetime, pg_table_name: str, vertica_table_name: str, date_column_name: str) :
        data = self.fetch_data_from_postgres(date, pg_table_name, date_column_name)
        self.load_data_to_vertica(data, vertica_table_name, )


