import contextlib
from typing import Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag

import pendulum
from raw_pydantic_stg_load_from_postgres import DataMigrator, TransactionModel, CurrenciesModel

pg_config = {
    'dbname': 'db1',
    'user': 'student',
    'password': 'de_student_112022',
    'host': 'rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net',
    'port': '6432'
}

vertica_config = {'host': 'vertica.tgcloudenv.ru',
             'port': '5433',
             'user': 'stv2024031257',
             'password': 'UD3dewuz6qI31fu',
             'database': 'dwh',
             # Вначале автокоммит понадобится, а позже решите сами.
                         'autocommit': True
}
@dag(
    #   schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['final', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True) # Остановлен/запущен при появлении. Сразу запущен.)
def dag_load_data_to_staging():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    @task(task_id="load_group_log_postgres")
    def load_group_log_postgres():
        # создаем экземпляр класса, в котором реализована логика.
        transaction_loader = DataMigrator(pg_config, vertica_config, TransactionModel)
        transaction_loader.migrate_data('2022-10-09',
                                        'public.transactions',
                                        'STV2024031257__STAGING.stg_transactions',
                                        'transaction_dt')  # Вызываем функцию, которая перельет данные.

    start >> [load_group_log_postgres] >> end


_ = dag_load_data_to_staging()
