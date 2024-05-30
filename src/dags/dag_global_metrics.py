from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable
from lib import ConnectionBuilder
import pendulum

from stg.stg_load_from_postgres import DataMigrator
from dds.global_metrics import MetricsCalculator

# Конфигурация подключения
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
    dag_id="dag_load_data_to_all",
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 10, 30, tz="UTC"),
    catchup=True,
    tags=['final', 'stg', 'origin', 'example'],
    is_paused_upon_creation=True,
    max_active_runs=1
)
def dag_load_data_to_all():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    pg_config = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    USER_NAME = Variable.get("USER_NAME")
    USER_PASSWORD = Variable.get("USER_PASSWORD")
    vertica_config = {'host' : 'vertica.tgcloudenv.ru',
                      'port' : '5433',
                      'user' : USER_NAME,
                      'password' : USER_PASSWORD,
                      'database' : 'dwh',
                      # Вначале автокоммит понадобится, а позже решите сами.
                      'autocommit' : True
                      }
    @task(task_id="load_transactions")
    def load_transactions(ds=None):
        transaction_loader = DataMigrator(pg_config, vertica_config)
        transaction_loader.migrate_data(
            ds,
            'public.transactions',
            'STV2024031257__STAGING.stg_transactions',
            'transaction_dt'
        )

    @task(task_id="load_currencies")
    def load_currencies(ds=None):
        currencies_loader = DataMigrator(pg_config, vertica_config)
        currencies_loader.migrate_data(
            ds,
            'public.currencies',
            'STV2024031257__STAGING.stg_сurrencies',
            'date_update'
        )

    @task(task_id="load_global_metrics")
    def load_global_metrics(ds=None):
        currencies_loader = MetricsCalculator(pg_config, vertica_config)
        currencies_loader.calculate_and_load_metrics(
            ds,
            'STV2024031257__DWH.dwh_global_metrics'
        )

    load_transactions_task = load_transactions()
    load_currencies_task = load_currencies()
    load_global_metrics = load_global_metrics()

    start >> [load_transactions_task, load_currencies_task] >> load_global_metrics >>end

dag_instance = dag_load_data_to_all()
