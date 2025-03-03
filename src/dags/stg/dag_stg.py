from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from stg_load_from_postgres import DataMigrator
from airflow.models.variable import Variable
from lib import ConnectionBuilder
import pendulum



@dag(
    dag_id="dag_load_data_to_staging",
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=True,
    tags=['final', 'stg', 'origin', 'example'],
    is_paused_upon_creation=True
)
def dag_load_data_to_staging():
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
    def load_transactions():
        transaction_loader = DataMigrator(pg_config, vertica_config)
        transaction_loader.migrate_data(
            '2022-10-09',
            'public.transactions',
            'STV2024031257__STAGING.stg_transactions',
            'transaction_dt'
        )

    @task(task_id="load_currencies")
    def load_currencies():
        currencies_loader = DataMigrator(pg_config, vertica_config)
        currencies_loader.migrate_data(
            '2022-10-09',
            'public.currencies',
            'STV2024031257__STAGING.stg_сurrencies',
            'date_update'
        )

    load_transactions_task = load_transactions()
    load_currencies_task = load_currencies()

    start >> [load_transactions_task, load_currencies_task] >> end

dag_instance = dag_load_data_to_staging()
