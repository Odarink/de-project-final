from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import pendulum
from global_metrics import MetricsCalculator
from airflow.models.variable import Variable

@dag(
    dag_id='dds01',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=True,
    tags=['final', 'dds', 'origin', 'example'],
    is_paused_upon_creation=True
)
def dag_load_data_to_dwh():
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
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
    @task(task_id="load_global_metrics")
    def load_global_metrics():
        load_global_metrics = MetricsCalculator(vertica_config)
        load_global_metrics.calculate_and_load_metrics(
            '2022-10-09',
            'STV2024031257__DWH.dwh_global_metrics'
        )
    t_load_global_metrics = load_global_metrics()

    start >> t_load_global_metrics >> end

dag_instance = dag_load_data_to_dwh()
