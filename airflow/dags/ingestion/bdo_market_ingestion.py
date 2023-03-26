import pendulum

from airflow.decorators import dag
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

DEVELOPER = 'Cloudskipper'
PROCESS_NAME = 'bdo_market_ingestion'


# Default arguments that will be passed to every task
DAG_ARGS = {
    'owner': DEVELOPER,
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=2),
}


# DAG definition
@dag(
    start_date = pendulum.datetime(year=2023, month=1, day=1, tz='America/New_York'),
    schedule_interval = None,
    description = None,
    catchup = False,
    max_active_runs = 1,
    default_args = DAG_ARGS
)
def bdo_market_ingestion() -> DAG:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    start >> end


run_dag = bdo_market_ingestion()
